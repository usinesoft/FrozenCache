using System.Collections.Frozen;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace PersistentStore;

/// <summary>
///  A persistent collection inside a data store.
///  The file layout:
///  -offset 0: file header containing the offset, size and index keys for all the objects in the file.
///  -offset _documentHeaderSize * _maxDocuments : the first object binary data
/// </summary>
public sealed class CollectionStore : IAsyncDisposable, IDisposable
{
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// For each file in the collection, we keep a map of offsets and index keys for each document.
    /// </summary>
    private readonly List<PersistentObjectHeader?[]> _fileMap;


    private readonly int _numberOfKeys;
    
    private readonly int _documentHeaderSize;


    /// <summary>
    ///     Total size of document data (one gigabyte by default)
    /// </summary>
    private readonly int _binaryFileDataSize;

    private readonly List<MemoryMappedFile> _files = [];

    /// <summary>
    ///     Maximum number of documents that can be stored in a binary file (one million by default)
    /// </summary>
    private readonly int _maxDocuments;

    private readonly List<MemoryMappedViewAccessor> _views = [];

    private MemoryMappedViewAccessor? _currentWriteView;

    private int _documentsInCurrentView;
    
    private int _firstFreeOffset;

    private FrozenDictionary<long, PersistentObjectHeader[]>? _byPrimaryKey;

    public CollectionStore(string storagePath, int numberOfKeys, int binaryFileDataSize = Consts.DefaultBinaryFileDataSize,
        int maxDocumentsInEachFile = Consts.DefaultMaxDocumentsInOneFile)
    {
        _numberOfKeys = numberOfKeys;

        _documentHeaderSize = sizeof(int) + sizeof(int)+ (sizeof(long) * _numberOfKeys); // offset, length, and keys

        _binaryFileDataSize = binaryFileDataSize;

        _maxDocuments = maxDocumentsInEachFile;


        StoragePath = storagePath;

        if (Directory.Exists(StoragePath))
        {
            var files = Directory.EnumerateFiles(StoragePath, Consts.BinaryFilePattern)
                .OrderBy(name => name);

            foreach (var fileName in files)
            {
                var mmFile = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open);
                _views.Add(mmFile.CreateViewAccessor());
                _files.Add(mmFile);
            }

            if (_views.Count > 0) _currentWriteView = _views[^1];
        }
        else
        {
            Directory.CreateDirectory(StoragePath);
        }

        _fileMap = new List<PersistentObjectHeader?[]>(_files.Count);

        if (_views.Count == 0)
        {
            CreateNewFile(1);
        }
        else
        {
            ReadMap();
        }

            
    }


    public string StoragePath { get; }

    #region Implementation of IDisposable

    /// <summary>
    ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        foreach (var view in _views) view.Dispose();

        foreach (var file in _files) file.Dispose();
    }

    #endregion


    /// <summary>
    /// When a new document is saved, two blocks are written:
    /// - a header containing the keys, and the offset
    /// - document data as a raw byte[]
    /// </summary>
    /// <param name="documentWithKeys"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public void StoreNewDocument(Item documentWithKeys)
    {
        if (documentWithKeys.Data.Length > _binaryFileDataSize)
            throw new NotSupportedException("Document size exceeds binary file size");

        bool tooManyDocuments = _documentsInCurrentView >= _maxDocuments;
        bool tooMuchData = _firstFreeOffset + documentWithKeys.Data.Length >= _binaryFileDataSize;


        if (tooMuchData && !tooManyDocuments)
        {
            WriteEndMarker(); // an end marker is written to the current file to indicate that it contains less than _maxDocuments documents
        }

        if (tooManyDocuments ||tooMuchData)
        {
            CreateNewFile(_views.Count + 1);
        }

        var headerOffset = _documentsInCurrentView * _documentHeaderSize;

        var header = new PersistentObjectHeader
        {
            OffsetInFile = _firstFreeOffset,
            Length = documentWithKeys.Data.Length,
            IndexKeys = documentWithKeys.Keys.ToArray()
        };

        WriteBytes(headerOffset, header.ToBytes());

        WriteBytes(_firstFreeOffset, documentWithKeys.Data);

        _fileMap[_views.Count - 1][_documentsInCurrentView] = header;

        _documentsInCurrentView++;
        _firstFreeOffset += documentWithKeys.Data.Length;

    }

    /// <summary>
    /// Writes an end marker to the current file. Useful when the file contains less than _maxDocuments documents,
    /// </summary>
    private void WriteEndMarker()
    {
        var headerOffset = _documentsInCurrentView * _documentHeaderSize;

        var header = new PersistentObjectHeader
        {
            OffsetInFile = 0,
            Length = 0, // this is an end marker, so length is 0
            IndexKeys = new long[_numberOfKeys]
        };

        WriteBytes(headerOffset, header.ToBytes());
    }

   
    public Item LoadDocument(PersistentObjectHeader header)
    {
        var view = _views[header.FileIndex];

        
        int length = header.Length;

        var data = new byte[length];

        ReadBytes(header.OffsetInFile, length, view, data);

        return new Item(data, header.IndexKeys);
    }

    

    private void CreateNewFile(int index)
    {
        var extension = Consts.BinaryFilePattern.Trim('*');
        var fileName = index.ToString("D4") + extension;

        var path = Path.Combine(StoragePath, fileName);
        var newFile = MemoryMappedFile.CreateFromFile(path, FileMode.CreateNew, null, _binaryFileDataSize);

        _currentWriteView = newFile.CreateViewAccessor();
        _views.Add(_currentWriteView);
        _files.Add(newFile);

        _documentsInCurrentView = 0;

        _firstFreeOffset = _documentHeaderSize * _maxDocuments;

        var headers = new PersistentObjectHeader[_maxDocuments];
        _fileMap.Add(headers);

    }


    private void ReadMap()
    {
        int fileIndex = 0;
        foreach (var viewAccessor in _views)
        {
            ReadFileMap(viewAccessor, fileIndex);
            fileIndex++;
        }

        CreateIndexes();
    }

    private unsafe void ReadFileMap(MemoryMappedViewAccessor view, int fileIndex)
    {
        var buffer = new byte[_documentHeaderSize];
        var ptr = (byte*)0;

        try
        {
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            List<PersistentObjectHeader> headersInThisFile = new List<PersistentObjectHeader>(_maxDocuments);

            for (int i = 0; i < _maxDocuments; i++)
            {
                Marshal.Copy(new IntPtr(ptr), buffer, 0, _documentHeaderSize);

                var header = new PersistentObjectHeader();
                header.FromBytes(buffer);

                if(header.IsEndMarker)
                {
                    // end marker, stop reading
                    break;
                }

                header.FileIndex = fileIndex; // enrich the header with the file index

                headersInThisFile.Add(header);

                ptr += _documentHeaderSize;

            }

            _fileMap.Add(headersInThisFile.ToArray());

        }
        finally
        {
            view.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    
    private static unsafe void ReadBytes(int offset, int num, MemoryMappedViewAccessor view, byte[] buffer)
    {
        var ptr = (byte*)0;
        view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        Marshal.Copy(IntPtr.Add(new IntPtr(ptr), offset), buffer, 0, num);
        view.SafeMemoryMappedViewHandle.ReleasePointer();
    }

    private unsafe void WriteBytes(int offset, byte[] data)
    {
        var ptr = (byte*)0;
        _currentWriteView?.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        Marshal.Copy(data, 0, IntPtr.Add(new IntPtr(ptr), offset), data.Length);
        _currentWriteView?.SafeMemoryMappedViewHandle.ReleasePointer();
    }


    // After the last document is written call this method to trigger index creation

    public void CreateIndexes()
    {
        var index = new Dictionary<long, List<PersistentObjectHeader>>();
        foreach (var headers in _fileMap)
        {
            foreach (var header in headers)
            {
                if (header == null)
                    break;

                var mostDiscriminantKey = header.IndexKeys[0];
                if (!index.ContainsKey(mostDiscriminantKey))
                {
                    index[mostDiscriminantKey] = [];
                }
                index[mostDiscriminantKey].Add(header);
                
            }
        }
        _byPrimaryKey = index.ToFrozenDictionary(x=>x.Key, x=>x.Value.ToArray());
    }
    public void EndOfFeed()
    {
        CreateIndexes();
    }

    public Item? GetByFirstKey(long keyValue)
    {

        if (_byPrimaryKey != null && _byPrimaryKey.TryGetValue(keyValue, out PersistentObjectHeader[]? values))
        {
            return values is { Length: > 0 } headers
                ? LoadDocument(headers[0])
                : null;
        }

        return null;
    }
}