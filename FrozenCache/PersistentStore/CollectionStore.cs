using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace PersistentStore;

/// <summary>
///     A persistent collection inside a data store.
///     The file layout:
///     -offset 0: file header containing the offset, size and index keys for all the objects in the file.
///     -offset _documentHeaderSize * _maxDocuments : the first object binary data
/// </summary>
public sealed class CollectionStore : IAsyncDisposable, IDisposable
{
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }


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

    public long TotalSizeInBytes { get; private set; }
    public int ObjectCount => _primaryIndex.ObjectCount;
    
    public int NonUniqueKeys => _primaryIndex.NonUniqueKeys;

    private readonly IIndex _primaryIndex;

    private readonly ILogger? _logger;

    /// <summary>
    ///     Create a new empty collection store at the specified path.Before data is fed in a new collection, the directory
    ///     will contain only metadata
    /// </summary>
    /// <param name="storagePath">The root path of the <see cref="DataStore" /> containing this collection</param>
    /// <param name="numberOfKeys"></param>
    /// <param name="logger"></param>
    /// <param name="binaryFileDataSize"></param>
    /// <param name="maxDocumentsInEachFile"></param>
    public CollectionStore(string storagePath, int numberOfKeys, ILogger? logger,
        int binaryFileDataSize = Consts.DefaultBinaryFileDataSize,
        int maxDocumentsInEachFile = Consts.DefaultMaxDocumentsInOneFile, IndexType primaryIndexType = IndexType.Dictionary)
    {
        _logger = logger;

        _numberOfKeys = numberOfKeys;

        _documentHeaderSize = sizeof(int) + sizeof(int) + sizeof(long) * _numberOfKeys; // offset, length, and keys

        _binaryFileDataSize = binaryFileDataSize;

        _maxDocuments = maxDocumentsInEachFile;


        StoragePath = storagePath;

        if (Directory.Exists(StoragePath))
        {
            var files = Directory.EnumerateFiles(StoragePath, Consts.BinaryFilePattern)
                .OrderBy(name => name).ToArray();

            _logger?.LogInformation("Mapping {Count} segment files", files.Length);

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

        _primaryIndex = primaryIndexType switch
        {
            IndexType.Dictionary => new DictionaryIndex(),
            IndexType.Ordered => new OrderedIndex(),
            _ => throw new ArgumentOutOfRangeException(nameof(primaryIndexType), primaryIndexType, null)
        };

        if (_views.Count == 0)
            CreateNewFile(1);
        else
            ReadMap();
    }


    private string StoragePath { get; }

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
    ///     When a new document is saved, two blocks are written:
    ///     - a header containing the keys, and the offset
    ///     - document data as a raw byte[]
    /// </summary>
    /// <param name="documentWithKeys"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public void StoreNewDocument(Item documentWithKeys)
    {
        if (documentWithKeys.Data.Length > _binaryFileDataSize)
            throw new NotSupportedException("Document size exceeds binary file size");

        var tooManyDocuments = _documentsInCurrentView >= _maxDocuments;
        var tooMuchData = _firstFreeOffset + documentWithKeys.Data.Length >= _binaryFileDataSize;


        if (tooMuchData && !tooManyDocuments)
            WriteEndMarker(); // an end marker is written to the current file to indicate that it contains less than _maxDocuments documents

        if (tooManyDocuments || tooMuchData) CreateNewFile(_views.Count + 1);

        var headerOffset = _documentsInCurrentView * _documentHeaderSize;

        var header = new PersistentObjectHeader
        {
            OffsetInFile = _firstFreeOffset,
            Length = documentWithKeys.Data.Length,
            IndexKeys = documentWithKeys.Keys.ToArray()
        };

        WriteBytes(headerOffset, header.ToBytes());

        WriteBytes(_firstFreeOffset, documentWithKeys.Data);

        IndexHeader(_views.Count - 1, header);

        _documentsInCurrentView++;
        _firstFreeOffset += documentWithKeys.Data.Length;

        TotalSizeInBytes += documentWithKeys.Data.Length;
    }

    /// <summary>
    ///     Writes an end marker to the current file. Useful when the file contains less than _maxDocuments documents,
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


    private Item LoadDocument(long primaryKey, IndexEntry entry)
    {
        var view = _views[entry.FileIndex];


        var data = new byte[entry.Length];

        ReadBytes(entry.OffsetInFile, entry.Length, view, data);

        return new Item(data, [primaryKey, ..entry.OtherKeys]);
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
    }


    private void ReadMap()
    {
        var fileIndex = 0;

        _logger?.LogInformation("Reading file index for {Count} files...", _views.Count);


        foreach (var viewAccessor in _views)
        {
            ReadFileMap(viewAccessor, fileIndex);
            fileIndex++;
        }

        _logger?.LogInformation("Done reading indexes for {Count} files.", _views.Count);

        CreateIndexes();
    }

    private unsafe void ReadFileMap(MemoryMappedViewAccessor view, int fileIndex)
    {
        var buffer = new byte[_documentHeaderSize];
        var ptr = (byte*)0;

        try
        {
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            for (var i = 0; i < _maxDocuments; i++)
            {
                Marshal.Copy(new IntPtr(ptr), buffer, 0, _documentHeaderSize);

                var header = new PersistentObjectHeader();
                header.FromBytes(buffer);

                if (header.IsEndMarker)
                    // end marker, stop reading
                    break;

                header.FileIndex = fileIndex; // enrich the header with the file index

                IndexHeader(fileIndex, header);

                TotalSizeInBytes += header.Length;

                ptr += _documentHeaderSize;
            }
        }
        finally
        {
            view.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }


    private void IndexHeader(int fileIndex, PersistentObjectHeader header)
    {
        // For now duplicated keys will be found both in the unique and non-unique collections
        // At the end the duplicate keys will be removed from the unique collection

        var key = header.IndexKeys[0];

        var newEntry = new IndexEntry
        {
            OtherKeys = header.IndexKeys[1..],
            FileIndex = fileIndex,
            OffsetInFile = header.OffsetInFile,
            Length = header.Length
        };

        _primaryIndex.Add(key, newEntry);
    }


    private static unsafe void ReadBytes(int offset, int length, MemoryMappedViewAccessor view, byte[] buffer)
    {
        var ptr = (byte*)0;
        view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        Marshal.Copy(IntPtr.Add(new IntPtr(ptr), offset), buffer, 0, length);
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

    private void CreateIndexes()
    {
        _logger?.LogInformation("Post-processing index..");

        _primaryIndex.PostProcess();


        _logger?.LogInformation("Done post-processing index");
    }

    public void EndOfFeed()
    {
        CreateIndexes();
    }

    public List<Item> GetByFirstKey(long keyValue)
    {
        List<Item> result = new();

        var entries = _primaryIndex.Get(keyValue);

        foreach (var indexEntry in entries)
        {
            var value = LoadDocument(keyValue, indexEntry);
            result.Add(value);
        }


        return result;
    }
}

/// <summary>
///     The value in the index by most discriminant key.
/// </summary>
//[StructLayout(LayoutKind.Sequential, Pack = 4)]
public class IndexEntry
{
    /// <summary>
    ///     Rest of the keys that can be used to retrieve the object.
    /// </summary>
    public long[] OtherKeys { get; init; } = [];

    /// <summary>
    ///     The file containing the object
    /// </summary>
    public int FileIndex { get; init; }

    /// <summary>
    /// </summary>
    public int OffsetInFile { get; init; }

    /// <summary>
    ///     Length of the object in bytes.
    /// </summary>
    public int Length { get; init; }
}