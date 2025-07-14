using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace PersistentStore;

/// <summary>
///  A persistent collection inside a data store.
///  The file layout:
///  -offset 0: file header containing the offset and index keys for all the objects in the file.
///  -offset 8 + (document_index * (8 + 8 * key_count)): long - offset of the document in the file
/// </summary>
public sealed class CollectionStore : IAsyncDisposable, IDisposable
{
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private readonly List<int[]> _fileMap;


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

    public CollectionStore(string storagePath, int numberOfKeys, int binaryFileDataSize = Consts.DefaultBinaryFileDataSize,
        int maxDocumentsInEachFile = Consts.DefaultMaxDocumentsInOneFile)
    {
        _numberOfKeys = numberOfKeys;

        _documentHeaderSize = sizeof(long) * (1 + _numberOfKeys); // 1 for the offset, and one for each key

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

        _fileMap = new List<int[]>(_files.Count);

        if (_views.Count == 0) CreateNewFile(1);


        ReadMap();
    }


    /// <summary>
    ///     (Number of documents + 1) X size of an offset (int) + the size of an int for the document counter
    /// </summary>
    private int BinaryFileIndexSize => (_maxDocuments + 1) * sizeof(int) + sizeof(int);

    private int BinaryFileSize => _binaryFileDataSize + BinaryFileIndexSize;

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


    public Pointer StoreNewDocument(byte[] documentData)
    {
        if (documentData.Length > _binaryFileDataSize)
            throw new NotSupportedException("Document size exceeds binary file size");

        if (_documentsInCurrentView < _maxDocuments &&
            _firstFreeOffset + documentData.Length < _binaryFileDataSize)
        {
            var offsetOfOffset = sizeof(int) + _documentsInCurrentView * sizeof(int);

            WriteInt(offsetOfOffset, _firstFreeOffset);
        }
        else // create a new file
        {
            CreateNewFile(_views.Count + 1);
        }

        WriteBytes(_firstFreeOffset, documentData);

        _fileMap[_views.Count - 1][_documentsInCurrentView] = _firstFreeOffset;

        _documentsInCurrentView++;
        _firstFreeOffset += documentData.Length;

        _fileMap[_views.Count - 1][_documentsInCurrentView] = _firstFreeOffset;

        var offsetOfNextOffset = sizeof(int) + _documentsInCurrentView * sizeof(int);
        WriteInt(offsetOfNextOffset, _firstFreeOffset);

        WriteInt(0, _documentsInCurrentView);


        return new Pointer(_views.Count - 1, _documentsInCurrentView - 1);
    }

    public byte[] LoadDocument(Pointer pointer)
    {
        var view = _views[pointer.FileIndex];

        var offset = _fileMap[pointer.FileIndex][pointer.DocumentIndex];

        var nextOffset = _fileMap[pointer.FileIndex][pointer.DocumentIndex + 1];

        return ReadBytes(offset, nextOffset - offset, view);
    }

    /// <summary>
    ///     Iterate on all the documents in all the files
    /// </summary>
    /// <returns></returns>
    public IEnumerable<KeyValuePair<Pointer, byte[]>> AllDocuments()
    {
        var fileIndex = 0;
        foreach (var view in _views)
        {
            var offsets = _fileMap[fileIndex];

            for (var docIndex = 0; docIndex < _maxDocuments; docIndex++)
            {
                var offset = offsets[docIndex];

                var nextOffset = offsets[docIndex + 1];
                if (nextOffset == 0) break;

                var data = ReadBytes(offset, nextOffset - offset, view);

                yield return new KeyValuePair<Pointer, byte[]>(new Pointer(fileIndex, docIndex), data);
            }

            fileIndex++;
        }
    }

    private void CreateNewFile(int index)
    {
        var extension = Consts.BinaryFilePattern.Trim('*');
        var fileName = index.ToString("D4") + extension;

        var path = Path.Combine(StoragePath, fileName);
        var newFile = MemoryMappedFile.CreateFromFile(path, FileMode.CreateNew, fileName, BinaryFileSize);

        _currentWriteView = newFile.CreateViewAccessor();
        _views.Add(_currentWriteView);
        _files.Add(newFile);

        WriteInt(0, 0); // zero documents for now
        _documentsInCurrentView = 0;

        WriteInt(sizeof(int), BinaryFileIndexSize); // the first free offset is at the end of the index
        _firstFreeOffset = BinaryFileIndexSize;

        var offsets = new int[_maxDocuments + 1];
        _fileMap.Add(offsets);

        offsets[0] = _firstFreeOffset = BinaryFileIndexSize;
    }


    private void ReadMap()
    {
        foreach (var viewAccessor in _views) ReadFileMap(viewAccessor);
    }

    private unsafe void ReadFileMap(MemoryMappedViewAccessor view)
    {
        var buffer = new byte[BinaryFileIndexSize];
        var ptr = (byte*)0;

        try
        {
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            Marshal.Copy(new IntPtr(ptr), buffer, 0, BinaryFileIndexSize);

            fixed (byte* bufferPtr = buffer)
            {
                var intPtr = (int*)bufferPtr;
                var count = *intPtr;

                var offsets = new int[_maxDocuments + 1];
                _fileMap.Add(offsets);
                for (var i = 0; i <= count; i++)
                {
                    intPtr++;

                    offsets[i] = *intPtr;
                }
            }
        }
        finally
        {
            view.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    private unsafe void WriteInt(int offset, int value)
    {
        var buffer = new byte[4];

        new BinaryWriter(new MemoryStream(buffer)).Write(value);

        var ptr = (byte*)0;

        try
        {
            _currentWriteView?.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            Marshal.Copy(buffer, 0, IntPtr.Add(new IntPtr(ptr), offset), buffer.Length);
        }
        finally
        {
            _currentWriteView?.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    private static unsafe byte[] ReadBytes(int offset, int num, MemoryMappedViewAccessor view)
    {
        var arr = new byte[num];
        var ptr = (byte*)0;
        view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        Marshal.Copy(IntPtr.Add(new IntPtr(ptr), offset), arr, 0, num);
        view.SafeMemoryMappedViewHandle.ReleasePointer();
        return arr;
    }

    private unsafe void WriteBytes(int offset, byte[] data)
    {
        var ptr = (byte*)0;
        _currentWriteView?.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        Marshal.Copy(data, 0, IntPtr.Add(new IntPtr(ptr), offset), data.Length);
        _currentWriteView?.SafeMemoryMappedViewHandle.ReleasePointer();
    }
}


/// <summary>
/// At the beginning of the binary file there is a header that contains the offsets and index keys for all the objects.
/// This class contains data for one object. 
/// </summary>
public class PersistentObjectHeader
{
    public long OffsetInFile { get; set; }

    public long[] IndexKeys { get; set; } = [];

    public void FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length % 8 != 0)
        {
            throw new CacheException("A header size must by a multiple of 8 (sizeof long)");
        }

        if (bytes.Length < 16)
        {
            throw new CacheException("A header size must contain at least an offset and one key");
        }

        int keys = bytes.Length / 8 - 1; // first 8 bytes are the offset, the rest are keys

        OffsetInFile = BitConverter.ToInt64(bytes[..8]);
        IndexKeys = new long[keys];

        var offset = 8; // start after the offset
        for (int i = 0; i < keys; i++)
        {
            IndexKeys[i] = BitConverter.ToInt64(bytes[offset..(offset+8)]);
            offset += 8;
        }
    }
}