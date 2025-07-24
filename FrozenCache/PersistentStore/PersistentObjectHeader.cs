namespace PersistentStore;

/// <summary>
/// At the beginning of the binary file there is a header that contains the offsets, data length and index keys for all the objects.
/// This class contains data for one object. 
/// </summary>
public class PersistentObjectHeader
{
    public int OffsetInFile { get; set; }

    public int Length { get; set; }

    public long[] IndexKeys { get; set; } = [];

    /// <summary>
    /// This property is not persistent. It is enriched by the loading code
    /// </summary>
    public int FileIndex { get; set; }

    /// <summary>
    /// This property is not persistent. It is enriched by the loading code
    /// </summary>
    
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

        OffsetInFile = BitConverter.ToInt32(bytes[..4]);
        Length = BitConverter.ToInt32(bytes[4..8]);

        IndexKeys = new long[keys];

        var offset = 8; // start after the offset
        for (int i = 0; i < keys; i++)
        {
            IndexKeys[i] = BitConverter.ToInt64(bytes[offset..(offset+8)]);
            offset += 8;
        }
    }

    public byte[] ToBytes()
    {
        Span<byte> bytes = stackalloc byte[4 + 4 + IndexKeys.Length * 8];
        
        BitConverter.TryWriteBytes(bytes[..4], OffsetInFile);
        BitConverter.TryWriteBytes(bytes[4..8], Length);

        var offset = 8; // start after the offset and length
        for (int i = 0; i < IndexKeys.Length; i++)
        {
            BitConverter.TryWriteBytes(bytes[offset..(offset+8)], IndexKeys[i]);
            offset += 8;
        }
        return bytes.ToArray();
    }

    public bool IsEndMarker => Length == 0;
}