using System.Buffers;
using MessagePack;

namespace Messages;

[MessagePackObject]
public class FeedItem : IMessage
{
    [Key(0)] public long[] Keys { get; set; } = [];

    [Key(1)] public byte[] Data { get; set; } = [];

    [IgnoreMember]
    public bool IsEndOfStream => Data.Length == 0 && Keys.Length == 0;


    [IgnoreMember]
    public MessageType Type => MessageType.FeedItem;

    public override string ToString()
    {
        return $"{nameof(Keys)}: {Keys}, {nameof(Data)}: {Data}, {nameof(Type)}: {Type}";
    }


    /// <summary>
    /// Manual serialization to improve speed. About twice as fast as MessagePack serialization.
    /// </summary>
    /// <param name="writer"></param>
    public void Serialize(BinaryWriter writer )
    {
        // Calculate the size of the serialized data: keyCount + keys + data
        var size = sizeof(int)+ Keys.Length * sizeof(long) + Data.Length;

        
        // the buffer will contain its own size at the beginning, so we need to add 4 bytes for that
        var buffer = ArrayPool<byte>.Shared.Rent(size + 4);

        BitConverter.TryWriteBytes(buffer.AsSpan(0,4), size);
        BitConverter.TryWriteBytes(buffer.AsSpan(4, 4), Keys.Length);

        var offset = 8;
        foreach (var key in Keys)
        {
            BitConverter.TryWriteBytes(buffer.AsSpan(offset, 8), key);
            offset += 8;
        }

        Data.CopyTo(buffer.AsSpan(offset, size+4-offset));

        writer.Write(buffer, 0, size + 4);

        writer.Flush();

        ArrayPool<byte>.Shared.Return(buffer);

        
    }

    public static FeedItem Deserialize(BinaryReader reader)
    {
        var item = new FeedItem();
        
        var size = reader.ReadInt32();
        
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        
        reader.Read(buffer, 0, size);

        var keysCount = BitConverter.ToInt32(buffer, 0);

        item.Keys = new long[keysCount];

        var offset = 4; 
        for (int i = 0; i < keysCount; i++)
        {
            item.Keys[i] = BitConverter.ToInt64(buffer, offset);
            offset += 8;
        }

        var dataLength = size - offset;
        item.Data = buffer.AsSpan(offset, dataLength).ToArray();

        ArrayPool<byte>.Shared.Return(buffer);

        return item;
    }
}