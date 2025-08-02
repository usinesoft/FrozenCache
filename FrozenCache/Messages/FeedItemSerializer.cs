using System.Buffers;

namespace Messages;

/// <summary>
/// Serializer for FeedItem messages. They are serialized manually to improve performance.
/// </summary>
public class FeedItemSerializer : IBinarySerializer<FeedItem>
{
    public FeedItem Deserialize(BinaryReader reader)
    {
        
        var item = new FeedItem();

        var size = reader.ReadInt32();

        var buffer = ArrayPool<byte>.Shared.Rent(size);

        int alreadyRead = 0;
        while (alreadyRead < size)
        {
            alreadyRead += reader.Read(buffer, alreadyRead, size - alreadyRead);
        }
        

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

    public void Serialize(BinaryWriter writer, FeedItem message)
    {
        
        // Calculate the size of the serialized data: keyCount + keys + data
        var size = sizeof(int) + message.Keys.Length * sizeof(long) + message.Data.Length;


        // the buffer will contain its own size at the beginning, so we need to add 4 bytes for that
        var buffer = ArrayPool<byte>.Shared.Rent(size + 4);

        BitConverter.TryWriteBytes(buffer.AsSpan(0, 4), size);
        BitConverter.TryWriteBytes(buffer.AsSpan(4, 4), message.Keys.Length);

        var offset = 8;
        foreach (var key in message.Keys)
        {
            BitConverter.TryWriteBytes(buffer.AsSpan(offset, 8), key);
            offset += 8;
        }

        message.Data.CopyTo(buffer.AsSpan(offset, size + 4 - offset));

        writer.Write(buffer, 0, size + 4);

        writer.Flush();

        ArrayPool<byte>.Shared.Return(buffer);
    }
}