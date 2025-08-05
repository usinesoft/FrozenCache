using System.Buffers;
using System.Text;

namespace Messages;

/// <summary>
/// Serializer for collections of FeedItem messages.
/// </summary>
public class FeedItemBatchSerializer : IBatchSerializer<FeedItem>
{
    private readonly FeedItemSerializer _serializer = new();
    public int Serialize(BinaryWriter writer, Span<FeedItem> items, int maxBatchSizeInBytes = 1_000_000)
    {
        

        // an empty batch marks the end of the stream
        if (items.Length == 0)
        {
            writer.Write(0);
            writer.Write(0);
            return 0;
        }

        
        byte[] buffer = ArrayPool<byte>.Shared.Rent(2 * maxBatchSizeInBytes);
        MemoryStream memoryStream = new MemoryStream(buffer);

        var memoryWriter = new BinaryWriter(memoryStream, Encoding.UTF8, true);

        int count = 0;
        foreach (var item in items)
        {
            _serializer.Serialize(memoryWriter, item);

            count++;

            if (memoryStream.Position > maxBatchSizeInBytes)
                break;
                
        }

        var blockSize = (int)memoryStream.Position;
        writer.Write(blockSize);
        writer.Write(count);

        writer.Write(buffer, 0, blockSize);


        ArrayPool<byte>.Shared.Return(buffer);

        int batches = 1;

        var lastBatchSize = 0;

        if (count < items.Length)
        {
            // Recursive call is used to handle large batches that exceed the maxBatchSizeInBytes
            lastBatchSize = Serialize(writer, items.Slice(count), maxBatchSizeInBytes);
            batches += lastBatchSize;
        }

        //if (lastBatchSize != 0)
        //{
        //    Serialize(writer, Array.Empty<FeedItem>()); // Write an empty batch to mark the end of stream
        //}

        return batches;
    }
    public ICollection<FeedItem> Deserialize(BinaryReader reader)
    {
        
        int size = reader.ReadInt32();

        byte[] buffer = ArrayPool<byte>.Shared.Rent(2 * size);

        
        var count = reader.ReadInt32();

        if (size == 0 && count == 0)// end of stream
            return Array.Empty<FeedItem>();

        var remainingBytes = size;
        var offset = 0;
        while (remainingBytes > 0)
        {
            var bytes = reader.Read(buffer, offset, remainingBytes);
            remainingBytes -= bytes;
            offset += bytes;
        }



        MemoryStream memoryStream = new MemoryStream(buffer);

        List<FeedItem> result = new List<FeedItem>(count);

        var memoryReader = new BinaryReader(memoryStream, Encoding.UTF8, true);
        for (int i = 0; i < count; i++)
        {
            var item = _serializer.Deserialize(memoryReader);
            result.Add(item);
        }

        ArrayPool<byte>.Shared.Return(buffer);


        return result;
    }
}