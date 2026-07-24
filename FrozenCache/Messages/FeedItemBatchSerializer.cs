using System.Buffers;
using System.Text;

namespace Messages;

/// <summary>
/// Serializer for collections of FeedItem messages.
/// </summary>
public class FeedItemBatchSerializer : IBatchSerializer<FeedItem>
{
    private readonly FeedItemSerializer _serializer = new();

    /// <summary>Size of the (blockSize, count) header written before every batch's payload.</summary>
    private const int HeaderSize = sizeof(int) + sizeof(int);

    public int Serialize(BinaryWriter writer, Span<FeedItem> items, int maxBatchSizeInBytes = 1_000_000)
    {

        // an empty batch marks the end of the stream
        if (items.Length == 0)
        {
            Span<byte> emptyHeader = stackalloc byte[HeaderSize];
            BitConverter.TryWriteBytes(emptyHeader[..4], 0);
            BitConverter.TryWriteBytes(emptyHeader[4..], 0);
            writer.Write(emptyHeader);
            return 0;
        }

        // The header is written into the same rented buffer, ahead of the payload, so the whole batch goes
        // out as a single Write call instead of three. Over a plain socket that barely matters, but over
        // SslStream each separate Write is its own TLS record (encrypt + MAC + send) - three calls per batch
        // means paying that overhead three times, disproportionately so for the 8-byte header.
        byte[] buffer = ArrayPool<byte>.Shared.Rent(HeaderSize + 2 * maxBatchSizeInBytes);
        MemoryStream memoryStream = new MemoryStream(buffer, HeaderSize, buffer.Length - HeaderSize);

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

        BitConverter.TryWriteBytes(buffer.AsSpan(0, 4), blockSize);
        BitConverter.TryWriteBytes(buffer.AsSpan(4, 4), count);

        writer.Write(buffer, 0, HeaderSize + blockSize);

        ArrayPool<byte>.Shared.Return(buffer);

        int batches = 1;


        if (count < items.Length)
        {
            // Recursive call is used to handle large batches that exceed the maxBatchSizeInBytes
            batches += Serialize(writer, items.Slice(count), maxBatchSizeInBytes);
        }

        return batches;
    }
    public ICollection<FeedItem> Deserialize(BinaryReader reader)
    {
        int size = reader.ReadInt32();

        var count = reader.ReadInt32();

        if (size == 0 && count == 0)// end of stream
            return Array.Empty<FeedItem>();

        byte[] buffer = ArrayPool<byte>.Shared.Rent(2 * size);

        try
        {
            var remainingBytes = size;
            var offset = 0;
            while (remainingBytes > 0)
            {
                var bytes = reader.Read(buffer, offset, remainingBytes);

                // Stream.Read returns 0 on a graceful disconnect instead of throwing; without this check
                // a client that disconnects mid-batch would spin this loop forever instead of surfacing
                // as a failure the caller can react to.
                if (bytes == 0)
                    throw new EndOfStreamException("Client disconnected while reading a feed batch.");

                remainingBytes -= bytes;
                offset += bytes;
            }

            var memoryStream = new MemoryStream(buffer);

            var result = new List<FeedItem>(count);

            var memoryReader = new BinaryReader(memoryStream, Encoding.UTF8, true);
            for (int i = 0; i < count; i++)
            {
                var item = _serializer.Deserialize(memoryReader);
                result.Add(item);
            }

            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}