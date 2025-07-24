using System.Buffers;
using MessagePack;

namespace Messages;

public static class StreamingHelper
{
    public static async Task WriteMessageAsync(this Stream stream, IMessage message, CancellationToken ct)
    {

        var header = ArrayPool<byte>.Shared.Rent(8);
        try
        {

            byte[] bytes;

            switch (message.Type)
            {
                case MessageType.Ping:
                    // Ping is a special case, it has no data
                    bytes = [];
                    break;

                case MessageType.BeginFeedRequest:
                    bytes = MessagePackSerializer.Serialize(message as BeginFeedRequest);
                    break;
                case MessageType.FeedItem:
                    bytes = MessagePackSerializer.Serialize(message as FeedItem);
                    break;
                case MessageType.EndFeedRequest:
                    bytes = MessagePackSerializer.Serialize(message as EndFeedRequest);
                    break;
                case MessageType.CreateCollectionRequest:
                    bytes = MessagePackSerializer.Serialize(message as CreateCollectionRequest);
                    break;
                case MessageType.StatusResponse:
                    bytes = MessagePackSerializer.Serialize(message as StatusResponse);
                    break;
                default:
                    throw new NotSupportedException("Unknown message type to stream");
            }

            

            BitConverter.TryWriteBytes(header.AsSpan(0, 4), (int)message.Type);
            BitConverter.TryWriteBytes(header.AsSpan(4, 4), bytes.Length);

            // for rented arrays, we need to ensure we write the exact size
            await stream.WriteAsync(header.AsMemory(0,8), ct);
            await stream.WriteAsync(bytes, ct);



        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
        }
            
    }

    public static async Task<IMessage> ReadMessageAsync(this Stream stream, CancellationToken ct)
    {

        var header = ArrayPool<byte>.Shared.Rent(8);
        try
        {
                
            await stream.ReadRawMessage(header,8, ct);
                
            MessageType messageType = (MessageType)BitConverter.ToInt32(header, 0);
            int size = BitConverter.ToInt32(header, 4);

            if (size < 0 || size > 1024 * 1024) // Arbitrary limit to prevent too large requests
            {
                throw new InvalidOperationException("Invalid request size.");
            }
                
            var buffer = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                await stream.ReadRawMessage(buffer,size, ct);
                    
                return messageType switch
                {
                    // ping is a special case (empty message)
                    MessageType.Ping => new PingMessage(),
                    MessageType.BeginFeedRequest => MessagePackSerializer.Deserialize<BeginFeedRequest>(buffer.AsMemory(0, size)),
                    MessageType.FeedItem => MessagePackSerializer.Deserialize<FeedItem>(buffer),
                    MessageType.EndFeedRequest => MessagePackSerializer.Deserialize<EndFeedRequest>(buffer),
                    MessageType.CreateCollectionRequest => MessagePackSerializer.Deserialize<CreateCollectionRequest>(buffer),
                    MessageType.StatusResponse => MessagePackSerializer.Deserialize<StatusResponse>(buffer),
                    _ => throw new InvalidOperationException($"Unknown message type: {messageType}")
                };
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
        }

    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="buffer"></param>
    /// <param name="size"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    private static async Task ReadRawMessage(this Stream stream, byte[] buffer, int size, CancellationToken ct)
    {
        int totalBytesRead = 0;
        
        while (totalBytesRead < size)
        {
            int read = await stream.ReadAsync(buffer, totalBytesRead, size - totalBytesRead, ct);
            if (read == 0)
            {
                throw new InvalidOperationException(
                    "Client disconnected while reading BeginFeedRequest data.");
            }

            totalBytesRead += read;
        }
    }


}