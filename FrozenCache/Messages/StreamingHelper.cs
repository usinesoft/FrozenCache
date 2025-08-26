using System.Buffers;
using System.Diagnostics;
using MessagePack;
#pragma warning disable S6966

namespace Messages;

public static class StreamingHelper
{

    [ThreadStatic]
    static MemoryStream? _memoryStream;

    [ThreadStatic]
    static byte[]? _buffer;

    public static async Task WriteMessageAsync(this Stream stream, IMessage message, CancellationToken ct)
    {

        if (_memoryStream == null)
        {
            _buffer = new byte[1024 * 1024];
            _memoryStream = new MemoryStream(_buffer);
        }
        

        _memoryStream.Seek(0, SeekOrigin.Begin);

        var header = ArrayPool<byte>.Shared.Rent(8);
        try
        {

            

            switch (message.Type)
            {
                case MessageType.Ping:
                    // Ping is a special case, it has no data
                    break;

                case MessageType.BeginFeedRequest:
                    MessagePackSerializer.Serialize(_memoryStream, message as BeginFeedRequest);
                    break;
                case MessageType.CreateCollectionRequest:
                    MessagePackSerializer.Serialize(_memoryStream, message as CreateCollectionRequest);
                    break;
                case MessageType.DropCollectionRequest:
                    MessagePackSerializer.Serialize(_memoryStream, message as DropCollectionRequest);
                    break;
                case MessageType.StatusResponse:
                    MessagePackSerializer.Serialize(_memoryStream, message as StatusResponse);
                    break;
                case MessageType.QueryByPrimaryKeyRequest:
                    MessagePackSerializer.Serialize(_memoryStream, message as QueryByPrimaryKey);
                    break;
                case MessageType.QueryResponse:
                    MessagePackSerializer.Serialize(_memoryStream, message as ResultWithData);
                    break;
                default:
                    throw new NotSupportedException("Unknown message type to stream");
            }

            

            BitConverter.TryWriteBytes(header.AsSpan(0, 4), (int)message.Type);

            var size = message.Type == MessageType.Ping? 0:(int)_memoryStream.Position;

            BitConverter.TryWriteBytes(header.AsSpan(4, 4), size);

            // for rented arrays, we need to ensure we write the exact size
            await stream.WriteAsync(header.AsMemory(0,8), ct);
            await stream.WriteAsync(_buffer.AsMemory(0, size), ct);



        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
        }
            
    }

    public static async ValueTask<IMessage?> ReadMessageAsync(this Stream stream, CancellationToken ct)
    {

        var header = ArrayPool<byte>.Shared.Rent(8);
        try
        {
                
            await stream.ReadRawMessage(header,8, ct);
                
            MessageType messageType = (MessageType)BitConverter.ToInt32(header, 0);

            Debug.Print($"message received:{Enum.GetName(typeof(MessageType), messageType)}");

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
                    MessageType.CreateCollectionRequest => MessagePackSerializer.Deserialize<CreateCollectionRequest>(buffer),
                    MessageType.DropCollectionRequest => MessagePackSerializer.Deserialize<DropCollectionRequest>(buffer),
                    MessageType.StatusResponse => MessagePackSerializer.Deserialize<StatusResponse>(buffer),
                    MessageType.QueryByPrimaryKeyRequest => MessagePackSerializer.Deserialize<QueryByPrimaryKey>(buffer),
                    MessageType.QueryResponse => MessagePackSerializer.Deserialize<ResultWithData>(buffer),
                    _ => throw new InvalidOperationException($"Unknown message type: {messageType}")
                };
            }
            catch(Exception)
            {
                // connection closed, ignore
                return null;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

        }
        catch (Exception)
        {
            // connection closed, ignore
            return null;
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
    private static async ValueTask ReadRawMessage(this Stream stream, byte[] buffer, int size, CancellationToken ct)
    {
        int totalBytesRead = 0;
        
        while (totalBytesRead < size)
        {
            int read = await stream.ReadAsync(buffer, totalBytesRead, size - totalBytesRead, ct);
            if (read == 0)
            {
                throw new InvalidOperationException(
                    "Client disconnected while reading data.");
            }

            totalBytesRead += read;
        }
    }


}