using MessagePack;

namespace Messages;

/// <summary>
/// Request to stream every document currently in a collection's active version. On success, the server
/// answers with a <see cref="StatusResponse"/> acknowledging the request, then streams the documents using the
/// same manual batch framing as a feed session (<see cref="FeedItemBatchSerializer"/>), terminated by an empty
/// batch.
/// </summary>
[MessagePackObject]
public class StreamAllDataRequest : IMessage
{
    [Key(0)]
    public string CollectionName { get; set; } = string.Empty;

    [IgnoreMember]
    public MessageType Type => MessageType.StreamAllDataRequest;

    public override string ToString()
    {
        return $"{nameof(CollectionName)}: {CollectionName}, {nameof(Type)}: {Type}";
    }
}
