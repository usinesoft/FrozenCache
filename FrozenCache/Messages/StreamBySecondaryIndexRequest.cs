using MessagePack;

namespace Messages;

/// <summary>
/// Request to stream every document in a collection's active version whose value at a named index equals
/// <see cref="KeyValue"/>. Works for any declared index (primary or secondary), though it exists mainly for
/// secondary indexes. Same response shape as <see cref="StreamAllDataRequest"/>: the server acknowledges with
/// a <see cref="StatusResponse"/>, then streams documents using the same manual batch framing as a feed
/// session (<see cref="FeedItemBatchSerializer"/>), terminated by an empty batch.
/// </summary>
[MessagePackObject]
public class StreamBySecondaryIndexRequest : IMessage
{
    [Key(0)]
    public string CollectionName { get; set; } = string.Empty;

    [Key(1)]
    public string IndexName { get; set; } = string.Empty;

    [Key(2)]
    public long KeyValue { get; set; }

    [IgnoreMember]
    public MessageType Type => MessageType.StreamBySecondaryIndexRequest;

    public override string ToString()
    {
        return $"{nameof(CollectionName)}: {CollectionName}, {nameof(IndexName)}: {IndexName}, " +
               $"{nameof(KeyValue)}: {KeyValue}, {nameof(Type)}: {Type}";
    }
}
