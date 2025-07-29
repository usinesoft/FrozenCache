using MessagePack;

namespace Messages;

[MessagePackObject]
public class BeginFeedRequest(string collectionName, string? collectionVersion) : IMessage
{
    [Key(0)]
    public string CollectionName { get; set; } = collectionName;

    [Key(1)]
    public string? CollectionVersion { get; set; } = collectionVersion;

    [IgnoreMember]
    public virtual MessageType Type => MessageType.BeginFeedRequest;

    public override string ToString()
    {
        return
            $"{nameof(CollectionName)}: {CollectionName}, {nameof(CollectionVersion)}: {CollectionVersion}, {nameof(Type)}: {Type}";
    }
}