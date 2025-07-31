using MessagePack;


namespace Messages;

[MessagePackObject]
public class DropCollectionRequest : IMessage
{
    [IgnoreMember]
    public MessageType Type => MessageType.DropCollectionRequest;

    [Key(0)]
    public string CollectionName { get; set; } = string.Empty;
    public override string ToString()
    {
        return $"{nameof(Type)}: {Type}, {nameof(CollectionName)}: {CollectionName}";
    }
}