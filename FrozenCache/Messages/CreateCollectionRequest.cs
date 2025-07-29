using MessagePack;

namespace Messages;

[MessagePackObject]
public class CreateCollectionRequest : IMessage
{
    [IgnoreMember]
    public MessageType Type => MessageType.CreateCollectionRequest;

    [Key(0)]
    public string CollectionName { get; set; } = string.Empty;

    [Key(1)]
    public string? PrimaryKeyName { get; set; } = null;

    [Key(2)]
    public string[] OtherIndexes { get; set; } = [];

    public override string ToString()
    {
        return
            $"{nameof(Type)}: {Type}, {nameof(CollectionName)}: {CollectionName}, {nameof(PrimaryKeyName)}: {PrimaryKeyName}, {nameof(OtherIndexes)}: {OtherIndexes}";
    }
}