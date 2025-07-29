using MessagePack;

namespace Messages;

/// <summary>
/// The result of a query can be sent as a single message or as a sequence of messages with an end marker
/// We can query multiple collections at once, so the result contains a collection name.
/// </summary>
[MessagePackObject]
public class ResultWithData:IMessage
{
    [Key(0)]
    public bool SingleAnswer { get; set; }
    
    [Key(1)]
    public byte[][] ObjectsData { get; set; }= [];

    [Key(2)]
    public string? CollectionName { get; set; }

    [IgnoreMember]
    public bool IsEndMarker => ObjectsData.Length == 0;

    public override string ToString()
    {
        return
            $"{nameof(SingleAnswer)}: {SingleAnswer}, {nameof(ObjectsData)}: {ObjectsData.Length}, {nameof(CollectionName)}: {CollectionName}, {nameof(IsEndMarker)}: {IsEndMarker}";
    }

    [IgnoreMember]
    public MessageType Type => MessageType.QueryResponse;
}