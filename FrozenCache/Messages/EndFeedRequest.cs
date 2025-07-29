using MessagePack;

namespace Messages;

[MessagePackObject]
public class EndFeedRequest : IMessage
{

    [IgnoreMember]
    public MessageType Type => MessageType.EndFeedRequest;


    [Key(0)]
    public bool Success { get; set; } = true;

    public override string ToString()
    {
        return $"{nameof(Type)}: {Type}, {nameof(Success)}: {Success}";
    }
}