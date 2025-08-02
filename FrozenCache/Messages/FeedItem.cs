using MessagePack;

namespace Messages;

[MessagePackObject]
public class FeedItem : IMessage
{
    [Key(0)] public long[] Keys { get; set; } = [];

    [Key(1)] public byte[] Data { get; set; } = [];

    [IgnoreMember]
    public bool IsEndOfStream => Data.Length == 0 && Keys.Length == 0;


    [IgnoreMember]
    public MessageType Type => MessageType.FeedItem;

    public override string ToString()
    {
        return $"{nameof(Keys)}: {Keys}, {nameof(Data)}: {Data}, {nameof(Type)}: {Type}";
    }

}