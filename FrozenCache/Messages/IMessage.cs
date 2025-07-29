using MessagePack;

namespace Messages;

[Union(0, typeof(BeginFeedRequest))]
[Union(1, typeof(FeedItem))]
[Union(2, typeof(FeedItem))]
public interface IMessage
{
    public MessageType Type { get; }
}