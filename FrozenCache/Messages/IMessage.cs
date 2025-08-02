using MessagePack;

namespace Messages;


public interface IMessage
{
    public MessageType Type { get; }
}