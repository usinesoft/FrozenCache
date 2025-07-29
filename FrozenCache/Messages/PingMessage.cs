namespace Messages
{
    /// <summary>
    /// Empty message (no need to be serializable)
    /// </summary>
    public class PingMessage : IMessage
    {
        public MessageType Type => MessageType.Ping;
        public override string ToString()
        {
            return "Ping";
        }
    }
}
