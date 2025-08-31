namespace Messages;

/// <summary>
/// Request to get the description of all collections
/// Empty message (no need to be serializable)
/// </summary>
public class GetCollectionsDescriptionRequest : IMessage
{
    public MessageType Type => MessageType.GetCollectionsDescriptionRequest;
    public override string ToString()
    {
        return "GetCollectionsDescriptionRequest";
    }
}