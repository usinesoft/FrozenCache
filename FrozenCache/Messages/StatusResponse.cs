using MessagePack;

namespace Messages;

/// <summary>
/// Response containing only a status. No data
/// If not successful it also contains an error message
/// </summary>
[MessagePackObject]
public class StatusResponse : IMessage
{
    [Key(0)]
    public bool Success { get; set; } = true;

    [Key(1)]
    public string? ErrorMessage { get; set; }

    [IgnoreMember]
    public MessageType Type => MessageType.StatusResponse;

    public override string ToString()
    {
        return $"{nameof(Success)}: {Success}, {nameof(ErrorMessage)}: {ErrorMessage}, {nameof(Type)}: {Type}";
    }
}