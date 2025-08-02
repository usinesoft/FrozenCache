namespace Messages;

/// <summary>
/// Messages sent as massive collections implement custom serialization for speed
/// </summary>
/// <typeparam name="TMessage"></typeparam>
public interface IBinarySerializer<TMessage> where TMessage : IMessage
{
    TMessage Deserialize(BinaryReader reader);

    void Serialize(BinaryWriter writer, TMessage message);
}