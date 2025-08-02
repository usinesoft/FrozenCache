namespace Messages;

public interface IBatchSerializer<TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Serializes a batch of messages into a stream.
    /// </summary>
    /// <param name="writer">The binary writer to write the serialized messages to.</param>
    /// <param name="messages">The messages to serialize.</param>
    /// <param name="maxBatchSizeInBytes">Maximum size for a serialized batch. If exceeded the messages are serializes in multiple batches</param>
    /// <returns>The number of binary messages written</returns>
    int Serialize(BinaryWriter writer, Span<TMessage> messages, int maxBatchSizeInBytes = 100_000);

    /// <summary>
    /// Deserializes a batch of messages from a stream.
    /// </summary>
    /// <param name="reader">The binary reader to read the serialized messages from.</param>
    /// <returns>An enumerable of deserialized messages.</returns>
    ICollection<TMessage> Deserialize(BinaryReader reader);
}