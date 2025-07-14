using System.Text.Json.Serialization;

namespace PersistentStore;

/// <summary>
/// An object stored in a collection. It contains raw data and a set of keys that can be used to retrieve it.
/// </summary>
public class Item(byte[] data, params long[] keys)
{
    public long[] Keys { get; private set; } = keys;

    public byte[] Data { get; private set; } = data;
}

[JsonSerializable(typeof(CollectionMetadata))]
[JsonSerializable(typeof(IndexMetadata))]
internal partial class MyJsonSerializerContext : JsonSerializerContext
{

}