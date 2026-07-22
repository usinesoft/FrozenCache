using System.Text.Json.Serialization;

namespace PersistentStore;



[JsonSerializable(typeof(CollectionMetadata))]
[JsonSerializable(typeof(IndexMetadata))]
internal partial class MyJsonSerializerContext : JsonSerializerContext
{

}