using System.Text.Json.Serialization;

namespace PersistentStore;

public class CollectionMetadata
{
    public CollectionMetadata(string name, string primaryIndexName, params string[] otherIndexes)
    {
        Name = name;

        Indexes.Add(new IndexMetadata(primaryIndexName, true));

        foreach (var index in otherIndexes)
        {
            Indexes.Add(new IndexMetadata(index));
        }
    }

    /// <summary>
    /// Constructor for serialization purposes.
    /// </summary>
    [JsonConstructor]
    public CollectionMetadata(string name, List<IndexMetadata> indexes, string? lastVersion)
    {
        Name = name;
        Indexes = indexes;
        LastVersion = lastVersion;
    }

    public List<IndexMetadata> Indexes { get;  } = [];

    public string Name { get; }
    
    public string? LastVersion { get;}
}