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
    
    public string? LastVersion { get; set; }

    /// <summary>
    /// Maximum number of items that can be stored in a single file. A collection is split into multiple files if it exceeds this limit.
    /// </summary>
    public int MaxItemsInFile { get; set; } = 1_000_000;

    /// <summary>
    /// Maximum size of a file in bytes. If a collection exceeds this size, it will be split into multiple files.
    /// </summary>
    public int FileSize { get; set; } = 1_000_000_000;

    /// <summary>
    /// Maximum number of versions to keep for this collection. When a new version is created, the oldest version will be deleted if the limit is exceeded.
    /// </summary>
    public int MaxVersionsToKeep { get; set; } = 2;
}

public record IndexMetadata(string Name, bool IsUnique = false);