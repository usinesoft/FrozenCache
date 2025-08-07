using System.Collections;
using System.Text.Json;
using System.Text.Json.Serialization;
using Messages;
using Microsoft.Extensions.Logging;

namespace PersistentStore;

/// <summary>
///     A store contains indexed persistent collections of items.
/// </summary>
public sealed class DataStore : IDataStore, IAsyncDisposable, IDisposable
{
    private readonly Dictionary<string, CollectionStore> _collectionStores = new();

    public DataStore(string rootPath)
    {
        RootPath = rootPath;

        if (!Directory.Exists(RootPath)) Directory.CreateDirectory(RootPath);
    }

    public static void Drop(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, true);
    }

    public string RootPath { get; }

    public void CreateCollection(CollectionMetadata metadata, int maxVersionToKeep = 2)
    {
        var path = Path.Combine(RootPath, metadata.Name);

        if (Directory.Exists(path)) throw new CacheException("Collection already exists");

        Directory.CreateDirectory(path);

        //metadata is stored as json in the collection root path
        var json = JsonSerializer.Serialize(metadata, AppJsonSerializerContext.Default.CollectionMetadata);
        File.WriteAllText(Path.Combine(path, "metadata.json"), json);
    }

    public CollectionMetadata[] GetCollections()
    {
        var collectionDirs = Directory.EnumerateDirectories(RootPath).ToList();

        var collections = new CollectionMetadata[collectionDirs.Count];

        for (var i = 0; i < collectionDirs.Count; i++)
        {
            var dir = collectionDirs[i];
            var metadataPath = Path.Combine(dir, "metadata.json");
            if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {dir}");
            var json = File.ReadAllText(metadataPath);
            collections[i] = JsonSerializer.Deserialize<CollectionMetadata>(json, AppJsonSerializerContext.Default.CollectionMetadata) ??
                             throw new CacheException("Failed to deserialize collection metadata");

            // get available versions
            var versions = Directory.EnumerateDirectories(dir)
                .Select(Path.GetFileName)
                .Where(name => name != "metadata.json")
                .OrderBy(x => x)
                .ToList();

            if (versions.Count > 0)
                collections[i].LastVersion = versions[^1];
        }

        return collections;
    }

    public void DropCollection(string name)
    {
        if (_collectionStores.TryGetValue(name, out var store))
        {
            store.Dispose();
            _collectionStores.Remove(name);
        }


        var path = Path.Combine(RootPath, name);

        if (!Directory.Exists(path)) throw new CacheException("Collection not found");

        Directory.Delete(path, true);
    }


    private bool _opened;


    public void Open(ILogger? logger = null)
    {
        if (_opened)
            throw new CacheException("DataStore is already opened");

        _opened = true;

        Directory.EnumerateDirectories(RootPath).ToList().ForEach(dir =>
        {
            var metadataPath = Path.Combine(dir, "metadata.json");
            if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {dir}");
            var json = File.ReadAllText(metadataPath);
            var metadata = JsonSerializer.Deserialize<CollectionMetadata>(json, AppJsonSerializerContext.Default.CollectionMetadata) ??
                           throw new CacheException("Failed to deserialize collection metadata");

            var allVersionsDirectories = Directory.EnumerateDirectories(dir)
                .OrderBy(x => x)
                .ToList();

            string? lastVersion = allVersionsDirectories.Count > 0
                ? Path.GetFileName(allVersionsDirectories[^1])
                : null;

            logger?.LogInformation("Opening collection {CollectionName} with last version {LastVersion}",
                metadata.Name, lastVersion);

            if (allVersionsDirectories.Count > 0)
                _collectionStores[metadata.Name] =
                    new CollectionStore(allVersionsDirectories[^1], metadata.Indexes.Count, logger);

            logger?.LogInformation("Collection {CollectionName} opened with last version {LastVersion}",
                metadata.Name, lastVersion);


        });
    }

    public List<Item> GetByPrimaryKey(string collectionName, long keyValue)
    {
        return _collectionStores[collectionName].GetByFirstKey(keyValue);
    }

    public CollectionStore BeginFeed(string collectionName, string newVersion)
    {
        if (!_opened)
            throw new CacheException("DataStore is not opened. Call Open() before feeding collections.");

        var path = Path.Combine(RootPath, collectionName);

        if (!Directory.Exists(path))
            throw new CacheException($"Collection {collectionName} not found. Call CreateCollection()");

        var metadataPath = Path.Combine(path, "metadata.json");
        if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {path}");

        var json = File.ReadAllText(metadataPath);

        var collectionMetadata = JsonSerializer.Deserialize<CollectionMetadata>(json, AppJsonSerializerContext.Default.CollectionMetadata) ??
                                 throw new CacheException("Failed to deserialize collection metadata");

        var versionPath = Path.Combine(path, newVersion);
        if (Directory.Exists(versionPath))
            throw new CacheException($"Version {newVersion} already exits");

        var collectionStore = new CollectionStore(versionPath, collectionMetadata.Indexes.Count,
            null, collectionMetadata.FileSize, collectionMetadata.MaxItemsInFile);

        return collectionStore;
    }

    public void EndFeed(CollectionStore collectionStore, string collectionName)
    {
        if (collectionStore == null) throw new ArgumentNullException(nameof(collectionStore));

        // create indexes
        collectionStore.EndOfFeed();

        // replace previous version if any
        if (_collectionStores.TryGetValue(collectionName, out var store)) store.Dispose();
        _collectionStores[collectionName] = collectionStore;

    }

    public int FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        var collectionStore = BeginFeed(collectionName, newVersion);

        int itemsCount = 0;
        foreach (var item in items)
        {
            collectionStore.StoreNewDocument(item);
            itemsCount++;
        }

        EndFeed(collectionStore, collectionName);

        return itemsCount;
    }

    public async Task<int> FeedCollectionAsync(string collectionName, string newVersion, IAsyncEnumerable<Item> items)
    {
        var collectionStore = BeginFeed(collectionName, newVersion);

        int itemsCount = 0;
        await foreach (var item in items)
        {
            collectionStore.StoreNewDocument(item);
            itemsCount++;
        }

        EndFeed(collectionStore, collectionName);

        return itemsCount;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var collectionStore in _collectionStores.Values) await collectionStore.DisposeAsync();
    }

    public void Dispose()
    {
        foreach (var collectionStore in _collectionStores.Values) collectionStore.Dispose();
    }

    
}

[JsonSerializable(typeof(CollectionMetadata))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{

}
