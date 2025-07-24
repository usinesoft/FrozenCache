using System.Text.Json;

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
        var json = JsonSerializer.Serialize(metadata);
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
            collections[i] = JsonSerializer.Deserialize<CollectionMetadata>(json) ??
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

    public void Open()
    {
        if (_opened)
            throw new CacheException("DataStore is already opened");

        _opened = true;

        Directory.EnumerateDirectories(RootPath).ToList().ForEach(dir =>
        {
            var metadataPath = Path.Combine(dir, "metadata.json");
            if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {dir}");
            var json = File.ReadAllText(metadataPath);
            var metadata = JsonSerializer.Deserialize<CollectionMetadata>(json) ??
                           throw new CacheException("Failed to deserialize collection metadata");

            var allVersionsDirectories = Directory.EnumerateDirectories(dir)
                .OrderBy(x => x)
                .ToList();


            if (allVersionsDirectories.Count > 0)
                _collectionStores[metadata.Name] =
                    new CollectionStore(allVersionsDirectories[^1], metadata.Indexes.Count);
        });
    }

    public Item? GetByPrimaryKey(string collectionName, long keyValue)
    {
        return _collectionStores[collectionName].GetByFirstKey(keyValue);
    }

    public void FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        if (!_opened)
            throw new CacheException("DataStore is not opened. Call Open() before feeding collections.");

        var path = Path.Combine(RootPath, collectionName);

        if (!Directory.Exists(path))
            throw new CacheException($"Collection {collectionName} not found. Call CreateCollection()");

        var metadataPath = Path.Combine(path, "metadata.json");
        if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {path}");

        var json = File.ReadAllText(metadataPath);
        var collectionMetadata = JsonSerializer.Deserialize<CollectionMetadata>(json) ??
                                 throw new CacheException("Failed to deserialize collection metadata");

        var versionPath = Path.Combine(path, newVersion);
        if (Directory.Exists(versionPath))
            throw new CacheException($"Version {newVersion} already exits");

        var collectionStore = new CollectionStore(versionPath, collectionMetadata.Indexes.Count,
            collectionMetadata.FileSize, collectionMetadata.MaxItemsInFile);

        foreach (var item in items) collectionStore.StoreNewDocument(item);

        collectionStore.EndOfFeed();

        // replace previous version if any
        if (_collectionStores.TryGetValue(collectionName, out var store)) store.Dispose();
        _collectionStores[collectionName] = collectionStore;
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