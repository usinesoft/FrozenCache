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

    private Dictionary<string, CollectionMetadata> _metadataByCollection = new();


    public DataStore(string rootPath, IndexType primaryIndexType)
    {
        RootPath = rootPath;
        PrimaryIndexType = primaryIndexType;

        if (!Directory.Exists(RootPath)) Directory.CreateDirectory(RootPath);
    }

    private IndexType PrimaryIndexType { get; }

    public static void Drop(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, true);
    }

    private string RootPath { get; }


    private static readonly string MetadataFileName = "metadata.json";

    public bool CreateCollection(CollectionMetadata metadata, int maxVersionToKeep = 2)
    {
        var path = Path.Combine(RootPath, metadata.Name);

        if (Directory.Exists(path))
        {
            var jsonMetadata = File.ReadAllText(Path.Combine(path, MetadataFileName));


            var currentMetadata =
                JsonSerializer.Deserialize<CollectionMetadata>(jsonMetadata,
                    AppJsonSerializerContext.Default.CollectionMetadata) ??
                throw new CacheException("Failed to deserialize existing collection metadata");
            if (!currentMetadata.IsCompatibleWith(metadata))
                throw new CacheException(
                    $"Collection {metadata.Name} already exists with different schema.");

            // if the collection already exists, and it is compatible we ignore the request
            return false;
        }

        Directory.CreateDirectory(path);
        //metadata is stored as json in the collection root path
        var json = JsonSerializer.Serialize(metadata, AppJsonSerializerContext.Default.CollectionMetadata);
        File.WriteAllText(Path.Combine(path, MetadataFileName), json);

        LoadCollectionsMetadata();
        return true;
    }


    private void LoadCollectionsMetadata()
    {
        var collectionDirs = Directory.EnumerateDirectories(RootPath).ToList();

        var collections = new CollectionMetadata[collectionDirs.Count];

        for (var i = 0; i < collectionDirs.Count; i++)
        {
            var dir = collectionDirs[i];
            var metadataPath = Path.Combine(dir, MetadataFileName);
            if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {dir}");
            var json = File.ReadAllText(metadataPath);
            collections[i] =
                JsonSerializer.Deserialize<CollectionMetadata>(json,
                    AppJsonSerializerContext.Default.CollectionMetadata) ??
                throw new CacheException("Failed to deserialize collection metadata");

            // get available versions; a version without the completion marker is a feed that never
            // finished (crashed/disconnected client) and must not be surfaced
            var versions = Directory.EnumerateDirectories(dir)
                .Where(IsVersionComplete)
                .Select(Path.GetFileName)
                .Where(name => name != MetadataFileName)
                .OrderBy(x => x)
                .ToList();

            if (versions.Count > 0)
                collections[i].LastVersion = versions[^1];
        }

        _metadataByCollection = collections.ToDictionary(c => c.Name, c => c);
    }

    /// <summary>
    /// A version directory is only valid once its data has been fully written, flushed and indexed -
    /// signaled by the presence of the <see cref="Consts.CompletedMarkerDirectoryName" /> marker. Its absence
    /// means the feed that created it never completed.
    /// </summary>
    private static bool IsVersionComplete(string versionDirectoryPath)
    {
        return Directory.Exists(Path.Combine(versionDirectoryPath, Consts.CompletedMarkerDirectoryName));
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

        LoadCollectionsMetadata();
    }


    private bool _opened;


    public void Open(ILogger? logger = null)
    {
        if (_opened)
            throw new CacheException("DataStore is already opened");

        _opened = true;

        _metadataByCollection.Clear();

        Directory.EnumerateDirectories(RootPath).ToList().ForEach(dir =>
        {
            var metadataPath = Path.Combine(dir, MetadataFileName);

            if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {dir}");

            var json = File.ReadAllText(metadataPath);

            var metadata =
                JsonSerializer.Deserialize<CollectionMetadata>(json,
                    AppJsonSerializerContext.Default.CollectionMetadata) ??
                throw new CacheException("Failed to deserialize collection metadata");

            var allVersionsDirectories = Directory.EnumerateDirectories(dir)
                .OrderBy(x => x)
                .ToList();

            var completeVersionsDirectories = allVersionsDirectories.Where(IsVersionComplete).ToList();

            // proactively remove versions left behind by a feed that never completed (crashed/disconnected
            // client): they are missing the completion marker and must not linger on disk
            foreach (var incompleteVersionDirectory in allVersionsDirectories.Except(completeVersionsDirectories))
            {
                logger?.LogWarning(
                    "Removing incomplete version {Version} of collection {CollectionName}: the feed that created it never completed",
                    Path.GetFileName(incompleteVersionDirectory), metadata.Name);

                Directory.Delete(incompleteVersionDirectory, true);
            }

            var lastVersion = completeVersionsDirectories.Count > 0
                ? Path.GetFileName(completeVersionsDirectories[^1])
                : null;

            metadata.LastVersion = lastVersion;

            _metadataByCollection[metadata.Name] = metadata;

            logger?.LogInformation("Opening collection {CollectionName} with last version {LastVersion}",
                metadata.Name, lastVersion);

            if (completeVersionsDirectories.Count > 0)
                _collectionStores[metadata.Name] =
                    new CollectionStore(completeVersionsDirectories[^1], metadata.Indexes.Count, logger,
                        metadata.FileSize, metadata.MaxItemsInFile, PrimaryIndexType);

            logger?.LogInformation("Collection {CollectionName} opened with last version {LastVersion}",
                metadata.Name, lastVersion);
        });
    }

    public List<Item> GetByPrimaryKey(string collectionName, long keyValue)
    {
        return _collectionStores[collectionName].GetByFirstKey(keyValue);
    }

    private CollectionStore BeginFeed(string collectionName, string newVersion)
    {
        if (!_opened)
            throw new CacheException("DataStore is not opened. Call Open() before feeding collections.");

        var path = Path.Combine(RootPath, collectionName);

        if (!Directory.Exists(path))
            throw new CacheException($"Collection {collectionName} not found. Call CreateCollection()");

        var metadataPath = Path.Combine(path, MetadataFileName);
        if (!File.Exists(metadataPath)) throw new CacheException($"Metadata file not found in {path}");

        var json = File.ReadAllText(metadataPath);

        var collectionMetadata =
            JsonSerializer.Deserialize<CollectionMetadata>(json, AppJsonSerializerContext.Default.CollectionMetadata) ??
            throw new CacheException("Failed to deserialize collection metadata");

        var versionPath = Path.Combine(path, newVersion);
        if (Directory.Exists(versionPath))
            throw new CacheException($"Version {newVersion} already exits");

        var collectionStore = new CollectionStore(versionPath, collectionMetadata.Indexes.Count,
            null, collectionMetadata.FileSize, collectionMetadata.MaxItemsInFile, PrimaryIndexType);

        return collectionStore;
    }

    private void EndFeed(CollectionStore collectionStore, string collectionName)
    {
        if (collectionStore == null) throw new ArgumentNullException(nameof(collectionStore));

        // flush data to disk and build the index
        collectionStore.EndOfFeed();

        // only now that data and index are safely on disk can this version be marked as valid
        Directory.CreateDirectory(Path.Combine(collectionStore.StoragePath, Consts.CompletedMarkerDirectoryName));

        // replace previous version if any
        if (_collectionStores.TryGetValue(collectionName, out var store)) store.Dispose();
        _collectionStores[collectionName] = collectionStore;

        LoadCollectionsMetadata();
    }

    public int FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        var collectionStore = BeginFeed(collectionName, newVersion);

        var itemsCount = 0;
        try
        {
            foreach (var item in items)
            {
                collectionStore.StoreNewDocument(item);
                itemsCount++;
            }
        }
        catch
        {
            AbortFeed(collectionStore);
            throw;
        }

        EndFeed(collectionStore, collectionName);

        return itemsCount;
    }

    public async Task<int> FeedCollectionAsync(string collectionName, string newVersion, IAsyncEnumerable<Item> items)
    {
        var collectionStore = BeginFeed(collectionName, newVersion);

        var itemsCount = 0;
        try
        {
            await foreach (var item in items)
            {
                collectionStore.StoreNewDocument(item);
                itemsCount++;
            }
        }
        catch
        {
            AbortFeed(collectionStore);
            throw;
        }

        EndFeed(collectionStore, collectionName);

        return itemsCount;
    }

    /// <summary>
    ///     Discards a collection store that failed while being fed: releases its file handles and deletes the
    ///     partially-written version directory so it doesn't linger as a broken, half-indexed version.
    /// </summary>
    private static void AbortFeed(CollectionStore collectionStore)
    {
        var storagePath = collectionStore.StoragePath;
        collectionStore.Dispose();

        if (Directory.Exists(storagePath))
            Directory.Delete(storagePath, true);
    }

    public CollectionsDescription GetCollectionInformation()
    {
        var description = new CollectionsDescription
        {
            Collections = new CollectionInformation[_collectionStores.Count]
        };


        var index = 0;
        foreach (var store in _collectionStores)
        {
            var collectionName = store.Key;

            var collectionMetadata = _metadataByCollection[collectionName];

            var collectionInfo = new CollectionInformation
            {
                Count = store.Value.ObjectCount,
                Keys = collectionMetadata.Indexes.Select(i => i.Name).ToArray(),
                LastVersion = collectionMetadata.LastVersion!,
                MaxObjectsPerSegment = collectionMetadata.MaxItemsInFile,
                SegmentFileSize = collectionMetadata.FileSize,
                SizeInBytes = store.Value.TotalSizeInBytes,
                Name = collectionName
            };

            description.Collections[index++] = collectionInfo;
        }

        return description;
    }

    public CollectionMetadata? GetCollectionMetadata(string collectionName)
    {
        return _metadataByCollection.GetValueOrDefault(collectionName);
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