using System.Text.Json;

namespace PersistentStore
{


    public record IndexMetadata(string Name,bool IsUnique = false);

    /// <summary>
    /// A store contains indexed persistent collections of items.
    /// </summary>
    public sealed  class DataStore : IDataStore, IAsyncDisposable, IDisposable
    {

        private readonly Dictionary<string, CollectionStore> _collectionStores = new();

        public DataStore(string rootPath)
        {
            RootPath = rootPath;

            if (!Directory.Exists(RootPath))
            {
                Directory.CreateDirectory(RootPath);
            }
        }

        public static void Drop(string path)
        {
            if(Directory.Exists(path))
                Directory.Delete(path, true);
        }

        public string RootPath { get; }

        public void CreateCollection(CollectionMetadata metadata, int maxVersionToKeep = 2)
        {
            var path = Path.Combine(RootPath, metadata.Name);

            if (Directory.Exists(path))
            {
                throw new CacheException("Collection already exists");
            }

            Directory.CreateDirectory(path);

            //metadata is stored as json in the collection root path
            var json = JsonSerializer.Serialize(metadata);
            File.WriteAllText(Path.Combine(path, "metadata.json"), json);

            _collectionStores[metadata.Name] = new CollectionStore(path);
        }

        public CollectionMetadata[] GetCollections()
        {

            var collectionDirs = Directory.EnumerateDirectories(RootPath).ToList();

            var collections = new CollectionMetadata[collectionDirs.Count];

            for (int i = 0; i < collectionDirs.Count; i++)
            {
                var dir = collectionDirs[i];
                var metadataPath = Path.Combine(dir, "metadata.json");
                if (!File.Exists(metadataPath))
                {
                    throw new CacheException($"Metadata file not found in {dir}");
                }
                var json = File.ReadAllText(metadataPath);
                collections[i] = JsonSerializer.Deserialize<CollectionMetadata>(json) ?? throw new CacheException("Failed to deserialize collection metadata");
            }
            return collections;

        }

        public void DropCollection(string name)
        {
            if(_collectionStores.TryGetValue(name, out var store))
            {
                store.Dispose();
                _collectionStores.Remove(name);
            }
            else
            {
                throw new CacheException("Collection not found");
            }

            var path = Path.Combine(RootPath, name);

            if (!Directory.Exists(path))
            {
                throw new CacheException("Collection not found");
            }

            Directory.Delete(path, true);
        }

        public void Open()
        {
            throw new NotImplementedException();
        }

        public Item GetByPrimaryKey(string collectionName, long keyValue)
        {
            throw new NotImplementedException();
        }

        public void FeedCollection(string collectionName, IEnumerable<Item> items)
        {
            throw new NotImplementedException();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var collectionStore in _collectionStores.Values)
            {
                await collectionStore.DisposeAsync();
            }
        }

        public void Dispose()
        {
            foreach (var collectionStore in _collectionStores.Values)
            {
                collectionStore.Dispose();
            }
        }
    }
}
