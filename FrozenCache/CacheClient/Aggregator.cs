using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Channels;
using CacheClient.LocalCache;
using Messages;
using PersistentStore;

namespace CacheClient;

/// <summary>
/// Aggregates multiple connectors to different replicas of the cache server.
/// </summary>
public class Aggregator
{
    private readonly List<ConnectorPool> _pools = [];

    private readonly Dictionary<string, LruLocalCache> _localCaches = new();

    /// <summary>
    /// Raised whenever any replica reports that a collection's last version has changed. The corresponding
    /// local cache (if any) has already been cleared, and <see cref="GetLastVersion"/>/
    /// <see cref="GetPoolsWithLastVersion"/> already reflect the change, by the time this fires.
    /// </summary>
    public event EventHandler<NewVersionEventArgs>? NewVersion;

    private readonly object _versionMapLock = new();

    /// <summary>
    /// Highest version known across all replicas, by collection name. Membership of "which servers have it" is
    /// deliberately not tracked here as a separate set - it's derived on demand from each pool's own
    /// <see cref="ConnectorPool.LastKnownVersions"/> in <see cref="GetPoolsWithLastVersion"/>, so there is a
    /// single source of truth per pool instead of two copies of the same fact that could drift apart.
    /// </summary>
    private readonly Dictionary<string, string> _clusterLastVersion = new();

    private void OnPoolNewVersion(object? sender, NewVersionEventArgs e)
    {
        if (_localCaches.TryGetValue(e.CollectionName, out var cache))
            cache.Clear();

        lock (_versionMapLock)
        {
            AdvanceClusterLastVersionLocked(e.CollectionName, e.NewVersion);
        }

        NewVersion?.Invoke(this, e);
    }

    /// <summary>
    /// Recomputes the cluster-wide last version for every collection from what each pool currently knows.
    /// Called once, right after all pools are connected, to build the initial picture; ongoing changes are
    /// then folded in incrementally by <see cref="OnPoolNewVersion"/> as replicas report them.
    /// </summary>
    private void RebuildClusterLastVersions()
    {
        lock (_versionMapLock)
        {
            foreach (var pool in _pools)
            foreach (var (collectionName, version) in pool.LastKnownVersions)
                AdvanceClusterLastVersionLocked(collectionName, version);
        }
    }

    /// <summary>
    /// Updates the tracked cluster-wide last version for a collection if <paramref name="version"/> is newer
    /// than what's currently tracked - never backward, so a slow or stale replica reporting an older version
    /// can't regress what's considered current. Must be called while holding <see cref="_versionMapLock"/>.
    /// </summary>
    private void AdvanceClusterLastVersionLocked(string collectionName, string version)
    {
        if (!_clusterLastVersion.TryGetValue(collectionName, out var currentMax) ||
            string.Compare(version, currentMax, StringComparison.InvariantCultureIgnoreCase) > 0)
            _clusterLastVersion[collectionName] = version;
    }

    /// <summary>
    /// The highest version known across all replicas for a collection, or null if none is known yet (e.g. the
    /// collection doesn't exist, or hasn't been fed, or no replica has reported it yet).
    /// </summary>
    public string? GetLastVersion(string collectionName)
    {
        lock (_versionMapLock)
        {
            return _clusterLastVersion.GetValueOrDefault(collectionName);
        }
    }

    /// <summary>
    /// The pools whose own last known version for a collection matches the cluster-wide last version. A pool
    /// that hasn't reported in yet, or is behind, is not included. Empty if no replica is known to have a
    /// version of this collection at all.
    /// </summary>
    public IReadOnlyList<ConnectorPool> GetPoolsWithLastVersion(string collectionName)
    {
        string? lastVersion;
        lock (_versionMapLock)
        {
            if (!_clusterLastVersion.TryGetValue(collectionName, out lastVersion))
                return [];
        }

        return _pools
            .Where(p => p.LastKnownVersions.TryGetValue(collectionName, out var version) && version == lastVersion)
            .ToList();
    }

    public void ConfigureLocalCache(string collectionName, int capacity)
    {
        _localCaches[collectionName] = new LruLocalCache(key =>
        {
            // if the item is not in the local cache, we try to get it from the servers
            var item = InternalQueryRawDataByPrimaryKey(collectionName, key).GetAwaiter().GetResult();
            if (item.Count == 0)
                return null; // not found on the server either

            return new CachedItem
            {
                PrimaryKey = key,
                Data = item[0]
            };
        }, evictionLimit: capacity, evictionCount: 1000);
    }

    /// <summary>
    /// Creates an aggregator for multiple cache server replicas.
    /// </summary>
    /// <param name="capacity">Maximum capacity of each pool.</param>
    /// <param name="servers">List of server addresses and ports.</param>
    public Aggregator(int capacity, params (string server, int port)[] servers)
        : this(capacity, false, true, 10_000, servers)
    {
    }

    /// <summary>
    /// Creates an aggregator for multiple cache server replicas, connecting over TLS.
    /// </summary>
    /// <param name="capacity">Maximum capacity of each pool.</param>
    /// <param name="useSsl">Wrap every connection in TLS. Every replica's server must have SSL enabled too.</param>
    /// <param name="validateServerCertificate">
    /// When true (the default), each server certificate must be trusted and match its host. Set to false only
    /// for testing against a self-signed/untrusted certificate.
    /// </param>
    /// <param name="watchDogFrequencyInMilliseconds">
    /// How often each replica's watchdog checks connectivity and collection versions.
    /// </param>
    /// <param name="servers">List of server addresses and ports.</param>
    public Aggregator(int capacity, bool useSsl, bool validateServerCertificate,
        int watchDogFrequencyInMilliseconds = 10_000, params (string server, int port)[] servers)
    {
        if (servers == null || servers.Length == 0) throw new ArgumentNullException(nameof(servers), "At least one server must be specified");

        foreach (var (server, port) in servers)
        {
            var pool = new ConnectorPool(capacity, server, port, watchDogFrequencyInMilliseconds,
                useSsl, validateServerCertificate);
            pool.NewVersion += OnPoolNewVersion;
            _pools.Add(pool);
        }

        // each pool has already established its own version baseline by the time its constructor returns
        // (see ConnectorPool.InternalConnect), so this reflects the true state at connection time
        RebuildClusterLastVersions();
    }

    /// <summary>
    /// Last server index used for round-robin selection.
    /// </summary>
    private int _lastServer;

    /// <param name="collectionName">
    /// When given, prefer replicas known to have this collection's last version. If none are known to have
    /// it (e.g. right after a feed, before every replica's watchdog has caught up, or no replica has reported
    /// this collection yet), falls back to any connected replica rather than fail the call - deliberately, so
    /// this can never throw for a reason <see cref="InternalQueryRawDataByPrimaryKey"/>'s unconditional retry
    /// loop can't recover from (it retries on any exception with no backoff).
    /// </param>
    private async Task<Connector> Get(string? collectionName = null)
    {
        var connected = _pools.Where(x => x.IsConnected).ToArray();

        if (connected.Length == 0)
        {
            throw new InvalidOperationException("No connected pools available. Please check the connection status of the servers.");
        }

        var candidates = connected;

        if (collectionName != null)
        {
            var upToDate = GetPoolsWithLastVersion(collectionName).Where(p => p.IsConnected).ToArray();

            if (upToDate.Length > 0)
                candidates = upToDate;
            else
                Debug.Print(
                    $"Get : no replica confirmed at the last version for '{collectionName}' yet, falling back to any connected replica");
        }

        // Round-robin selection of the next available pool
        var pool = candidates[_lastServer % candidates.Length];

        _lastServer = (_lastServer + 1) % candidates.Length; // Round-robin selection

        if (pool.IsConnected)
            return await pool.Get();

        // If the selected pool is not connected, try to get from another pool
        return await Get(collectionName);
    }

    private void Return(Connector connector)
    {
        // Return to the appropriate pool based on address
        var serverAddress = connector.Address;
        var pool = _pools.FirstOrDefault(p => p.Address == serverAddress);
        if(pool != null)
        {
            pool.Return(connector);
        }
    }

    /// <summary>
    /// Declares a new collection and its keys. If the collection does not exist on the server it is created
    /// </summary>
    public async Task DeclareCollection(string collectionName, string primaryKey, params string[] otherIndexes)
    {
        
        await Parallel.ForEachAsync(_pools.Where(x=>x.IsConnected),  async (p, _) =>
        {
            var connector = await p.Get();
            try
            {
                await connector.CreateCollection(collectionName, primaryKey, otherIndexes);
            }
            catch (SocketException )
            {
                p.MarkAsNotConnected();
                throw;
            }
        });
    }

    /// <summary>
    /// Remove all versions of data and the metadata. All servers need to be up
    /// </summary>
    /// <param name="collectionName"></param>
    /// <returns></returns>
    public async Task DropCollection(string collectionName)
    {
        if (_pools.Any(x => !x.IsConnected))
            throw new CacheException("Can not drop a collection if all the servers are not available");

        await Parallel.ForEachAsync(_pools.Where(x => x.IsConnected), async (p, _) =>
        {
            var connector = await p.Get();
            try
            {
                await connector.DropCollection(collectionName);
            }
            catch (SocketException)
            {

                p.MarkAsNotConnected();
                throw;
            }
        });

    }

    public async Task<CollectionsDescription?[]> GetCollectionsDescription()
    {
        var result = new CollectionsDescription?[_pools.Count];

        await Parallel.ForAsync(0, _pools.Count, async(i, _) =>
        {
            var pool = _pools[i];

            if (!pool.IsConnected)
            {
                result[i] = null;
                return;
            }
            var connector = await pool.Get();
            try
            {
                result[i] = await connector.GetCollectionsDescription();
                Return(connector);
            }
            catch (Exception)
            {

                pool.MarkAsNotConnected();
                result[i] = null;
            }
        });

        return result;
    }

    public async Task<List<byte[]>> QueryRawDataByPrimaryKey(string collection, params long[] keys)
    {
        List<byte[]> result = new List<byte[]>(keys.Length);

        if (_localCaches.TryGetValue(collection, out var cache))
        {
            foreach (var key in keys)
            {
                var item = cache.TryGet(key);
                if (item != null)
                {
                    result.Add(item.Data);
                }
            }

            return result;
        }

        return await InternalQueryRawDataByPrimaryKey(collection, keys);
    }

    private async Task<List<byte[]>> InternalQueryRawDataByPrimaryKey(string collection, params long[] keys)
    {
        
        Connector? connector = null;
        try
        {
            connector = await Get(collection);
            return await connector.QueryByPrimaryKey(collection, keys);
        }
        catch (Exception)
        {
            if (connector != null)
            {
                var serverAddress = connector.Address;
                var pool = _pools.FirstOrDefault(p => p.Address == serverAddress);
                pool?.MarkAsNotConnected();
            }
            
            // try another server
            return await InternalQueryRawDataByPrimaryKey(collection, keys);

        }
        finally
        {
            if (connector != null)
                Return(connector);
        }
    }

    public async Task<List<T>> QueryByPrimaryKey<T>(string collection, params long[] keys)
    {
        
        var rawData =  await QueryRawDataByPrimaryKey(collection, keys);

        List<T> result = new List<T>(keys.Length);

        if (!_deserializers.TryGetValue(typeof(T), out var deserializer))
        {
            throw new CacheException($"No collection was registered for type {typeof(T)}");
        }

        foreach (var data in rawData)
        {
            result.Add((T)deserializer(data));
        }

        return result;
    }


    /// <summary>
    /// Registers a typed collection with the cache server. A typed collection needs to specify a serialization/deserialization method and at least one key generator.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="collectionName"></param>
    /// <param name="serializer"></param>
    /// <param name="deserializer"></param>
    /// <param name="keyGenerators"></param>
    /// <exception cref="CacheException"></exception>
    public void RegisterTypedCollection<T>(string collectionName, Func<T, byte[]> serializer, Func<byte[], T> deserializer, params Func<T, long>[] keyGenerators)
    {
        if (serializer == null) throw new ArgumentNullException(nameof(serializer));
        if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

        if (!_pools.Any(x => x.IsConnected))
            throw new CacheException("No server is available");

        if (keyGenerators.Length == 0)
            throw new CacheException("At least one key generator must be specified");
        
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(collectionName));

        // function that transforms a typed item into an Item object 
        Item PackObject(object typedItem)
        {
            if (typedItem is not T item)
                throw new CacheException($"Item is not of type {typeof(T).Name}");
            var keys = keyGenerators.Select(k => k(item)).ToArray();
            return new Item(serializer(item), keys);
        }

        
        _packers[typeof(T)] = PackObject;

        // function that deserializes a byte array into an object of type T
        object DeserializeObject(byte[] data)
        {
            return deserializer(data) ?? throw new CacheException($"Can not deserialize data as {typeof(T).Name}");
        }

        _deserializers[typeof(T)] = DeserializeObject;


    }

    private IEnumerable<Item> PackTypedItems<T>(IEnumerable<T> items)
    {
        if (!_packers.TryGetValue(typeof(T), out var packer))
        {
            throw new CacheException($"No collection was registered for type {typeof(T)}");
        }

        foreach (var item in items)
        {
            yield return packer(item!);
        }
    }


    /// <summary>
    /// Feed a collection with a sequence of typed objects. A replica will be fed to all the servers
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="collectionName"></param>
    /// <param name="items"></param>
    /// <returns></returns>
    /// <exception cref="CacheException"></exception>
    public async Task FeedCollection<T>(string collectionName, IEnumerable<T> items)
    {
        
        string newVersion = DateTime.UtcNow.ToString("yyyyMMdd_hhmmss");

        // generate once , feed all in parallel

        var connected = _pools.Where(x => x.IsConnected).ToArray();

        if (connected.Length == 0)
            throw new CacheException("No servers available. Please check the connection status of the servers.");

        // Create a channel for each connected pool
        var channels = new Channel<Item>[connected.Length];
        for (int i = 0; i < connected.Length; i++)
        {
            channels[i] = Channel.CreateBounded<Item>(10_000);
        }

        // Get a connector for each connected pool
        var connectors = new Connector[connected.Length];
        for (int i = 0; i < connected.Length; i++)
        {
            connectors[i] = await connected[i].Get();
        }

        // Create tuples of channels and connectors for parallel processing
        (Channel<Item>, Connector)[] chCxTuples = channels.Zip(connectors).ToArray();

        try
        {

            // Feed the items to each connector from a consumer task that reads from the corresponding channel
            var feederTask = Task.Run(async () =>
            {
                await Parallel.ForEachAsync(chCxTuples, async (chcx, _) =>
                {

                    var connector = chcx.Item2;
                    var reader = chcx.Item1.Reader;
                    var packedSequence =  reader.ReadAllAsync(CancellationToken.None).ToBlockingEnumerable(CancellationToken.None);


                    await connector.FeedCollection(collectionName, newVersion, packedSequence);
                
                });
            });

            // Pack the items once and write them to the channels

            foreach (var item in PackTypedItems(items))
            {
                foreach (var chanel in channels)
                {
                    await chanel.Writer.WriteAsync(item);
                }
            }

            // Complete the writers to signal that no more items will be written
            foreach (var chanel in channels)
            {
                chanel.Writer.Complete();
            }

            // Wait for all feeders to complete
            await feederTask;

        }
        finally
        {
            // Return all connectors to their respective pools
            foreach (var connector in connectors)
            {
                Return(connector);
            }
        }

        
        

    }

    private readonly Dictionary<Type, Func<object, Item>> _packers = new();
    
    private readonly Dictionary<Type, Func<byte[], object>> _deserializers = new();


    public Dictionary<string, CacheStatistics> GetStatistics()
    {
        var result = new Dictionary<string, CacheStatistics>();
        foreach (var lruLocalCache in _localCaches)
        {
            result[lruLocalCache.Key] = lruLocalCache.Value.GetStatistics();
        }


        return result;
    }
}