using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using CacheClient.LocalCache;
using Messages;


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
            {
                foreach (var (collectionName, version) in pool.LastKnownVersions)
                {
                    AdvanceClusterLastVersionLocked(collectionName, version);
                }
            }
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
        ArgumentNullException.ThrowIfNull(servers);
        if (servers.Length == 0) throw new ArgumentException("At least one server must be specified", nameof(servers));

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
            await WithConnector(p, connector => connector.CreateCollection(collectionName, primaryKey, otherIndexes));
        });
    }

    /// <summary>
    /// Runs a request on a connector of <paramref name="pool"/>, which is then returned to it - or, if the
    /// connection failed, closed and the pool marked as disconnected (its watchdog reconnects it).
    /// </summary>
    private static async Task WithConnector(ConnectorPool pool, Func<Connector, Task> request)
    {
        var connector = await pool.Get();
        try
        {
            await request(connector);
        }
        catch (Exception e) when (IsConnectionFailure(e))
        {
            connector.Dispose();
            pool.MarkAsNotConnected();
            throw;
        }

        pool.Return(connector);
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
            await WithConnector(p, connector => connector.DropCollection(collectionName));
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
    /// All the documents whose value for the index <paramref name="indexName"/> equals <paramref name="keyValue"/>,
    /// deserialized as <typeparamref name="T"/> (see <see cref="RegisterTypedCollection{T}"/>). The whole result is
    /// held in memory; use <see cref="StreamBySecondaryKey{T}"/> for large ones. If a replica fails part way
    /// through, the query is restarted on another one.
    /// </summary>
    /// <param name="collection">Collection name.</param>
    /// <param name="indexName">Name of an index declared by <see cref="DeclareCollection"/>.</param>
    /// <param name="keyValue">Key value, converted exactly like the key generator registered for this index.</param>
    public async Task<List<T>> QueryBySecondaryKey<T>(string collection, string indexName, long keyValue)
    {
        var deserializer = GetDeserializer<T>();

        while (true)
        {
            var result = new List<T>();
            try
            {
                await foreach (var (_, data) in StreamRawDataBySecondaryKey(collection, indexName, keyValue))
                    result.Add((T)deserializer(data));

                return result;
            }
            catch (Exception e) when (IsConnectionFailure(e))
            {
                // the replica is now marked as disconnected, so the next attempt goes to another one
                Debug.Print($"QueryBySecondaryKey : replica failed during the query, retrying: {e.Message}");
            }
        }
    }

    /// <summary>
    /// Streams the documents whose value for the index <paramref name="indexName"/> equals
    /// <paramref name="keyValue"/>, deserialized as <typeparamref name="T"/> (see
    /// <see cref="RegisterTypedCollection{T}"/>). Same behavior as <see cref="StreamRawDataBySecondaryKey"/>.
    /// </summary>
    public IAsyncEnumerable<T> StreamBySecondaryKey<T>(string collection, string indexName, long keyValue,
        CancellationToken cancellationToken = default)
    {
        // resolved eagerly: in an iterator method the throw would only surface once enumeration starts
        var deserializer = GetDeserializer<T>();

        return StreamBySecondaryKeyCore<T>(collection, indexName, keyValue, deserializer, cancellationToken);
    }

    private async IAsyncEnumerable<T> StreamBySecondaryKeyCore<T>(string collection, string indexName, long keyValue,
        Func<byte[], object> deserializer, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var (_, data) in StreamRawDataBySecondaryKey(collection, indexName, keyValue, cancellationToken))
            yield return (T)deserializer(data);
    }

    /// <summary>
    /// Streams the documents whose value for the index <paramref name="indexName"/> equals
    /// <paramref name="keyValue"/>, as raw data paired with their primary key.
    /// <para>
    /// Results come from a single replica, preferring those on the latest version of the collection, and are
    /// streamed in batches, so a large result never needs to fit in memory. If the replica fails before the first
    /// result, the query is retried on another one; after that, the failure is propagated, since a retry would
    /// return the same documents twice. Server-side errors (unknown collection or index) are never retried.
    /// </para>
    /// <para>
    /// The enumeration can be stopped early: the connection, which still holds unread results, is then replaced
    /// rather than returned to the pool.
    /// </para>
    /// </summary>
    /// <param name="collection">Collection name.</param>
    /// <param name="indexName">Name of an index declared by <see cref="DeclareCollection"/>.</param>
    /// <param name="keyValue">Key value, converted exactly like the key generator registered for this index.</param>
    /// <param name="cancellationToken">Checked between results.</param>
    public IAsyncEnumerable<(long PrimaryKey, byte[] Data)> StreamRawDataBySecondaryKey(string collection,
        string indexName, long keyValue, CancellationToken cancellationToken = default) =>
        StreamFromOneReplica(collection,
            connector => connector.StreamBySecondaryIndex(collection, indexName, keyValue), cancellationToken);

    /// <summary>
    /// Streams every document of the collection's active version, as raw data paired with its primary key. Same
    /// behavior as <see cref="StreamRawDataBySecondaryKey"/>: the documents come from a single replica, preferring
    /// those on the latest version, with failover before the first document.
    /// </summary>
    /// <param name="collection">Collection name.</param>
    /// <param name="cancellationToken">Checked between documents.</param>
    public IAsyncEnumerable<(long PrimaryKey, byte[] Data)> StreamAllData(string collection,
        CancellationToken cancellationToken = default) =>
        StreamFromOneReplica(collection, connector => connector.StreamAllData(collection), cancellationToken);

    /// <summary>
    /// Runs a stream opened by <paramref name="open"/> on one replica: see <see cref="StreamRawDataBySecondaryKey"/>
    /// for the failover and early-stop rules.
    /// </summary>
    private async IAsyncEnumerable<(long PrimaryKey, byte[] Data)> StreamFromOneReplica(string collection,
        Func<Connector, IAsyncEnumerable<(long PrimaryKey, byte[] Data)>> open,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            var connector = await Get(collection);
            var pool = _pools.FirstOrDefault(p => p.Address == connector.Address);

            var results = open(connector).GetAsyncEnumerator(CancellationToken.None);

            var completed = false;
            var connectionFailed = false;
            var anyResult = false;

            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    bool hasResult;
                    try
                    {
                        hasResult = await results.MoveNextAsync();
                    }
                    catch (Exception e) when (IsConnectionFailure(e))
                    {
                        connectionFailed = true;
                        pool?.MarkAsNotConnected();

                        if (anyResult)
                            throw;

                        break; // nothing returned yet: retry on another replica
                    }

                    if (!hasResult)
                    {
                        completed = true;
                        yield break;
                    }

                    anyResult = true;
                    yield return results.Current;
                }
            }
            finally
            {
                await results.DisposeAsync();

                if (completed)
                    Return(connector);
                else if (connectionFailed || pool == null)
                    connector.Dispose(); // the pool was already cleared
                else
                    pool.Discard(connector); // abandoned, cancelled or server-side error: unread data may remain
            }
        }
    }

    /// <summary>
    /// True for failures of the connection itself, as opposed to errors reported by the server (other
    /// <see cref="CacheException"/>s) that would occur on any replica.
    /// </summary>
    private static bool IsConnectionFailure(Exception e) =>
        e is ConnectionClosedException or IOException or SocketException or ObjectDisposedException;

    private Func<byte[], object> GetDeserializer<T>() =>
        _deserializers.TryGetValue(typeof(T), out var deserializer)
            ? deserializer
            : throw new CacheException($"No collection was registered for type {typeof(T)}");


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
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(deserializer);

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
        // Resolved eagerly rather than inside the iterator below: a method containing "yield return" is
        // entirely deferred, so this throw would otherwise only surface once the caller starts enumerating.
        if (!_packers.TryGetValue(typeof(T), out var packer))
        {
            throw new CacheException($"No collection was registered for type {typeof(T)}");
        }

        return PackTypedItemsCore(items, packer);
    }

    private static IEnumerable<Item> PackTypedItemsCore<T>(IEnumerable<T> items, Func<object, Item> packer)
    {
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
        
        // 24-hour clock: the server orders versions by name, and with "hh" a feed at 13:00 ("_01...") would sort
        // before one from the same morning and be rejected as older
        string newVersion = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");

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

        // Cancelled by the first replica whose feed fails. The producer below hands every item to every replica's
        // bounded channel: without this, once the failed replica's channel is full, it would wait forever - and
        // so would every other replica.
        using var feedFailed = new CancellationTokenSource();
        Exception? firstFailure = null;

        // One feeder per replica, uploading the items of its channel
        var feeders = new Task[connected.Length];
        for (int i = 0; i < connected.Length; i++)
        {
            var connector = connectors[i];
            var reader = channels[i].Reader;
            feeders[i] = Task.Run(async () =>
            {
                try
                {
                    var packedSequence = reader.ReadAllAsync(CancellationToken.None)
                        .ToBlockingEnumerable(CancellationToken.None);

                    await connector.FeedCollection(collectionName, newVersion, packedSequence);
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref firstFailure,
                        new CacheException($"Feeding {collectionName} failed on {connector.Address}: {e.Message}"),
                        null);
                    await feedFailed.CancelAsync();
                    throw;
                }
            });
        }

        var succeeded = false;
        try
        {
            try
            {
                // Pack the items once and write them to the channels
                foreach (var item in PackTypedItems(items))
                {
                    foreach (var channel in channels)
                    {
                        await channel.Writer.WriteAsync(item, feedFailed.Token);
                    }
                }
            }
            catch (Exception e)
            {
                // A replica failed, or the items couldn't be produced: abort the feed on every replica. Completing
                // the channels with an error (not normally) stops each upload without its end marker, so that no
                // server commits a partial version.
                foreach (var channel in channels)
                    channel.Writer.TryComplete(e);

                await WhenAllIgnoringErrors(feeders);

                throw e is OperationCanceledException && firstFailure != null ? firstFailure : e;
            }

            // Complete the writers to signal that no more items will be written
            foreach (var channel in channels)
                channel.Writer.Complete();

            // Wait for all feeders to complete
            await WhenAllIgnoringErrors(feeders);
            if (firstFailure != null)
                throw firstFailure;

            succeeded = true;
        }
        finally
        {
            for (int i = 0; i < connected.Length; i++)
            {
                if (succeeded)
                    connected[i].Return(connectors[i]);
                else
                    // possibly in the middle of a feed session: closed (the server then discards the partial
                    // version) and replaced, never reused
                    connected[i].Discard(connectors[i]);
            }
        }

        
        

    }

    private static async Task WhenAllIgnoringErrors(Task[] tasks)
    {
        try
        {
            await Task.WhenAll(tasks);
        }
        catch
        {
            // each failure is recorded by its task; the caller reports the first one
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