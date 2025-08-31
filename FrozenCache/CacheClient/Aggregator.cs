using System.Collections;
using System.Net.Sockets;
using System.Threading.Channels;
using Messages;
using PersistentStore;

namespace CacheClient;

/// <summary>
/// Aggregates multiple connectors to different replicas of the cache server.
/// </summary>
public class Aggregator
{
    private readonly List<ConnectorPool> _pools = [];

    /// <summary>
    /// Creates an aggregator for multiple cache server replicas.
    /// </summary>
    /// <param name="capacity">Maximum capacity of each pool.</param>
    /// <param name="servers">List of server addresses and ports.</param>
    public Aggregator(int capacity, params (string server, int port)[] servers)
    {
        if (servers == null || servers.Length == 0) throw new ArgumentNullException(nameof(servers), "At least one server must be specified");

        foreach (var (server, port) in servers)
        {
            _pools.Add(new ConnectorPool(capacity, server, port));
        }
    }

    /// <summary>
    /// Last server index used for round-robin selection.
    /// </summary>
    private int _lastServer;

    private async Task<Connector> Get()
    {
        var connected = _pools.Where(x => x.IsConnected).ToArray();

        if (connected.Length == 0)
        {
            throw new InvalidOperationException("No connected pools available. Please check the connection status of the servers.");
        }

        // Round-robin selection of the next available pool
        var pool = connected[_lastServer % connected.Length];
        
        _lastServer = (_lastServer + 1) % connected.Length; // Round-robin selection

        if(pool.IsConnected)
            return await pool.Get();

        // If the selected pool is not connected, try to get from another pool
        return await Get();
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

        await Parallel.ForAsync(0, _pools.Count, async(i, token) =>
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
        
        Connector? connector = null;
        try
        {
            connector = await Get();
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
            return await QueryRawDataByPrimaryKey(collection, keys);

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

        if (_pools.Any(x => !x.IsConnected))
            throw new CacheException("Can not register a typed collection if all the servers are not available");

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



}