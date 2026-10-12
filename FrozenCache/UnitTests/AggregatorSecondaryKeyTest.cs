using CacheClient;
using FrozenCache.Server;
using MessagePack;
using Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

/// <summary>
/// Verifies the Aggregator's queries by secondary key end to end, against two real servers: typed and raw
/// results, streams abandoned or cancelled part way (whose connection still holds unread batches), server-side
/// errors, and failover to another replica.
/// </summary>
public class AggregatorSecondaryKeyTest
{
    private const string StoreNamePrefix = "teststore_aggregatorsecondarykey";

    /// <summary>
    /// More than two wire batches (5,000 items each), so that a stream stopped after its first item leaves
    /// unread data on the connection.
    /// </summary>
    private const int MatchingCount = 12_000;

    private const long Category = 42;

    private readonly List<HostedTcpServer> _servers = [];
    private readonly List<DataStore> _stores = [];

    [MessagePackObject]
    public class Product
    {
        [Key(0)] public long Id { get; set; }
        [Key(1)] public long Category { get; set; }
    }

    [SetUp]
    public void Setup()
    {
        for (var i = 0; i < 2; i++)
        {
            DataStore.Drop($"{StoreNamePrefix}{i}");

            var store = new DataStore($"{StoreNamePrefix}{i}", IndexType.Dictionary);
            store.Open();
            _stores.Add(store);

            var configuration = new Mock<IOptions<ServerSettings>>();
            configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });

            var server = new HostedTcpServer(store, new Mock<ILogger<HostedTcpServer>>().Object, configuration.Object);
            server.StartAsync(CancellationToken.None);
            _servers.Add(server);
        }
    }

    [TearDown]
    public void Clean()
    {
        foreach (var server in _servers)
            server.StopAsync(CancellationToken.None).GetAwaiter().GetResult();

        for (var i = 0; i < _stores.Count; i++)
        {
            _stores[i].Dispose();
            DataStore.Drop($"{StoreNamePrefix}{i}");
        }

        _servers.Clear();
        _stores.Clear();
    }

    /// <summary>
    /// Feeds the first <paramref name="serverCount"/> servers with <see cref="MatchingCount"/> products in
    /// <see cref="Category"/> plus a few in other categories, then returns a new aggregator over them, as a reader
    /// process would have: it learns the collection versions when connecting, so the replicas it routes to are
    /// known. The watchdog is effectively disabled so that only the code under test changes the connection state.
    /// </summary>
    private async Task<Aggregator> CreateFedAggregator(int serverCount, int poolCapacity = 2)
    {
        var servers = _servers.Take(serverCount).Select(s => ("localhost", s.Port)).ToArray();

        var feeder = CreateTypedAggregator(2, servers);

        await feeder.DeclareCollection("products", "id", "category");

        var products = Enumerable.Range(0, MatchingCount).Select(i => new Product { Id = i, Category = Category })
            .Concat(Enumerable.Range(MatchingCount, 10).Select(i => new Product { Id = i, Category = 7 }));

        await feeder.FeedCollection("products", products);

        return CreateTypedAggregator(poolCapacity, servers);
    }

    private static Aggregator CreateTypedAggregator(int poolCapacity, (string, int)[] servers)
    {
        var aggregator = new Aggregator(poolCapacity, false, true, 600_000, servers);

        aggregator.RegisterTypedCollection<Product>("products",
            p => MessagePackSerializer.Serialize(p),
            b => MessagePackSerializer.Deserialize<Product>(b),
            p => p.Id,
            p => p.Category);

        return aggregator;
    }

    private static void AssertAllMatching(IReadOnlyCollection<Product> products)
    {
        Assert.That(products, Has.Count.EqualTo(MatchingCount));
        Assert.That(products.All(p => p.Category == Category));
        Assert.That(products.Select(p => p.Id).Distinct().Count(), Is.EqualTo(MatchingCount));
    }

    [Test]
    public async Task QueryBySecondaryKeyReturnsEveryMatchingDocumentTyped()
    {
        var aggregator = await CreateFedAggregator(1);

        AssertAllMatching(await aggregator.QueryBySecondaryKey<Product>("products", "category", Category));

        var others = await aggregator.QueryBySecondaryKey<Product>("products", "category", 7);
        Assert.That(others.Select(p => p.Id), Is.EquivalentTo(Enumerable.Range(MatchingCount, 10)));
    }

    [Test]
    public async Task StreamBySecondaryKeyReturnsEveryMatchingDocumentTyped()
    {
        var aggregator = await CreateFedAggregator(1);

        var products = new List<Product>();
        await foreach (var product in aggregator.StreamBySecondaryKey<Product>("products", "category", Category))
            products.Add(product);

        AssertAllMatching(products);
    }

    [Test]
    public async Task RawStreamPairsEachDocumentWithItsPrimaryKey()
    {
        var aggregator = await CreateFedAggregator(1);

        await foreach (var (primaryKey, data) in aggregator.StreamRawDataBySecondaryKey("products", "category", 7))
            Assert.That(MessagePackSerializer.Deserialize<Product>(data).Id, Is.EqualTo(primaryKey));
    }

    [Test]
    public async Task UnmatchedValueReturnsNothing()
    {
        var aggregator = await CreateFedAggregator(1);

        Assert.That(await aggregator.QueryBySecondaryKey<Product>("products", "category", 999), Is.Empty);
    }

    [Test]
    public async Task AbandonedAndCancelledStreamsLeaveThePoolUsable()
    {
        // twice the pool capacity: if abandoned connections were neither returned nor replaced, the pool would be
        // exhausted and the queries below would hang; if they were returned, their unread batches would corrupt
        // the next responses
        var aggregator = await CreateFedAggregator(1, poolCapacity: 2);

        for (var i = 0; i < 4; i++)
        {
            await foreach (var _ in aggregator.StreamRawDataBySecondaryKey("products", "category", Category))
                break;
        }

        using (var cancellation = new CancellationTokenSource())
        {
            Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await foreach (var _ in aggregator.StreamBySecondaryKey<Product>("products", "category", Category,
                                   cancellation.Token))
                    await cancellation.CancelAsync();
            });
        }

        var query = aggregator.QueryBySecondaryKey<Product>("products", "category", Category);
        Assert.That(await Task.WhenAny(query, Task.Delay(TimeSpan.FromSeconds(10))), Is.SameAs(query),
            "the pool is exhausted");
        AssertAllMatching(await query);

        Assert.That((await aggregator.QueryByPrimaryKey<Product>("products", 5)).Single().Id, Is.EqualTo(5));
    }

    [Test]
    public async Task ServerErrorsAreReportedWithoutMarkingTheReplicaDown()
    {
        var aggregator = await CreateFedAggregator(1);

        var unknownIndex = Assert.ThrowsAsync<CacheException>(() =>
            aggregator.QueryBySecondaryKey<Product>("products", "nonexistent", Category));
        Assert.That(unknownIndex, Is.Not.InstanceOf<ConnectionClosedException>());

        Assert.ThrowsAsync<CacheException>(() =>
            aggregator.QueryBySecondaryKey<Product>("nonexistent", "category", Category));

        // the replica is still considered up, and still answers correctly
        Assert.That(aggregator.GetPoolsWithLastVersion("products").Single().IsConnected);
        AssertAllMatching(await aggregator.QueryBySecondaryKey<Product>("products", "category", Category));
    }

    [Test]
    public async Task UnregisteredTypeIsRejectedImmediately()
    {
        var aggregator = await CreateFedAggregator(1);

        Assert.ThrowsAsync<CacheException>(() => aggregator.QueryBySecondaryKey<string>("products", "category", 1));

        // thrown by the call itself, not deferred to the enumeration
        Assert.Throws<CacheException>(() => aggregator.StreamBySecondaryKey<string>("products", "category", 1));
    }

    [Test]
    public async Task ConnectionClosedByTheServerIsDistinguishedFromServerErrors()
    {
        // a "server" that reads the request, then closes the connection cleanly instead of answering
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var serverSide = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            await client.GetStream().ReadAsync(new byte[1024]);
            client.Client.Shutdown(System.Net.Sockets.SocketShutdown.Both);
        });

        try
        {
            using var connector = new Connector("localhost", port);
            Assert.That(connector.Connect(), Is.True);

            Assert.ThrowsAsync<ConnectionClosedException>(async () =>
            {
                await foreach (var _ in connector.StreamBySecondaryIndex("products", "category", Category)) { }
            });

            await serverSide;
        }
        finally
        {
            listener.Stop();
        }
    }

    [Test]
    public async Task QueriesFailOverWhenAReplicaStops()
    {
        var aggregator = await CreateFedAggregator(2);

        var stoppedAddress = $"localhost:{_servers[1].Port}";
        await _servers[1].StopAsync(CancellationToken.None);

        // round-robin: with several queries, some are first sent to the stopped replica
        for (var i = 0; i < 4; i++)
        {
            AssertAllMatching(await aggregator.QueryBySecondaryKey<Product>("products", "category", Category));

            var streamed = new List<Product>();
            await foreach (var product in aggregator.StreamBySecondaryKey<Product>("products", "category", Category))
                streamed.Add(product);
            AssertAllMatching(streamed);
        }

        var stoppedPool = aggregator.GetPoolsWithLastVersion("products").Single(p => p.Address == stoppedAddress);
        Assert.That(stoppedPool.IsConnected, Is.False, "the failure of the stopped replica was not detected");
    }
}
