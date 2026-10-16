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
/// A feed that fails on the server's side, after the client started streaming its batches, must fail promptly - for
/// a single connector and for a cluster fed through the Aggregator - instead of freezing client and servers. The
/// failure is caused by items carrying more keys than the collection declares, which the server can't store.
/// </summary>
public class FeedFailureTest
{
    private const string StoreNamePrefix = "teststore_feedfailure";

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

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

    private static async Task<T> WithinTimeout<T>(Task<T> task)
    {
        Assert.That(await Task.WhenAny(task, Task.Delay(Timeout)), Is.SameAs(task), "the operation froze");
        return await task;
    }

    [Test]
    public async Task ServerReportsAFeedFailingOnItsSideInsteadOfFreezing()
    {
        // one key declared, two sent: the server fails to store the first item
        _stores[0].CreateCollection(new CollectionMetadata("products", "id"));

        using var connector = new Connector("localhost", _servers[0].Port);
        Assert.That(connector.Connect(), Is.True);

        // more items than the server's internal buffer (1,000,000): before the fix, it stopped reading once full
        var items = Enumerable.Range(0, 1_500_000).Select(i => new Item([1], i, i % 10));

        var feed = Task.Run(async () =>
        {
            try
            {
                await connector.FeedCollection("products", "v1", items);
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        });

        var error = await WithinTimeout(feed);
        Assert.That(error, Is.InstanceOf<CacheException>());
        Assert.That(error!.Message, Does.StartWith("Failed to feed collection:"),
            "the server's reason, not just a broken connection");

        // nothing was committed, and the server still serves other connections
        using var other = new Connector("localhost", _servers[0].Port);
        Assert.That(other.Connect(), Is.True);
        Assert.That(await other.Ping(), Is.True);
        Assert.That(_stores[0].GetCollectionInformation().Collections.Any(c => c.Name == "products"), Is.False);
    }

    [Test]
    public async Task ClusterFeedFailsInsteadOfFreezingWhenOneReplicaFails()
    {
        // replica 0 can store the items, replica 1 can't
        _stores[0].CreateCollection(new CollectionMetadata("products", "id", "category"));
        _stores[1].CreateCollection(new CollectionMetadata("products", "id"));

        var aggregator = new Aggregator(2, false, true, 600_000,
            ("localhost", _servers[0].Port), ("localhost", _servers[1].Port));
        aggregator.RegisterTypedCollection<Product>("products",
            p => MessagePackSerializer.Serialize(p),
            b => MessagePackSerializer.Deserialize<Product>(b),
            p => p.Id,
            p => p.Category);

        var products = Enumerable.Range(0, 500_000).Select(i => new Product { Id = i, Category = i % 10 });

        var feed = Task.Run(async () =>
        {
            try
            {
                await aggregator.FeedCollection("products", products);
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        });

        var error = await WithinTimeout(feed);
        Assert.That(error, Is.InstanceOf<CacheException>());
        Assert.That(error!.Message, Does.Contain($"localhost:{_servers[1].Port}"), "names the failing replica");

        // The healthy replica didn't commit a partial version: its upload was aborted without the end marker. The
        // server notices the closed connection asynchronously, so give it a moment.
        var started = DateTime.UtcNow;
        while (_stores[0].GetCollectionInformation().Collections.Any(c => c.Name == "products") &&
               DateTime.UtcNow - started < TimeSpan.FromSeconds(5))
            await Task.Delay(50);
        Assert.That(_stores[0].GetCollectionInformation().Collections.Any(c => c.Name == "products"), Is.False,
            "the healthy replica committed a partial version");

        // the connections used by the feed were replaced: the aggregator still works on both replicas
        var descriptions = await WithinTimeout(aggregator.GetCollectionsDescription());
        Assert.That(descriptions, Has.All.Not.Null);
    }

    [Test]
    public async Task DeclareAndDropReturnTheirConnectionsToThePool()
    {
        // two connections per server: before the fix, the third declare waited forever for a connection
        var aggregator = new Aggregator(2, false, true, 600_000, ("localhost", _servers[0].Port));

        var calls = Task.Run(async () =>
        {
            for (var i = 0; i < 5; i++)
            {
                await aggregator.DeclareCollection($"collection{i}", "id");
                await aggregator.DropCollection($"collection{i}");
            }

            return true;
        });

        Assert.That(await WithinTimeout(calls), Is.True);
    }
}
