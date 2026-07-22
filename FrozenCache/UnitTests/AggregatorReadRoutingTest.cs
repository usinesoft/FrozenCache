using System.Text;
using CacheClient;
using FrozenCache;
using Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

/// <summary>
/// Verifies that reads are routed only to replicas known to have a collection's latest version, so a
/// lagging replica never answers a query with stale data.
/// </summary>
public class AggregatorReadRoutingTest
{
    private const string StoreNamePrefix = "teststore_readrouting";
    private const int ServerCount = 2;

    private readonly List<DataStore> _stores = [];
    private readonly List<HostedTcpServer> _servers = [];

    [TearDown]
    public void Clean()
    {
        foreach (var server in _servers)
            server.StopAsync(CancellationToken.None).GetAwaiter().GetResult();

        foreach (var store in _stores)
            store.Dispose();

        for (var i = 0; i < ServerCount; i++)
            DataStore.Drop($"{StoreNamePrefix}{i}");

        _stores.Clear();
        _servers.Clear();
    }

    [SetUp]
    public void Setup()
    {
        for (var i = 0; i < ServerCount; i++)
            DataStore.Drop($"{StoreNamePrefix}{i}");

        var logger = new Mock<ILogger<HostedTcpServer>>();

        for (var i = 0; i < ServerCount; i++)
        {
            var store = new DataStore($"{StoreNamePrefix}{i}", IndexType.Dictionary);
            store.Open();
            _stores.Add(store);

            var configuration = new Mock<IOptions<ServerSettings>>();
            configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });

            var server = new HostedTcpServer(store, logger.Object, configuration.Object);
            server.StartAsync(CancellationToken.None);
            _servers.Add(server);
        }
    }

    [Test]
    public async Task ReadsAreRoutedOnlyToReplicasWithTheLatestVersionEvenWhenOneLagsBehind()
    {
        // both replicas start in sync at v001
        foreach (var store in _stores)
        {
            store.CreateCollection(new CollectionMetadata("persons", "id"));
            store.FeedCollection("persons", "v001", [new Item(Encoding.UTF8.GetBytes("v001-data"), 1)]);
        }

        var servers = _servers.Select(s => ("localhost", s.Port)).ToArray();
        var aggregator = new Aggregator(2, false, true, 200, servers);

        // both replicas should be confirmed at the baseline version once connected
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && aggregator.GetPoolsWithLastVersion("persons").Count < ServerCount)
            await Task.Delay(100);

        Assert.That(aggregator.GetPoolsWithLastVersion("persons").Count, Is.EqualTo(ServerCount));

        // only replica 0 gets fed the new version; replica 1 is deliberately left behind, simulating
        // replication lag or a partially-failed cluster-wide feed
        _stores[0].FeedCollection("persons", "v002", new[] { new Item(Encoding.UTF8.GetBytes("v002-data"), 1) });

        deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && aggregator.GetLastVersion("persons") != "v002")
            await Task.Delay(100);

        Assert.That(aggregator.GetLastVersion("persons"), Is.EqualTo("v002"));
        Assert.That(aggregator.GetPoolsWithLastVersion("persons").Count, Is.EqualTo(1),
            "Only the replica that was actually fed the new version should be considered up to date");

        // every read must now come from the up-to-date replica; round-robin alone (the old behavior) would
        // have bounced between both and occasionally returned the stale value
        for (var i = 0; i < 20; i++)
        {
            var result = await aggregator.QueryRawDataByPrimaryKey("persons", 1);
            Assert.That(result.Count, Is.EqualTo(1));
            Assert.That(Encoding.UTF8.GetString(result[0]), Is.EqualTo("v002-data"),
                "Every read must be served by the replica with the latest version, never the lagging one");
        }
    }

    [Test]
    public async Task ReadsFallBackWhenNoReplicaIsConfirmedAtTheLatestVersionYet()
    {
        // a long watchdog interval and a single replica in this aggregator: the collection is created only
        // after the aggregator has already connected, so the map genuinely has no confirmed replica for it
        // yet - the eager per-pool bootstrap that runs at connection time can't have seen it, and nothing
        // will make the watchdog re-check for a long time. With a single replica there's nowhere else the
        // fallback could route to but the right one anyway, so this safely proves the fallback path lets the
        // read through instead of failing it, without also depending on the pre-existing (unrelated) retry
        // behavior in InternalQueryRawDataByPrimaryKey for a collection a replica has genuinely never heard of.
        var aggregator = new Aggregator(2, false, true, 60_000, ("localhost", _servers[0].Port));

        _stores[0].CreateCollection(new CollectionMetadata("persons", "id"));
        _stores[0].FeedCollection("persons", "v001", new[] { new Item(Encoding.UTF8.GetBytes("server-0-data"), 1) });

        Assert.That(aggregator.GetPoolsWithLastVersion("persons"), Is.Empty,
            "The map should not yet know about a collection created after the aggregator connected");

        var result = await aggregator.QueryRawDataByPrimaryKey("persons", 1);
        Assert.That(result.Count, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(result[0]), Is.EqualTo("server-0-data"));
    }
}
