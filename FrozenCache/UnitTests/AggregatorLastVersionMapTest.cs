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
/// Verifies the cluster-wide "last version per collection" tracking used to later route reads only to
/// replicas that actually have a collection's latest version.
/// </summary>
public class AggregatorLastVersionMapTest
{
    private const string StoreName = "teststore_lastversionmap";

    private HostedTcpServer? _server;
    private DataStore _store = null!;

    [TearDown]
    public void Clean()
    {
        _server?.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
        _store.Dispose();
        DataStore.Drop(StoreName);
    }

    [SetUp]
    public void Setup()
    {
        DataStore.Drop(StoreName);

        _store = new DataStore(StoreName, IndexType.Dictionary);
        _store.Open();

        var logger = new Mock<ILogger<HostedTcpServer>>();
        var configuration = new Mock<IOptions<ServerSettings>>();
        configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });

        _server = new HostedTcpServer(_store, logger.Object, configuration.Object);
        _server.StartAsync(CancellationToken.None);
    }

    [Test]
    public void TheMapIsPopulatedAtConnectionWithoutWaitingForTheWatchdog()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", [new Item(new byte[10], 1)]);

        // a long watchdog interval: if the map only populated on the first tick, this assertion would fail
        var aggregator = new Aggregator(2, false, true, 60_000, ("localhost", _server!.Port));

        Assert.That(aggregator.GetLastVersion("persons"), Is.EqualTo("v001"),
            "The last version should be known immediately after construction, not after the first watchdog tick");

        var poolsWithLastVersion = aggregator.GetPoolsWithLastVersion("persons");
        Assert.That(poolsWithLastVersion.Count, Is.EqualTo(1));
    }

    [Test]
    public void UnknownCollectionHasNoLastVersionAndNoEligibleServers()
    {
        var aggregator = new Aggregator(2, false, true, 60_000, ("localhost", _server!.Port));

        Assert.That(aggregator.GetLastVersion("nonexistent"), Is.Null);
        Assert.That(aggregator.GetPoolsWithLastVersion("nonexistent"), Is.Empty);
    }

    [Test]
    public async Task TheMapTracksACollectionCreatedAfterTheAggregatorConnected()
    {
        // no collections exist yet when the aggregator connects
        var aggregator = new Aggregator(2, false, true, 200, ("localhost", _server!.Port));

        Assert.That(aggregator.GetLastVersion("persons"), Is.Null);

        // the collection is created and fed only now, after the aggregator is already up and running
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && aggregator.GetLastVersion("persons") == null)
            await Task.Delay(100);

        Assert.That(aggregator.GetLastVersion("persons"), Is.EqualTo("v001"),
            "A collection that didn't exist at connection time should still be picked up once it appears");
        Assert.That(aggregator.GetPoolsWithLastVersion("persons").Count, Is.EqualTo(1));
    }

    [Test]
    public async Task TheMapAdvancesToTheNewVersionAndNeverGoesBackward()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var aggregator = new Aggregator(2, false, true, 200, ("localhost", _server!.Port));

        Assert.That(aggregator.GetLastVersion("persons"), Is.EqualTo("v001"));

        _store.FeedCollection("persons", "v002", new[] { new Item(new byte[10], 1) });

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && aggregator.GetLastVersion("persons") != "v002")
            await Task.Delay(100);

        Assert.That(aggregator.GetLastVersion("persons"), Is.EqualTo("v002"));
        Assert.That(aggregator.GetPoolsWithLastVersion("persons").Count, Is.EqualTo(1));
    }
}
