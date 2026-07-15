using System.Text;
using CacheClient;
using FrozenCache;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

/// <summary>
/// Verifies that <see cref="Aggregator"/> collects <see cref="ConnectorPool.NewVersion"/> from its replicas,
/// re-raises it to its own clients, and clears the matching local cache before doing so.
/// </summary>
public class AggregatorVersionWatchTest
{
    private const string StoreName = "teststore_aggregatorversionwatch";

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
    public async Task NewVersionEventClearsLocalCacheAndIsPropagatedToClients()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(Encoding.UTF8.GetBytes("v1-data"), 1) });

        var aggregator = new Aggregator(2, false, true, 200, ("localhost", _server!.Port));
        aggregator.ConfigureLocalCache("persons", 1000);

        // prime the local cache
        var firstRead = await aggregator.QueryRawDataByPrimaryKey("persons", 1);
        Assert.That(Encoding.UTF8.GetString(firstRead[0]), Is.EqualTo("v1-data"));
        Assert.That(aggregator.GetStatistics()["persons"].CallsToExternalCache, Is.EqualTo(1));

        // second read should be served from the local cache, not the server
        await aggregator.QueryRawDataByPrimaryKey("persons", 1);
        Assert.That(aggregator.GetStatistics()["persons"].CallsToExternalCache, Is.EqualTo(1),
            "the second read should have been served from the local cache");

        var events = new List<NewVersionEventArgs>();
        aggregator.NewVersion += (_, e) => events.Add(e);

        await Task.Delay(500); // let the watchdog establish its baseline version

        // feed a new version with different data for the same key
        _store.FeedCollection("persons", "v002", new[] { new Item(Encoding.UTF8.GetBytes("v2-data"), 1) });

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && events.Count == 0)
            await Task.Delay(100);

        Assert.That(events.Count, Is.EqualTo(1), "The NewVersion event should have been propagated to the aggregator's clients");
        Assert.That(events[0].CollectionName, Is.EqualTo("persons"));
        Assert.That(events[0].NewVersion, Is.EqualTo("v002"));

        // the stale entry must have been evicted: this read has to go back to the server and see the new data
        var afterVersionChange = await aggregator.QueryRawDataByPrimaryKey("persons", 1);
        Assert.That(Encoding.UTF8.GetString(afterVersionChange[0]), Is.EqualTo("v2-data"),
            "Stale cached data should have been cleared when the collection's version changed");
        Assert.That(aggregator.GetStatistics()["persons"].CallsToExternalCache, Is.EqualTo(2),
            "The read after the cache was cleared should have gone back to the server");
    }

    [Test]
    public async Task NewVersionEventIsNotRaisedWhenNoLocalCacheIsConfigured()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var aggregator = new Aggregator(2, false, true, 200, ("localhost", _server!.Port));
        // no ConfigureLocalCache call for "persons"

        var events = new List<NewVersionEventArgs>();
        aggregator.NewVersion += (_, e) => events.Add(e);

        await Task.Delay(500); // establish baseline

        _store.FeedCollection("persons", "v002", new[] { new Item(new byte[10], 1) });

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && events.Count == 0)
            await Task.Delay(100);

        // the event still fires even without a local cache configured for the collection
        Assert.That(events.Count, Is.EqualTo(1));
        Assert.That(events[0].CollectionName, Is.EqualTo("persons"));
        Assert.That(events[0].NewVersion, Is.EqualTo("v002"));
    }
}
