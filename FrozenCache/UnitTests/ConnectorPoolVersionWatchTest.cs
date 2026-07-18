using CacheClient;
using FrozenCache;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

/// <summary>
/// Verifies that <see cref="ConnectorPool"/>'s watchdog detects collection version changes on a real server
/// and raises <see cref="ConnectorPool.NewVersion"/> accordingly.
/// </summary>
public class ConnectorPoolVersionWatchTest
{
    private const string StoreName = "teststore_versionwatch";

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
    public async Task WatchdogRaisesNewVersionWhenACollectionsLastVersionChanges()
    {
        // establish a baseline version before the pool starts watching
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var events = new List<NewVersionEventArgs>();

        using var pool = new ConnectorPool(2, "localhost", _server!.Port, watchDogFrequencyInMilliseconds: 200);
        pool.NewVersion += (_, e) => events.Add(e);

        // let a couple of watchdog ticks pass so the first observation (the baseline) happens
        await Task.Delay(500);

        Assert.That(events, Is.Empty,
            "No event should be raised for the version observed on the first check, only on a change");

        // feed a new version while the pool is watching
        _store.FeedCollection("persons", "v002", [new Item(new byte[10], 1)]);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && events.Count == 0)
            await Task.Delay(100);

        Assert.That(events.Count, Is.EqualTo(1), "Exactly one NewVersion event should have been raised");
        Assert.That(events[0].CollectionName, Is.EqualTo("persons"));
        Assert.That(events[0].NewVersion, Is.EqualTo("v002"));
    }

    [Test]
    public async Task WatchdogDoesNotRaiseNewVersionWhenNothingChanges()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var events = new List<NewVersionEventArgs>();

        using var pool = new ConnectorPool(2, "localhost", _server!.Port, watchDogFrequencyInMilliseconds: 200);
        pool.NewVersion += (_, e) => events.Add(e);

        // several ticks with no feed in between
        await Task.Delay(1000);

        Assert.That(events, Is.Empty, "No event should be raised when no version has changed");
    }

    [Test]
    public async Task WatchdogRaisesOneEventPerChangedCollection()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.CreateCollection(new CollectionMetadata("invoices", "id"));
        _store.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });
        _store.FeedCollection("invoices", "v001", new[] { new Item(new byte[10], 1) });

        var events = new List<NewVersionEventArgs>();

        using var pool = new ConnectorPool(2, "localhost", _server!.Port, watchDogFrequencyInMilliseconds: 200);
        pool.NewVersion += (_, e) => events.Add(e);

        await Task.Delay(500); // establish baseline for both collections

        // only "invoices" gets a new version
        _store.FeedCollection("invoices", "v002", new[] { new Item(new byte[10], 1) });

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline && events.Count == 0)
            await Task.Delay(100);

        // give it a bit more time to make sure "persons" doesn't also fire
        await Task.Delay(500);

        Assert.That(events.Count, Is.EqualTo(1), "Only the collection whose version actually changed should raise an event");
        Assert.That(events[0].CollectionName, Is.EqualTo("invoices"));
        Assert.That(events[0].NewVersion, Is.EqualTo("v002"));
    }
}
