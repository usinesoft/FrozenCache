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
/// Verifies the StreamBySecondaryIndex request end to end through the real client/server stack: streaming
/// every document matching a named (non-primary) index's value back to the client, using the same manual
/// batch framing as StreamAllData/a feed session.
/// </summary>
public class StreamBySecondaryIndexTest
{
    private const string StoreName = "teststore_streambysecondaryindex";

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

    private static Item Order(long id, long customerId, byte marker) => new(new[] { marker }, id, customerId);

    [Test]
    public async Task StreamsEveryDocumentMatchingASecondaryIndexValueAcrossMultipleBatches()
    {
        const int matchingCount = 12_000; // spans more than 2 batches (the wire format batches 5_000 items at a time)

        _store.CreateCollection(new CollectionMetadata("orders", "id", "customerId"));

        var items = Enumerable.Range(0, matchingCount)
            .Select(i => Order(i, 42, (byte)(i % 256))) // all share customerId=42
            .Append(Order(999_999, 7, 1)); // one document with a different customerId, must not be returned

        _store.FeedCollection("orders", "v001", items);

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var received = new List<(long PrimaryKey, byte[] Data)>();
        await foreach (var item in connector.StreamBySecondaryIndex("orders", "customerId", 42))
            received.Add(item);

        Assert.That(received.Count, Is.EqualTo(matchingCount));
        Assert.That(received.Select(i => i.PrimaryKey).OrderBy(k => k),
            Is.EqualTo(Enumerable.Range(0, matchingCount).Select(i => (long)i)));

        // the connection must still be perfectly usable afterward
        var pingOk = await connector.Ping();
        Assert.That(pingOk, Is.True);

        var direct = await connector.QueryByPrimaryKey("orders", 999_999);
        Assert.That(direct.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task StreamingByAnUnmatchedSecondaryValueYieldsNoItemsAndLeavesTheConnectionUsable()
    {
        _store.CreateCollection(new CollectionMetadata("orders", "id", "customerId"));
        _store.FeedCollection("orders", "v001", new[] { Order(1, 100, 11) });

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var received = new List<(long PrimaryKey, byte[] Data)>();
        await foreach (var item in connector.StreamBySecondaryIndex("orders", "customerId", 999))
            received.Add(item);

        Assert.That(received, Is.Empty);

        var pingOk = await connector.Ping();
        Assert.That(pingOk, Is.True);
    }

    [Test]
    public void ThrowsForAnUnknownIndexName()
    {
        _store.CreateCollection(new CollectionMetadata("orders", "id", "customerId"));
        _store.FeedCollection("orders", "v001", new[] { Order(1, 100, 11) });

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var ex = Assert.ThrowsAsync<CacheException>(async () =>
        {
            await foreach (var _ in connector.StreamBySecondaryIndex("orders", "nonexistent", 100)) { }
        });

        Assert.That(ex!.Message, Does.Contain("no index named"));
    }

    [Test]
    public void ThrowsForANonExistentCollection()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var ex = Assert.ThrowsAsync<CacheException>(async () =>
        {
            await foreach (var _ in connector.StreamBySecondaryIndex("nonexistent", "customerId", 100)) { }
        });

        Assert.That(ex!.Message, Does.Contain("does not exist"));
    }

    [Test]
    public void ThrowsForACollectionWithNoDataYet()
    {
        _store.CreateCollection(new CollectionMetadata("orders", "id", "customerId"));
        // note: never fed, so it has no version yet

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var ex = Assert.ThrowsAsync<CacheException>(async () =>
        {
            await foreach (var _ in connector.StreamBySecondaryIndex("orders", "customerId", 100)) { }
        });

        Assert.That(ex!.Message, Does.Contain("no data"));
    }
}
