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
/// Verifies the StreamAllData request: streaming every document of a collection back to the client using the
/// same manual big-batch framing as a feed session, in reverse.
/// </summary>
public class StreamAllDataTest
{
    private const string StoreName = "teststore_streamalldata";

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
    public async Task StreamAllDataReturnsEveryDocumentAcrossMultipleBatches()
    {
        const int itemCount = 12_000; // spans more than 2 batches (the wire format batches 5_000 items at a time)

        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", GenerateItems(itemCount));

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var received = new List<Item>();
        await foreach (var item in connector.StreamAllData("persons"))
            received.Add(item);

        Assert.That(received.Count, Is.EqualTo(itemCount));

        var byKey = received.ToDictionary(i => i.Keys[0]);
        for (var i = 0; i < itemCount; i += 997) // sample across the whole range, including batch boundaries
        {
            Assert.That(byKey.ContainsKey(i), Is.True, $"Missing item with key {i}");
            Assert.That(byKey[i].Data.Length, Is.EqualTo(i % 2 == 0 ? 50 : 150));
        }

        // the connection must still be perfectly usable for a normal request afterward - proving the manual
        // batch framing fully resynchronized with the regular per-message protocol
        var pingOk = await connector.Ping();
        Assert.That(pingOk, Is.True, "The connection should still answer pings after a stream completes");

        var queryResult = await connector.QueryByPrimaryKey("persons", 42);
        Assert.That(queryResult.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task StreamAllDataOfAnEmptyCollectionYieldsNoItemsAndLeavesTheConnectionUsable()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        _store.FeedCollection("persons", "v001", []);

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var received = new List<Item>();
        await foreach (var item in connector.StreamAllData("persons"))
            received.Add(item);

        Assert.That(received, Is.Empty);

        var pingOk = await connector.Ping();
        Assert.That(pingOk, Is.True);
    }

    [Test]
    public void StreamAllDataThrowsForANonExistentCollection()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var ex = Assert.ThrowsAsync<CacheException>(async () =>
        {
            await foreach (var _ in connector.StreamAllData("nonexistent")) { }
        });

        Assert.That(ex!.Message, Does.Contain("does not exist"));
    }

    [Test]
    public void StreamAllDataThrowsForACollectionWithNoDataYet()
    {
        _store.CreateCollection(new CollectionMetadata("persons", "id"));
        // note: never fed, so it has no version yet

        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var ex = Assert.ThrowsAsync<CacheException>(async () =>
        {
            await foreach (var _ in connector.StreamAllData("persons")) { }
        });

        Assert.That(ex!.Message, Does.Contain("no data"));
    }

    private static IEnumerable<Item> GenerateItems(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var size = i % 2 == 0 ? 50 : 150;
            yield return new Item(new byte[size], i);
        }
    }
}
