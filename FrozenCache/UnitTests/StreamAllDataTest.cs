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

    /// <summary>
    /// Reproduces the reported race: a client feeding a new version of a collection used to dispose the
    /// memory-mapped files of the version another client was still streaming, out from under it. Feeds a
    /// third version too, low enough retention that the version being streamed becomes a prune target while
    /// still leased - proving that a failed prune (blocked by the active read) doesn't break the feed either.
    /// </summary>
    [Test]
    public async Task StreamingContinuesSafelyWhileNewVersionsAreFedConcurrently()
    {
        const int itemCount = 50_000;

        _store.CreateCollection(new CollectionMetadata("persons", "id") { MaxVersionsToKeep = 2 });
        _store.FeedCollection("persons", "v001", GenerateMarkedItems(itemCount, marker: 1));

        using var streamingConnector = new Connector("localhost", _server!.Port);
        streamingConnector.Connect();

        using var feedingConnector = new Connector("localhost", _server!.Port);
        feedingConnector.Connect();

        var received = new List<Item>();

        await using var enumerator = streamingConnector.StreamAllData("persons").GetAsyncEnumerator();

        // consume a handful of items first, to make sure the stream is genuinely mid-flight (holding a read
        // lease on v001's CollectionStore) before anything else happens
        for (var i = 0; i < 100; i++)
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            received.Add(enumerator.Current);
        }

        // feed v002 on a separate connection while the stream above is still active. Awaiting this to
        // completion guarantees EndFeed (and its Dispose of the v001 store) has already run server-side.
        await feedingConnector.FeedCollection("persons", "v002", GenerateMarkedItems(1_000, marker: 2));

        // feed v003 too: with MaxVersionsToKeep=2, this makes v001 a prune target while it's still leased by
        // the stream - the directory deletion must fail quietly and not fail this feed
        await feedingConnector.FeedCollection("persons", "v003", GenerateMarkedItems(1_000, marker: 3));

        // keep consuming the rest of the original stream: it must still see a fully intact v001 snapshot,
        // never a crash, never a mix of markers from the newer versions
        while (await enumerator.MoveNextAsync())
            received.Add(enumerator.Current);

        Assert.That(received.Count, Is.EqualTo(itemCount));
        Assert.That(received.All(i => i.Data[0] == 1), Is.True,
            "The stream must see a consistent snapshot of v001 throughout, never data from v002/v003");

        // and regular queries must now see v003, the current version
        using var queryConnector = new Connector("localhost", _server!.Port);
        queryConnector.Connect();

        var queried = await queryConnector.QueryByPrimaryKey("persons", 0);
        Assert.That(queried[0][0], Is.EqualTo(3));
    }

    private static IEnumerable<Item> GenerateItems(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var size = i % 2 == 0 ? 50 : 150;
            yield return new Item(new byte[size], i);
        }
    }

    private static IEnumerable<Item> GenerateMarkedItems(int count, byte marker)
    {
        for (var i = 0; i < count; i++)
            yield return new Item([marker], i);
    }
}
