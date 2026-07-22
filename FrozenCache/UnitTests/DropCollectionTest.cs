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
/// Integration coverage for DropCollection through the real client/server/store stack - previously untested.
/// </summary>
public class DropCollectionTest
{
    private const string StoreName = "teststore_dropcollection";

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
    public async Task DropCollectionRemovesItFromDescriptionAndDisk()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var collectionPath = Path.Combine(StoreName, "persons");
        Assert.That(Directory.Exists(collectionPath), Is.True, "Collection directory should exist after feeding");

        await connector.DropCollection("persons", ignoreIfNotFound: false);

        var description = await connector.GetCollectionsDescription();
        Assert.That(description.Collections.Any(c => c.Name == "persons"), Is.False,
            "A dropped collection should no longer be listed");

        Assert.That(Directory.Exists(collectionPath), Is.False, "The collection directory should be removed from disk");
    }

    [Test]
    public async Task QueryingADroppedCollectionFails()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        await connector.DropCollection("persons", ignoreIfNotFound: false);

        Assert.ThrowsAsync<CacheException>(async () => await connector.QueryByPrimaryKey("persons", 1));
    }

    [Test]
    public void DroppingANonExistentCollectionThrowsWhenNotIgnored()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        var ex = Assert.ThrowsAsync<CacheException>(async () =>
            await connector.DropCollection("nonexistent", ignoreIfNotFound: false));

        Assert.That(ex!.Message, Does.Contain("Failed to drop collection"));
    }

    [Test]
    public async Task DroppingANonExistentCollectionSucceedsByDefault()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        // ignoreIfNotFound defaults to true - must not throw
        await connector.DropCollection("nonexistent");
    }

    [Test]
    public async Task CollectionCanBeRecreatedWithADifferentSchemaAfterBeingDropped()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        await connector.DropCollection("persons", ignoreIfNotFound: false);

        // a schema that would have been rejected as incompatible if the old collection were still there
        await connector.CreateCollection("persons", "id", "name");
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1, 100) });

        var result = await connector.QueryByPrimaryKey("persons", 1);
        Assert.That(result.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task DroppingACollectionWithMultipleVersionsRemovesAllOfThem()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });
        await connector.FeedCollection("persons", "v002", new[] { new Item(new byte[10], 1) });

        var collectionPath = Path.Combine(StoreName, "persons");
        Assert.That(Directory.EnumerateDirectories(collectionPath).Count(), Is.EqualTo(2),
            "Both versions should exist before dropping");

        await connector.DropCollection("persons", ignoreIfNotFound: false);

        Assert.That(Directory.Exists(collectionPath), Is.False);
    }

    /// <summary>
    /// Dropping a collection that's being streamed must fail clearly and leave the collection completely
    /// untouched - not throw a raw file-locked IOException, and not silently schedule a deferred disposal
    /// that would tear the collection down later on its own, without the drop ever actually succeeding.
    /// </summary>
    [Test]
    public async Task DroppingACollectionWhileItIsBeingStreamedFailsClearlyAndLeavesItFullyUsable()
    {
        using var streamingConnector = new Connector("localhost", _server!.Port);
        streamingConnector.Connect();

        using var dropConnector = new Connector("localhost", _server!.Port);
        dropConnector.Connect();

        await streamingConnector.CreateCollection("persons", "id");
        await streamingConnector.FeedCollection("persons", "v001", GenerateItems(50_000));

        await using var enumerator = streamingConnector.StreamAllData("persons").GetAsyncEnumerator();
        for (var i = 0; i < 100; i++)
            Assert.That(await enumerator.MoveNextAsync(), Is.True);

        // drop the collection on a separate connection while the stream above is still active
        var ex = Assert.ThrowsAsync<CacheException>(async () =>
            await dropConnector.DropCollection("persons", ignoreIfNotFound: false));
        Assert.That(ex!.Message, Does.Contain("streamed").Or.Contain("use"),
            "The error should explain a stream is active, not leak a raw file-locked message");

        // the collection must remain fully queryable right after the failed drop
        var pointLookup = await dropConnector.QueryByPrimaryKey("persons", 1);
        Assert.That(pointLookup.Count, Is.EqualTo(1));

        // the stream itself must be unaffected and able to finish reading its full snapshot
        var remaining = 0;
        while (await enumerator.MoveNextAsync())
            remaining++;
        Assert.That(remaining, Is.EqualTo(49_900));

        // now that the stream has finished, retrying the drop must succeed
        await dropConnector.DropCollection("persons", ignoreIfNotFound: false);
        Assert.ThrowsAsync<CacheException>(async () => await dropConnector.QueryByPrimaryKey("persons", 1));
    }

    private static IEnumerable<Item> GenerateItems(int count)
    {
        for (var i = 0; i < count; i++)
            yield return new Item(new byte[10], i);
    }

    [Test]
    public async Task DroppingAnUnrelatedCollectionDoesNotAffectOthers()
    {
        using var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        await connector.CreateCollection("invoices", "id");
        await connector.FeedCollection("invoices", "v001", new[] { new Item(new byte[10], 1) });

        await connector.DropCollection("persons", ignoreIfNotFound: false);

        var result = await connector.QueryByPrimaryKey("invoices", 1);
        Assert.That(result.Count, Is.EqualTo(1), "Dropping one collection should not affect an unrelated one");
    }
}
