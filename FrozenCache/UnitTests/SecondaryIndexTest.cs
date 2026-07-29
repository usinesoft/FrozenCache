using Messages;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

/// <summary>
/// Verifies secondary indexes at the storage layer: every declared index (not just the primary) is built
/// during a feed, queryable via DataStore.StreamBySecondaryIndex, correctly rebuilt when a store is reopened,
/// and reconstructs each document's full, original-order key array regardless of which index found it.
/// All tests use the same 3-index schema (id, customerId, status) for consistency.
/// </summary>
public class SecondaryIndexTest
{
    private const string StoreName = "teststore_secondaryindex";

    [TearDown]
    public void Clean()
    {
        DataStore.Drop(StoreName);
    }

    [SetUp]
    public void Setup()
    {
        DataStore.Drop(StoreName);
    }

    private static CollectionMetadata OrdersMetadata() => new("orders", "id", "customerId", "status");

    private static Item Order(long id, long customerId, long status, byte marker) =>
        new(new[] { marker }, id, customerId, status);

    [Test]
    public void StreamBySecondaryIndexReturnsMatchingDocumentsWithOriginalKeyOrder()
    {
        using var store = new DataStore(StoreName, IndexType.Dictionary);
        store.Open();

        store.CreateCollection(OrdersMetadata());

        store.FeedCollection("orders", "v001", new[]
        {
            Order(1, 100, 1, 11),
            Order(2, 100, 2, 22),
            Order(3, 200, 1, 33),
            Order(4, 100, 1, 44)
        });

        var byCustomer = store.StreamBySecondaryIndex("orders", "customerId", 100).ToList();
        Assert.That(byCustomer.Select(i => i.Keys[0]).OrderBy(k => k), Is.EqualTo(new long[] { 1, 2, 4 }));

        foreach (var item in byCustomer)
        {
            // Keys must always be [id, customerId, status], regardless of which index found the document
            Assert.That(item.Keys.Length, Is.EqualTo(3));
            Assert.That(item.Keys[1], Is.EqualTo(100));
        }

        var byStatus = store.StreamBySecondaryIndex("orders", "status", 1).ToList();
        Assert.That(byStatus.Select(i => i.Keys[0]).OrderBy(k => k), Is.EqualTo(new long[] { 1, 3, 4 }));

        foreach (var item in byStatus)
            Assert.That(item.Keys[2], Is.EqualTo(1));

        // spot check full key + data reconstruction for one specific document found via a secondary index
        var order3 = byStatus.Single(i => i.Keys[0] == 3);
        Assert.That(order3.Keys, Is.EqualTo(new long[] { 3, 200, 1 }));
        Assert.That(order3.Data, Is.EqualTo(new byte[] { 33 }));
    }

    [Test]
    public void StreamBySecondaryIndexReturnsEmptyForAnUnmatchedValue()
    {
        using var store = new DataStore(StoreName, IndexType.Dictionary);
        store.Open();

        store.CreateCollection(OrdersMetadata());
        store.FeedCollection("orders", "v001", new[] { Order(1, 100, 1, 11) });

        var result = store.StreamBySecondaryIndex("orders", "customerId", 999).ToList();
        Assert.That(result, Is.Empty);
    }

    [Test]
    public void StreamBySecondaryIndexThrowsForAnUnknownIndexName()
    {
        using var store = new DataStore(StoreName, IndexType.Dictionary);
        store.Open();

        store.CreateCollection(OrdersMetadata());
        store.FeedCollection("orders", "v001", new[] { Order(1, 100, 1, 11) });

        Assert.Throws<CacheException>(() => store.StreamBySecondaryIndex("orders", "nonexistent", 100).ToList());
    }

    [Test]
    public void StreamBySecondaryIndexWorksWithThePrimaryIndexNameToo()
    {
        using var store = new DataStore(StoreName, IndexType.Dictionary);
        store.Open();

        store.CreateCollection(OrdersMetadata());
        store.FeedCollection("orders", "v001", new[] { Order(1, 100, 1, 11), Order(2, 200, 1, 22) });

        var result = store.StreamBySecondaryIndex("orders", "id", 2).ToList();
        Assert.That(result.Count, Is.EqualTo(1));
        Assert.That(result[0].Keys, Is.EqualTo(new long[] { 2, 200, 1 }));
    }

    [Test]
    public void PrimaryKeyLookupStillWorksCorrectlyAlongsideSecondaryIndexes()
    {
        using var store = new DataStore(StoreName, IndexType.Dictionary);
        store.Open();

        store.CreateCollection(OrdersMetadata());
        store.FeedCollection("orders", "v001", new[] { Order(1, 100, 1, 11), Order(2, 100, 2, 22) });

        var result = store.GetByPrimaryKey("orders", 2);
        Assert.That(result.Count, Is.EqualTo(1));
        Assert.That(result[0].Keys, Is.EqualTo(new long[] { 2, 100, 2 }));
        Assert.That(result[0].Data, Is.EqualTo(new byte[] { 22 }));
    }

    [Test]
    public void SecondaryIndexesAreRebuiltCorrectlyWhenTheStoreIsReopened()
    {
        using (var store = new DataStore(StoreName, IndexType.Dictionary))
        {
            store.Open();
            store.CreateCollection(OrdersMetadata());
            store.FeedCollection("orders", "v001", new[]
            {
                Order(1, 100, 1, 11),
                Order(2, 100, 2, 22),
                Order(3, 200, 1, 33)
            });
        }

        // reopen: this exercises ReadFileMap/IndexHeader's reopen path, not the fresh-feed path
        using var reopened = new DataStore(StoreName, IndexType.Dictionary);
        reopened.Open();

        var byCustomer = reopened.StreamBySecondaryIndex("orders", "customerId", 100).ToList();
        Assert.That(byCustomer.Select(i => i.Keys[0]).OrderBy(k => k), Is.EqualTo(new long[] { 1, 2 }));

        var byStatus = reopened.StreamBySecondaryIndex("orders", "status", 1).ToList();
        Assert.That(byStatus.Select(i => i.Keys[0]).OrderBy(k => k), Is.EqualTo(new long[] { 1, 3 }));

        // and the primary index must still be intact too
        var primary = reopened.GetByPrimaryKey("orders", 3);
        Assert.That(primary.Single().Keys, Is.EqualTo(new long[] { 3, 200, 1 }));
    }

    [Test]
    public void StreamBySecondaryIndexHandlesDuplicateSecondaryKeyValuesAcrossManyDocuments()
    {
        using var store = new DataStore(StoreName, IndexType.Dictionary);
        store.Open();

        store.CreateCollection(OrdersMetadata());

        const int itemCount = 5_000;
        store.FeedCollection("orders", "v001", Enumerable.Range(0, itemCount)
            .Select(i => Order(i, 42, 0, (byte)(i % 256)))); // every document shares the same customerId

        var result = store.StreamBySecondaryIndex("orders", "customerId", 42).ToList();
        Assert.That(result.Count, Is.EqualTo(itemCount));
        Assert.That(result.Select(i => i.Keys[0]).OrderBy(k => k), Is.EqualTo(Enumerable.Range(0, itemCount).Select(i => (long)i)));
    }
}
