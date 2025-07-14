using System.Diagnostics;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using PersistentStore;

namespace UnitTests
{
    public class PersistentStoreTest
    {

        const string StoreName = "teststore";


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


        [Test]
        public void NewStoreHasNoCollections()
        {
            using var store = new DataStore(StoreName);

            var collections = store.GetCollections();
            
            CollectionAssert.IsEmpty(collections, "New store should have no collections");

        }


        [Test]
        public void CreateCollectionsInNewDatastore()
        {
            using var store = new DataStore(StoreName);

            var metadata = new CollectionMetadata("persons", "id", "name", "age");

            store.CreateCollection(metadata);

            Assert.Throws<CacheException>(() => store.CreateCollection(metadata),
                "Creating a collection that already exists should throw exception");

            var collections = store.GetCollections();

            Assert.That(collections.Length, Is.EqualTo(1), "Store should have one collection after creation");

            Assert.That(collections[0].Name, Is.EqualTo("persons"), "Collection name should match the one created");

            Assert.That(collections[0].Indexes.Count, Is.EqualTo(3), "Collection should have 3 indexes");

            Assert.That(collections[0].Indexes[0].Name, Is.EqualTo("id"), "First index should be the primary key");
            
            Assert.That(collections[0].Indexes[0].IsUnique, Is.True, "Primary key index should be unique");

            Assert.That(collections[0].Indexes[1].Name, Is.EqualTo("name"), "Second index should be 'name'");
            
            Assert.That(collections[0].Indexes[1].IsUnique, Is.False, "Index 'name' should not be unique");

            store.DropCollection("persons");

            collections = store.GetCollections();

            CollectionAssert.IsEmpty(collections, "Store should have no collections after dropping the collection");
        }

        [Test]
        public void FeedANonExistentCollection()
        {
            using var store = new DataStore(StoreName);

            var items = new Item[] { new(new byte[100], 1, 200), new(new byte[1000], 2, 300) };
 
            Assert.Throws<CacheException>(() => store.FeedCollection("persons", "001", items), "should throw store not opened");

            store.Open();
            Assert.Throws<CacheException>(() => store.FeedCollection("persons", "001", items), "should throw collection not found");
        }


        [Test]
        public void FeedFirstVersionOfACollection()
        {
            using var store = new DataStore(StoreName);

            var items = new Item[] { new(new byte[100], 1, 200), new(new byte[1000], 2, 300) };

            store.Open();

            store.CreateCollection(new CollectionMetadata("persons", "id", "client_id"));
            
            store.FeedCollection("persons", "001", items);

            var collections  = store.GetCollections();
            Assert.That(collections.Length, Is.EqualTo(1), "Store should have one collection after feeding first version");
            Assert.That(collections[0].Name, Is.EqualTo("persons"), "Collection name should match the one created");
            Assert.That(collections[0].LastVersion, Is.EqualTo("001"), "Last version should be '001' after first feed");
        }

        [Test]
        public void FeedTwoMillionItemsInCollection()
        {
            using var store = new DataStore(StoreName);

            byte[] data1 = new byte[1000];
            byte[] data2 = new byte[100];

            var items = new List<Item>();
            for (int i = 0; i < 2_000_000; i++)
            {
                items.Add(i % 2 == 0 ? new Item(data1, i, i + 1) : new Item(data2, i, i + 1));
            }


            store.Open();

            store.CreateCollection(new CollectionMetadata("persons", "id", "client_id"));

            Stopwatch watch = Stopwatch.StartNew();

            store.FeedCollection("persons", "v001", items);

            var duration = watch.ElapsedMilliseconds;
            
            Console.WriteLine($"Feeding 2 million items took {duration} ms");

            var collections = store.GetCollections();
            Assert.That(collections.Length, Is.EqualTo(1), "Store should have one collection after feeding first version");
            Assert.That(collections[0].Name, Is.EqualTo("persons"), "Collection name should match the one created");
            Assert.That(collections[0].LastVersion, Is.EqualTo("v001"), "Last version should be '001' after first feed");

        }
    }
}
