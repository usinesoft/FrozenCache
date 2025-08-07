using System.Diagnostics;
using System.Text;
using Messages;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using PersistentStore;
// ReSharper disable AccessToDisposedClosure

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
        public async Task FeedANonExistentCollection()
        {
            await using var store = new DataStore(StoreName);

            var items = new Item[] { new(new byte[100], 1, 200), new(new byte[1000], 2, 300) };
 
            Assert.Throws<CacheException>(
                () => _ = store.FeedCollection("persons", "001", items),
                "should throw store not opened");

            store.Open();

            Assert.Throws<CacheException>(() => _ =  store.FeedCollection("persons", "001", items), "should throw collection not found");
        }


        [Test]
        public async Task FeedFirstVersionOfACollection()
        {
            await using var store = new DataStore(StoreName);

            var items = new Item[] { new(new byte[100], 1, 200), new(new byte[1000], 2, 300) };

            store.Open();

            store.CreateCollection(new CollectionMetadata("persons", "id", "client_id"));
            
            _ = store.FeedCollection("persons", "001", items);

            var collections  = store.GetCollections();
            
            Assert.That(collections.Length, Is.EqualTo(1), "Store should have one collection after feeding first version");
            Assert.That(collections[0].Name, Is.EqualTo("persons"), "Collection name should match the one created");
            Assert.That(collections[0].LastVersion, Is.EqualTo("001"), "Last version should be '001' after first feed");
        }

        [Test]
        [TestCase(100, 1000)]
        [TestCase(2000, 5000)]
        public async Task FeedTwoMillionItemsInCollection(int smallObjectSize, int largeObjectSize)
        {
            await using (var store = new DataStore(StoreName))
            {

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

                _ =  store.FeedCollection("persons", "v001", items);

                var duration = watch.ElapsedMilliseconds;

                Console.WriteLine($"Feeding 2 million items took {duration} ms");

                var collections = store.GetCollections();
                Assert.That(collections.Length, Is.EqualTo(1),
                    "Store should have one collection after feeding first version");
                Assert.That(collections[0].Name, Is.EqualTo("persons"), "Collection name should match the one created");
                Assert.That(collections[0].LastVersion, Is.EqualTo("v001"),
                    "Last version should be '001' after first feed");
            }


            // retrieve items by primary key
            await using (var store = new DataStore(StoreName))
            {
                store.Open();
                var collections = store.GetCollections();
                Assert.That(collections.Length, Is.EqualTo(1), "Store should have one collection after reopening");
                Assert.That(collections[0].Name, Is.EqualTo("persons"), "Collection name should match the one created");
                Assert.That(collections[0].LastVersion, Is.EqualTo("v001"),
                    "Last version should be '001' after first feed");
                var item0 = store.GetByPrimaryKey("persons", 0).FirstOrDefault();
                var item1000 = store.GetByPrimaryKey("persons", 1000).FirstOrDefault();
                var item1001 = store.GetByPrimaryKey("persons", 1001).FirstOrDefault();

                Assert.That(item0, Is.Not.Null, "Item with id 0 should exist");
                Assert.That(item0!.Keys[0], Is.EqualTo(0));
                Assert.That(item0.Data.Length, Is.EqualTo(1000), "Item with id 1000 should have data length of 1000");

                Assert.That(item1000, Is.Not.Null, "Item with id 1000 should exist");
                Assert.That(item1000!.Data.Length, Is.EqualTo(1000), "Item with id 1000 should have data length of 1000");

                Assert.That(item1001, Is.Not.Null, "Item with id 1001 should exist");
                Assert.That(item1001!.Keys[0], Is.EqualTo(1001));
                Assert.That(item1001.Data.Length, Is.EqualTo(100), "Item with id 1000 should have data length of 1000");


                // check for items in the second segment
                var itemOther = store.GetByPrimaryKey("persons", 1_000_003).FirstOrDefault();
                Assert.That(itemOther, Is.Not.Null, "Item with id 1_000_003 should exist");
                Assert.That(itemOther!.Keys[0], Is.EqualTo(1_000_003));


                Benchmark(() =>
                {
                    for (int i = 100; i < 1100; i++)
                    {
                        _ = store.GetByPrimaryKey("persons", i * 100);
                    }

                }, "getting 1000 items form the store");
            }
                



        }


        [Test]
        public async Task FeedMultipleCollectionsAndRetrieveData()
        {
            await using (var store = new DataStore(StoreName))
            {
                store.Open();

                store.CreateCollection(new CollectionMetadata("first", "id", "name"));
                store.CreateCollection(new CollectionMetadata("second", "id", "name"));
                store.CreateCollection(new CollectionMetadata("third", "id", "name"));

                var items1 = GenerateItemsWithCollectionInformation(1000, "first");
                var items2 = GenerateItemsWithCollectionInformation(2000, "second");
                var items3 = GenerateItemsWithCollectionInformation(3000, "third");
                
                _ = store.FeedCollection("first", "v001", items1);
                _ = store.FeedCollection("second", "v001", items2);
                _ = store.FeedCollection("third", "v001", items3);
                
                var collections = store.GetCollections();
                Assert.That(collections.Length, Is.EqualTo(3), "Store should have three collections after feeding");
                Assert.That(collections[0].Name, Is.EqualTo("first"), "First collection should be 'first'");
                Assert.That(collections[1].Name, Is.EqualTo("second"), "Second collection should be 'second'");
                Assert.That(collections[2].Name, Is.EqualTo("third"), "Third collection should be 'third'");

                var item1 = store.GetByPrimaryKey("first", 10).FirstOrDefault();
                Assert.That(item1, Is.Not.Null);
                Assert.That(item1!.Keys.Length, Is.EqualTo(2));
                Assert.That(item1!.Keys[0], Is.EqualTo(10));
                var content = Encoding.UTF8.GetString(item1.Data);
                Assert.That(content, Is.EqualTo("first"));

                var item2 = store.GetByPrimaryKey("second", 101).FirstOrDefault();
                Assert.That(item2, Is.Not.Null);
                Assert.That(item2!.Keys.Length, Is.EqualTo(2));
                Assert.That(item2!.Keys[0], Is.EqualTo(101));
                content = Encoding.UTF8.GetString(item2.Data);
                Assert.That(content, Is.EqualTo("second"));

            }

            // reopen the store and check the data again
            await using (var store = new DataStore(StoreName))
            {
                store.Open();

                var item1 = store.GetByPrimaryKey("first", 10).FirstOrDefault();
                Assert.That(item1, Is.Not.Null);
                Assert.That(item1!.Keys.Length, Is.EqualTo(2));
                Assert.That(item1!.Keys[0], Is.EqualTo(10));
                var content = Encoding.UTF8.GetString(item1.Data);
                Assert.That(content, Is.EqualTo("first"));

                var item2 = store.GetByPrimaryKey("second", 101).FirstOrDefault();
                Assert.That(item2, Is.Not.Null);
                Assert.That(item2!.Keys.Length, Is.EqualTo(2));
                Assert.That(item2!.Keys[0], Is.EqualTo(101));
                content = Encoding.UTF8.GetString(item2.Data);
                Assert.That(content, Is.EqualTo("second"));
            }
        }


        private static List<Item> GenerateItemsWithCollectionInformation(int count, string collectionName)
        {
            var items = new List<Item>();
            for (int i = 0; i < count; i++)
            {
                var data = Encoding.UTF8.GetBytes(collectionName);

                items.Add(new Item(data, i, i*100));
            }
            return items;
        }


        public static void Benchmark(Action action, string actionDescription)
        {
            // execute once for warmup
            action();

            var watch = Stopwatch.StartNew();
            for (int i = 0; i < 10; i++)
            {
                action();
            }

            watch.Stop();
            Console.WriteLine($"One iteration of {actionDescription} took: {watch.ElapsedMilliseconds / 10f} ms");
        }
    }
}
