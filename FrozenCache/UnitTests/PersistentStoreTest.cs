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
    }
}
