using System.Diagnostics;
using PersistentStore;


public class NullDataStore : IDataStore
{
    public void CreateCollection(CollectionMetadata metadata, int maxVersionToKeep = 2)
    {

    }

    public CollectionMetadata[] GetCollections()
    {
        throw new NotImplementedException();
    }

    public void DropCollection(string name)
    {
        throw new NotImplementedException();
    }

    public void Open()
    {
        throw new NotImplementedException();
    }

    public Item? GetByPrimaryKey(string collectionName, long keyValue)
    {
        if (keyValue == 12)
        {
            return new Item(new byte[121], keyValue);
        }

        return null;
    }

    public int FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        var watch = Stopwatch.StartNew();

        int count = 0;
        foreach (var item in items)
        {
            count++;
        }

        watch.Stop();

        Console.WriteLine($"Read {count} items in {watch.ElapsedMilliseconds} ms");

        return count;
    }
}