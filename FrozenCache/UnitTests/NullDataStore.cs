using System.Diagnostics;
using Microsoft.Extensions.Logging;
using PersistentStore;


public class NullDataStore : IDataStore
{
    public void CreateCollection(CollectionMetadata metadata, int maxVersionToKeep = 2)
    {

    }

    public CollectionMetadata[] GetCollections()
    {
        return [new CollectionMetadata("testCollection", "id"){LastVersion = null}];
    }

    public void DropCollection(string name)
    {
        throw new NotImplementedException();
    }

    public void Open(ILogger? logger = null)
    {
        throw new NotImplementedException();
    }

    public List<Item> GetByPrimaryKey(string collectionName, long keyValue)
    {
        if (keyValue == 12)
        {
            return [new Item(new byte[121], keyValue)];
        }

        return [];
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

    public async Task<int> FeedCollectionAsync(string collectionName, string newVersion, IAsyncEnumerable<Item> items)
    {
        var watch = Stopwatch.StartNew();

        int count = 0;
        await foreach (var item in items)
        {
            count++;
        }

        watch.Stop();

        Console.WriteLine($"Read {count} items in {watch.ElapsedMilliseconds} ms");

        return count;
    }
}