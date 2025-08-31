using System.Diagnostics;
using Messages;
using Microsoft.Extensions.Logging;
using PersistentStore;

namespace ProfilingTool;

public class NullDataStore : IDataStore
{
    public void CreateCollection(CollectionMetadata metadata, int maxVersionToKeep = 2)
    {

    }

    public CollectionsDescription GetCollectionInformation()
    {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
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

    public CollectionMetadata? GetCollectionMetadata(string collectionName)
    {
        throw new NotImplementedException();
    }
}