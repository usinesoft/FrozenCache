using NUnit.Framework;

namespace UnitTests;

public class LocalCacheTest
{
    [Test]
    public void KeepTrackOfItemsNotFoundInExternalSource()
    {
        int callsToExternalSource = 0;

        var cache = new CacheClient.LocalCache.LruLocalCache(key =>
        {
            callsToExternalSource++;
            return null;
        });

        var none = cache.TryGet(120);

        Assert.That(none, Is.Null);
        Assert.That(callsToExternalSource, Is.EqualTo(1));

        // The second time we ask for the same key, it should not call the external source again.
        none = cache.TryGet(120);
        Assert.That(none, Is.Null);
        Assert.That(callsToExternalSource, Is.EqualTo(1));

    }

    [Test]
    public void EvictionPolicyIsApplied()
    {
        int callsToExternalSource = 0;
        var cache = new CacheClient.LocalCache.LruLocalCache(key =>
        {
            callsToExternalSource++;
            return new CacheClient.LocalCache.CachedItem
            {
                PrimaryKey = key,
                Data = [0, 1, 2]
            };
        }, evictionLimit: 3, evictionCount: 1);

        // add 3 items to the cache
        cache.TryGet(1);
        cache.TryGet(2);
        cache.TryGet(3);
        
        Assert.That(callsToExternalSource, Is.EqualTo(3));
        
        // add a fourth item, this should trigger eviction of the first one (key=1)
        cache.TryGet(4);
        Assert.That(callsToExternalSource, Is.EqualTo(4));

        // [2, 3, 4] should be in the cache now, and 1 should have been evicted.

        // ask for the first item again, it should call the external source again because it was evicted
        var item1 = cache.TryGet(1);
        Assert.That(item1, Is.Not.Null);
        Assert.That(item1!.PrimaryKey, Is.EqualTo(1));
        Assert.That(callsToExternalSource, Is.EqualTo(5));

        // [3, 4, 1] should be in the cache now, and 2 should have been evicted.

        // ask for item 3, it should be in the cache
        var item3 = cache.TryGet(3);
        Assert.That(item3, Is.Not.Null);
        Assert.That(item3!.PrimaryKey, Is.EqualTo(3));
        Assert.That(callsToExternalSource, Is.EqualTo(5));

        var stats = cache.GetStatistics();
        Console.WriteLine(stats);
    }
}