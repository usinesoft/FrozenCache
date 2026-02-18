using System.Diagnostics;

namespace CacheClient.LocalCache;

/// <summary>
/// Local cache based on a less recently used eviction policy. It is thread safe and can be used in a multithreaded environment.
/// </summary>
/// <param name="fetchFunc"></param>
public class LruLocalCache(Func<long, CachedItem?> fetchFunc, int evictionLimit = 1_000_000, int evictionCount = 100)
{
    private readonly Dictionary<long, CachedItem> _cache = new();

    private readonly LruEvictionPolicy _evictionPolicy = new(evictionLimit, evictionCount);

    private long _foundInLocalCache;
    private long _calls;
    private long _callsToExternalCache;
    private long _totalTicksInExternalCalls;

    private Stopwatch? _watch;

    public CacheStatistics GetStatistics()
    {
        lock (_cache)
        {
            var averageTicks = _callsToExternalCache > 0 ? (double)_totalTicksInExternalCalls / _callsToExternalCache : 0;
            var averageMilliseconds = averageTicks * 1000D / Stopwatch.Frequency;
            return new CacheStatistics(_calls, _foundInLocalCache, _callsToExternalCache, averageMilliseconds);
        }
    }


    public CachedItem? TryGet(long key)
    {
        lock (_cache)
        {
            _watch??= Stopwatch.StartNew();

            _calls++;

            var fromCache = _cache.GetValueOrDefault(key);


            if (fromCache != null)
            {
                _foundInLocalCache++;

                _evictionPolicy.Touch(fromCache);

                if (fromCache.IsNotFoundMarker)
                {
                    return null;
                }

                return fromCache;
            }

            _callsToExternalCache++;
            var before = _watch.ElapsedTicks;
            var fromExternalSource = fetchFunc(key);
            var after = _watch.ElapsedTicks;
            _totalTicksInExternalCalls += (after - before);

            if (fromExternalSource == null)
            {
                // add a not found marker to the cache to avoid fetching it again in the near future
                var notFoundMarker = new CachedItem
                {
                    PrimaryKey = key
                };

                _evictionPolicy.AddItem(notFoundMarker);

                _cache[key] = notFoundMarker;

                return null;
            }

            _evictionPolicy.AddItem(fromExternalSource);
            _cache[key] = fromExternalSource;

            
            // for now, we apply eviction policy only for real items , not for not found markers. 
            ApplyEvictionPolicy(); 

            
            return fromExternalSource;
        }


    }

    private void ApplyEvictionPolicy()
    {
        if (_evictionPolicy.IsEvictionRequired)
        {
            var toRemove = _evictionPolicy.DoEviction();
            foreach (var item in toRemove)
            {
                _cache.Remove(item.PrimaryKey);
            }
        }
    }
}