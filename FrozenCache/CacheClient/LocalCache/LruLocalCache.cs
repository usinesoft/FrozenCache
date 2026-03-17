using System.Diagnostics;

namespace CacheClient.LocalCache;

/// <summary>
///     Local cache based on a less recently used eviction policy. It is thread safe and can be used in a multithreaded
///     environment.
/// </summary>
/// <param name="fetchFunc"></param>
public class LruLocalCache(Func<long, CachedItem?> fetchFunc, int evictionLimit = 1_000_000, int evictionCount = 100)
{
    private readonly LruCachePolicy _evictionPolicy = new(evictionLimit, evictionCount);

    private long _foundInLocalCache;
    private long _calls;
    private long _callsToExternalCache;
    private long _totalTicksInExternalCalls;
    private long _notFoundInExternalCache;

    private Stopwatch? _watch;

    private readonly object _lock = new();

    public CacheStatistics GetStatistics()
    {
        return _statistics;
        
    }

    // We keep track of the statistics in a field to avoid creating a new object every time GetStatistics is called and to avoid the lock in GetStatistics. 
    // All data is updated at once to ensure consistency of the statistics. The statistics are updated only in TryGet, which is the only method that modifies the cache state, so we can be sure that the statistics are always up to date.
    private CacheStatistics _statistics = new CacheStatistics(0, 0, 0,0, 0);

    public CachedItem? TryGet(long key)
    {
        lock (_lock)
        {
            _watch ??= Stopwatch.StartNew();

            _calls++;

            var fromCache = _evictionPolicy.TryGet(key);

            if (fromCache != null)
            {
                _foundInLocalCache++;


                if (fromCache.IsNotFoundMarker) return null;

                return fromCache;
            }

            _callsToExternalCache++;
            var before = _watch.ElapsedTicks;
            var fromExternalSource = fetchFunc(key);
            var after = _watch.ElapsedTicks;
            _totalTicksInExternalCalls += after - before;

            if (fromExternalSource == null)
            {
                // add a not found marker to the cache to avoid fetching it again in the near future
                var notFoundMarker = new CachedItem
                {
                    PrimaryKey = key
                };

                _evictionPolicy.AddNew(notFoundMarker);

                _notFoundInExternalCache++;


                return null;
            }

            _evictionPolicy.AddNew(fromExternalSource);

            _statistics = new CacheStatistics(_calls, _foundInLocalCache, _callsToExternalCache, _notFoundInExternalCache, _callsToExternalCache > 0 ? (double)_totalTicksInExternalCalls / _callsToExternalCache * 1000D / Stopwatch.Frequency : 0);

            return fromExternalSource;
        }
    }
}