//! Per-collection LRU cache in front of the servers: the counterpart of the .NET `LruLocalCache`.

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Duration;

use lru::LruCache;

/// Statistics of a collection's local cache, since it was configured.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CacheStatistics {
    /// Keys looked up.
    pub calls: u64,
    /// Keys answered by the local cache, including known missing keys.
    pub found_in_local_cache: u64,
    /// Keys fetched from a server.
    pub calls_to_server: u64,
    /// Fetched keys that the server didn't have either (then cached as missing).
    pub not_found_on_server: u64,
    /// Average duration of a server fetch.
    pub average_server_call: Duration,
}

/// The documents of one collection, by primary key: `Some(data)`, or `None` for a key known to be missing (so
/// that repeated lookups of missing keys don't reach the server). Holds one document per key: for collections
/// with a unique primary key.
pub(crate) struct LocalCache {
    entries: Mutex<LruCache<i64, Option<Vec<u8>>>>,
    statistics: Mutex<Counters>,
}

#[derive(Default)]
struct Counters {
    calls: u64,
    hits: u64,
    fetches: u64,
    not_found: u64,
    fetch_time: Duration,
}

/// Result of looking a key up in the local cache.
pub(crate) enum Lookup {
    /// The cached document.
    Found(Vec<u8>),
    /// Known to be missing on the server.
    Missing,
    /// Not cached: to fetch from a server.
    Unknown,
}

impl LocalCache {
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap();
        LocalCache {
            entries: Mutex::new(LruCache::new(capacity)),
            statistics: Mutex::new(Counters::default()),
        }
    }

    pub fn lookup(&self, key: i64) -> Lookup {
        let result = match self.entries.lock().unwrap().get(&key) {
            Some(Some(data)) => Lookup::Found(data.clone()),
            Some(None) => Lookup::Missing,
            None => Lookup::Unknown,
        };

        let mut statistics = self.statistics.lock().unwrap();
        statistics.calls += 1;
        if !matches!(result, Lookup::Unknown) {
            statistics.hits += 1;
        }
        result
    }

    /// Records what a server returned for `key` (`None`: no such document), and how long it took.
    pub fn store(&self, key: i64, data: Option<Vec<u8>>, elapsed: Duration) {
        {
            let mut statistics = self.statistics.lock().unwrap();
            statistics.fetches += 1;
            statistics.fetch_time += elapsed;
            if data.is_none() {
                statistics.not_found += 1;
            }
        }
        self.entries.lock().unwrap().put(key, data);
    }

    /// Discards every entry, e.g. because the collection was fed a new version. Statistics are kept.
    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }

    pub fn statistics(&self) -> CacheStatistics {
        let statistics = self.statistics.lock().unwrap();
        CacheStatistics {
            calls: statistics.calls,
            found_in_local_cache: statistics.hits,
            calls_to_server: statistics.fetches,
            not_found_on_server: statistics.not_found,
            average_server_call: if statistics.fetches == 0 {
                Duration::ZERO
            } else {
                statistics.fetch_time / statistics.fetches as u32
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hits_misses_missing_keys_and_eviction() {
        let cache = LocalCache::new(2);

        assert!(matches!(cache.lookup(1), Lookup::Unknown));
        cache.store(1, Some(vec![1]), Duration::from_millis(2));
        cache.store(2, None, Duration::from_millis(4));

        assert!(matches!(cache.lookup(1), Lookup::Found(data) if data == [1]));
        assert!(matches!(cache.lookup(2), Lookup::Missing));

        // capacity 2: adding a third evicts the least recently used (1, used before 2)
        cache.store(3, Some(vec![3]), Duration::from_millis(3));
        assert!(matches!(cache.lookup(1), Lookup::Unknown));
        assert!(matches!(cache.lookup(3), Lookup::Found(_)));

        let statistics = cache.statistics();
        assert_eq!(statistics.calls, 5);
        assert_eq!(statistics.found_in_local_cache, 3);
        assert_eq!(statistics.calls_to_server, 3);
        assert_eq!(statistics.not_found_on_server, 1);
        assert_eq!(statistics.average_server_call, Duration::from_millis(3));

        cache.clear();
        assert!(matches!(cache.lookup(3), Lookup::Unknown));
    }

    #[test]
    fn an_empty_document_is_not_mistaken_for_a_missing_one() {
        // the .NET cache uses an empty byte array as its "not found" marker
        let cache = LocalCache::new(10);
        cache.store(1, Some(vec![]), Duration::ZERO);
        assert!(matches!(cache.lookup(1), Lookup::Found(data) if data.is_empty()));
    }
}
