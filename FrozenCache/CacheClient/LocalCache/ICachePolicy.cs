namespace CacheClient.LocalCache;

/// <summary>
/// Abstract cache policy
/// </summary>
public interface ICachePolicy
{
    
    EvictionType Type { get; }

    /// <summary>
    /// An incremental cache is fed item by item, while a non-incremental cache is fed at initialization.
    /// Incremental caches are for large quantities of data and usually implement automatic eviction, while non-incremental caches are for small quantities of data.
    /// </summary>
    bool IsIncremental { get; }

    /// <summary>
    /// Clear the cache. Called by client code, generally when the external data store was updated.
    /// </summary>
    void Clear();

    /// <summary>
    ///  Add a new item to the cache. Not available for non-incremental cache policies. 
    /// </summary>
    /// <param name="newItem"></param>
    void AddNew(CachedItem newItem);

    /// <summary>
    ///     If found, move it at the end of the queue and return the item; otherwise return null
    /// </summary>
    CachedItem? TryGet(long key);
}