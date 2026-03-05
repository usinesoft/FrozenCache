namespace CacheClient.LocalCache;

/// <summary>
///     Mixed data structure. Can be used both as linked list and dictionary
///     Used to manage the eviction priority for cached items
///     The eviction candidates are at the beginning of the list
///     If an item is used it is moved at the end of the list.
///     Not thread safe, the caller should ensure the synchronization.
/// </summary>
public sealed class LruCachePolicy(int capacity, int evictionCount):ICachePolicy
{
    private readonly Dictionary<long, LinkedListNode<CachedItem>> _cachedObjectsByKey = new();

    private readonly LinkedList<CachedItem> _queue = [];


    /// <summary>
    ///     When eviction is needed <see cref="EvictionCount" /> items are removed. We remove more than one item to avoid too
    ///     frequent eviction
    /// </summary>
    public int EvictionCount { get; } = evictionCount;

    /// <summary>
    ///     Maximum capacity (if more items are added) eviction is needed
    /// </summary>
    public int Capacity { get; } = capacity;

    private int Count => _cachedObjectsByKey.Count;

    public bool EvictionRequired => _cachedObjectsByKey.Count > Capacity;

    public EvictionType Type => EvictionType.LessRecentlyUsed;
    public bool IsIncremental => true;

    public void Clear()
    {
        _cachedObjectsByKey.Clear();
        _queue.Clear();
    }

    /// <summary>
    ///     Add a new item to the eviction queue. The item is stored at the end (less likely to be evicted)
    ///     REQUIRE: Item not already present in the queue
    /// </summary>
    /// <param name="newItem"></param>
    public void AddNew(CachedItem newItem)
    {
        if (newItem == null) throw new ArgumentNullException(nameof(newItem));

        if (_cachedObjectsByKey.ContainsKey(newItem.PrimaryKey))
            throw new NotSupportedException("Item already in eviction queue");

        var lastNode = _queue.AddLast(newItem);
        _cachedObjectsByKey.Add(newItem.PrimaryKey, lastNode);

        ProceedToEviction();
    }

    /// <summary>
    ///     Proceed to eviction (the first <see cref="EvictionCount" /> items will be removed
    /// </summary>
    /// <returns>The items removed</returns>
    private void ProceedToEviction()
    {
        if (!EvictionRequired)
            return;

        var currentCount = 0;
        var node = _queue.First;


        while (currentCount < EvictionCount && node?.Next != null)
        {
            var nextNode = node.Next;

            _queue.Remove(node);
            _cachedObjectsByKey.Remove(node.Value.PrimaryKey);

            node = nextNode;
            currentCount++;
        }
    }


    /// <summary>
    ///     If found, move it at the end of the queue and return the item; otherwise return null
    /// </summary>
    public CachedItem? TryGet(long key)
    {
        if (_cachedObjectsByKey.TryGetValue(key, out var node))
        {
            _queue.Remove(node);
            _queue.AddLast(node);
            return node.Value;
        }


        return null;
    }
}