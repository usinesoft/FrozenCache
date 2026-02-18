namespace CacheClient.LocalCache;

/// <summary>
///     Mixed data structure. Can be used both as linked list and dictionary
///     Used to manage the eviction priority for cached items
///     The eviction candidates are at the beginning of the list
///     If an item is used it is moved at the end of the list.
///     Not thread safe, the caller should ensure the synchronization. It is not responsible for eviction itself, but only
///     for managing the priority of items and providing the eviction candidates when eviction is needed
/// </summary>
public class EvictionQueue
{
    private readonly Dictionary<long, LinkedListNode<CachedItem>> _cachedObjectsByKey = new();

    private readonly LinkedList<CachedItem> _queue = [];


    /// <summary>
    ///     When eviction is needed <see cref="EvictionCount" /> items are removed
    /// </summary>
    public int EvictionCount { get; set; }

    /// <summary>
    ///     Maximum capacity (if more items are added) eviction is needed
    /// </summary>
    public int Capacity { get; set; }

    private int Count => _cachedObjectsByKey.Count;

    public bool EvictionRequired => _cachedObjectsByKey.Count > Capacity;

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
    }

    /// <summary>
    ///     Proceed to eviction (the first <see cref="EvictionCount" /> items will be removed
    /// </summary>
    /// <returns>The items removed</returns>
    public IList<CachedItem> Go()
    {
        var result = new List<CachedItem>(EvictionCount);
        if (!EvictionRequired)
            return result;


        var currentCount = 0;
        var node = _queue.First;

        //remove more the Capacity - Count to avoid the eviction to be triggered for each added item
        var itemsToRemove = Count - Capacity + EvictionCount - 1;
        if (itemsToRemove <= 0)
            return result;

        while (currentCount < itemsToRemove && node?.Next != null)
        {
            var nextNode = node.Next;

            _queue.Remove(node);
            _cachedObjectsByKey.Remove(node.Value.PrimaryKey);
            result.Add(node.Value);

            node = nextNode;
            currentCount++;
        }


        return result;
    }


    /// <summary>
    ///     Mark the item as used. Moves it at the end of the queue
    ///     If the item is not present ignore (maybe useful if certain items are excluded by the eviction policy)
    /// </summary>
    /// <param name="item"></param>
    public void Touch(CachedItem item)
    {
        if (item == null) throw new ArgumentNullException(nameof(item));

        if (_cachedObjectsByKey.TryGetValue(item.PrimaryKey, out var node))
        {
            _queue.Remove(node);
            _queue.AddLast(node);
        }
    }
}