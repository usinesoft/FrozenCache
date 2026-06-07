namespace PersistentStore;

/// <summary>
///     An index on a key that is mostly discriminant. This means that most of the time the key is unique, but not always.
/// </summary>
public sealed class DictionaryIndex : IIndex
{
    /// <summary>
    ///     This is an index used to store documents by most discriminant key.
    ///     The key is unique most of the time but not always. To reduce memory usage, we store two different collections for
    ///     duplicate and unique values.
    /// </summary>
    private readonly Dictionary<long, IndexEntry[]> _byMostDiscriminantKey = new();

    /// <summary>
    ///     For unique keys, we store a single entry per key.
    /// </summary>
    private readonly Dictionary<long, IndexEntry> _byMostDiscriminantKeyUnique = new();

    public void Add(long key, IndexEntry newEntry)
    {
        // if the key is not unique copy the entry in the dictionary for duplicates
        if (_byMostDiscriminantKeyUnique.TryGetValue(key, out var entry))
        {
            // if already a duplicate, add to the existing entries
            if (_byMostDiscriminantKey.TryGetValue(key, out var entries))
            {
                var newEntries = new IndexEntry[entries.Length + 1];
                Array.Copy(entries, newEntries, entries.Length);
                newEntries[^1] = newEntry;
                _byMostDiscriminantKey[key] = newEntries;
            }
            else
            {
                // create a new entry for the duplicate key
                _byMostDiscriminantKey[key] =
                [
                    entry,
                    newEntry
                ];
            }
        }
        else
        {
            _byMostDiscriminantKeyUnique[key] =
                newEntry;
        }
    }

    public void PostProcess()
    {
        if (_byMostDiscriminantKey is { Count: > 0 }) // duplicate keys exist
            // remove duplicate keys from the unique collection
            foreach (var key in _byMostDiscriminantKey.Keys)
                _byMostDiscriminantKeyUnique.Remove(key);

        var duplicatedValues = _byMostDiscriminantKey.Values.Sum(x => x.Length);
        var uniqueValues = _byMostDiscriminantKeyUnique.Count;

        NonUniqueKeys = duplicatedValues;
        ObjectCount = duplicatedValues + uniqueValues;
    }

    public int ObjectCount { get; private set; }

    public int NonUniqueKeys { get; private set; }

    public List<IndexEntry> Get(long keyValue)
    {
        List<IndexEntry> result = new();


        // first search in the unique key collection
        if (_byMostDiscriminantKeyUnique.TryGetValue(keyValue, out var value))
        {
            result.Add(value);
            return result;
        }

        // if not found, search in the duplicate key collection
        if (_byMostDiscriminantKey.TryGetValue(keyValue, out var values))
            foreach (var entry in values)
                result.Add(entry);


        return result;
    }
}