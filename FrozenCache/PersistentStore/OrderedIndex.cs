namespace PersistentStore;

public sealed class OrderedIndex : IIndex
{
    
    /// <summary>
    /// Temporary storage
    /// </summary>
    private readonly List<long> _temporaryKeysList = new(100_000_000);
    
    private readonly List<IndexEntry> _temporaryValuesList = new(100_000_000);

    private long[] _keys = [];
    
    private IndexEntry[] _values =[];

    public void Add(long key, IndexEntry newEntry)
    {
        _temporaryKeysList.Add(key);
        _temporaryValuesList.Add(newEntry);
    }
    public void PostProcess()
    {
        _keys = _temporaryKeysList.ToArray();
        _values = _temporaryValuesList.ToArray();

        _temporaryKeysList.Clear();
        _temporaryValuesList.Clear();

        ObjectCount = _keys.Length;

        Array.Sort(_keys, _values);
    }
    public int ObjectCount { get; private set; }
    public int NonUniqueKeys => 0;
    public List<IndexEntry> Get(long keyValue)
    { 
        List<IndexEntry> result = new();

        int index = Array.BinarySearch(_keys, keyValue);

        if (index >= 0)
        {
            result.Add(_values[index]);
            
            // for non-unique keys, we would need to check adjacent entries in the sorted array
            if(index > 0 && _keys[index - 1] == keyValue)
            {
                int i = index - 1;
                while (i >= 0 && _keys[i] == keyValue)
                {
                    result.Add(_values[i]);
                    i--;
                }
            }
            
            if(index < _keys.Length - 1 && _keys[index + 1] == keyValue)
            {
                int i = index + 1;
                while (i < _keys.Length && _keys[i] == keyValue)
                {
                    result.Add(_values[i]);
                    i++;
                }
            }
        }

        return result;
    }

    public IEnumerable<KeyValuePair<long, IndexEntry>> GetAll()
    {
        for (var i = 0; i < _keys.Length; i++)
            yield return new KeyValuePair<long, IndexEntry>(_keys[i], _values[i]);
    }
}