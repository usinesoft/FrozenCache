using PersistentStore;

namespace FrozenCache;

public class ServerSettings
{
    public int Port { get; set; }

    /// <summary>
    /// The dictionary index is slightly faster for lookups, but uses more memory. The ordered index is slower for lookups, but uses less memory.
    /// </summary>
    public IndexType PrimaryIndexType { get; set; } = IndexType.Dictionary;
}




