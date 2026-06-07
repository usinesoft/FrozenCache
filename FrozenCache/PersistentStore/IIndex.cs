namespace PersistentStore;

internal interface IIndex
{
    
    /// <summary>
    ///   Add a new entry to the index for the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="newEntry"></param>
    void Add(long key, IndexEntry newEntry);

    /// <summary>
    /// After all entries have been added, this method is called to trigger any post-processing needed to optimize the index for reads.  
    /// </summary>
    void PostProcess();

    /// <summary>
    ///   Get the list of entries for the specified key. 
    /// </summary>
    /// <param name="keyValue"></param>
    /// <returns></returns>
    List<IndexEntry> Get(long keyValue);

    int ObjectCount { get; }
    int NonUniqueKeys { get; }

}