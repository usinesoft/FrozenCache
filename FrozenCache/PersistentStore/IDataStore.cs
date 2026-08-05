using Messages;
using Microsoft.Extensions.Logging;

namespace PersistentStore;

/// <summary>
/// Abstract definition of a frozen data store.
/// It contains collections of items, each with a schema that defines the indexes available.
/// Collections are versioned, and the most recent version is always available for fast access.
/// Once created the collections cannot be modified, but new versions can be created.
/// </summary>
public interface IDataStore
{
    /// <summary>
    /// Creates a new collection with the specified schema. The number of versions retained for this collection
    /// is controlled by <see cref="CollectionMetadata.MaxVersionsToKeep"/> on <paramref name="metadata"/>.
    /// </summary>
    /// <param name="metadata"></param>
    /// <returns>true if the collection was created successfully, false if it already exists and is compatible</returns>
    public bool CreateCollection(CollectionMetadata metadata);

    /// <summary>
    /// Retrieves all available collections in the data store.
    /// </summary>
    /// <returns></returns>
    public CollectionsDescription GetCollectionInformation();

    public void DropCollection(string name);

    /// <summary>
    /// Opens the data store for operations. This method should be called before any other operations.
    /// Data from the most recent version of each collection will be indexed in memory for fast access.
    /// </summary>
    public void Open(ILogger? logger = null);

    /// <summary>
    /// Retrieves an object's raw data in a collection by primary key. The caller already knows the key it
    /// queried by, so only the data is returned.
    /// </summary>
    /// <param name="collectionName"></param>
    /// <param name="keyValue"></param>
    /// <returns></returns>
    public List<byte[]> GetByPrimaryKey(string collectionName, long keyValue);

    /// <summary>
    /// Enumerates every document currently in a collection's active version, in on-disk order, each paired
    /// with its primary key. Any other key information a caller needs is expected to already be present in
    /// the serialized data itself.
    /// </summary>
    /// <param name="collectionName">name of an existing, already fed collection</param>
    public IEnumerable<(long PrimaryKey, byte[] Data)> StreamAllData(string collectionName);

    /// <summary>
    /// Enumerates every document in a collection's active version whose value at the named index equals
    /// <paramref name="keyValue"/>, each paired with its primary key. Works for any declared index (primary
    /// or secondary), though it exists mainly for secondary indexes - <see cref="GetByPrimaryKey"/> already
    /// covers fast primary-key lookups.
    /// </summary>
    /// <param name="collectionName">name of an existing, already fed collection</param>
    /// <param name="indexName">name of a declared index (primary or secondary) on the collection</param>
    /// <param name="keyValue"></param>
    public IEnumerable<(long PrimaryKey, byte[] Data)> StreamBySecondaryIndex(string collectionName, string indexName, long keyValue);

    /// <summary>
    /// Create a new version of a collection and index it in memory.
    /// The collection must already exist. This new version will be available when iteration ends;
    /// </summary>
    /// <param name="collectionName">name of an existing collection</param>
    /// <param name="newVersion">unique name of a new version</param>
    /// <param name="items"></param>
    public int FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items);


    /// <summary>
    /// Async version of FeedCollection.
    /// </summary>
    /// <param name="collectionName"></param>
    /// <param name="newVersion"></param>
    /// <param name="items"></param>
    /// <returns></returns>
    public Task<int> FeedCollectionAsync(string collectionName, string newVersion, IAsyncEnumerable<Item> items);

    public CollectionMetadata? GetCollectionMetadata(string collectionName);

}

