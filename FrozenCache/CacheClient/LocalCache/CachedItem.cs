namespace CacheClient.LocalCache;

public class CachedItem
{
    

    public byte[] Data { get; set; } = [];

    public required long PrimaryKey { get; init; }

    public bool IsNotFoundMarker => Data.Length == 0;
}