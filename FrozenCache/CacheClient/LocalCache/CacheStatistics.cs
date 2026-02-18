namespace CacheClient.LocalCache;

public record CacheStatistics(long Calls, long FoundInLocalCache, long CallsToExternalCache, double AverageMillisecondsForExternalCache);