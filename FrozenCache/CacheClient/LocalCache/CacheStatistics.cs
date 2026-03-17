namespace CacheClient.LocalCache;

public record CacheStatistics(long Calls, long FoundInLocalCache, long CallsToExternalCache, long NotFoundInExternalCache, double AverageMillisecondsForExternalCache);