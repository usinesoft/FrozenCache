namespace CacheClient;

/// <summary>
/// Raised by <see cref="ConnectorPool"/>'s watchdog when it observes that a collection's last version has
/// changed since the previous check against that server.
/// </summary>
public sealed record NewVersionEventArgs(string CollectionName, string NewVersion);
