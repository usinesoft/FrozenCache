using Messages;

namespace CacheClient;

/// <summary>
/// The server closed the connection instead of answering: it stopped or crashed, or the client and server
/// disagree on SSL. Unlike other <see cref="CacheException"/>s, this is not an error reported by the server, so
/// the operation may succeed on another replica.
/// </summary>
public sealed class ConnectionClosedException(string message) : CacheException(message);
