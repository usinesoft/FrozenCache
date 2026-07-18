using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Messages;

namespace CacheClient;

/// <summary>
///     Manage a pool of connectors to the cache server. If the pool is empty the client awaits for a connector to become
///     available.
///     One pool is created for each cache server.
/// </summary>
public sealed class ConnectorPool : IDisposable, IAsyncDisposable
{
    private readonly Channel<Connector> _pool = Channel.CreateUnbounded<Connector>();

    
    public string Address { get; }

    public bool IsConnected { get; private set; }

    /// <summary>
    /// Raised by the watchdog when a collection's last version differs from the one observed on the previous
    /// check against this server.
    /// </summary>
    public event EventHandler<NewVersionEventArgs>? NewVersion;

    /// <summary>
    /// Last version seen for each collection, by collection name. Written only by the watchdog task (which
    /// runs one iteration at a time), but read by arbitrary caller threads wanting to route a query to a
    /// server known to have a specific collection's latest version - hence the concurrent dictionary.
    /// </summary>
    private readonly ConcurrentDictionary<string, string> _lastKnownVersions = new();

    /// <summary>
    /// Last known version of every collection this server has reported, by collection name.
    /// </summary>
    public IReadOnlyDictionary<string, string> LastKnownVersions => _lastKnownVersions;

    private readonly string _server;
    private readonly int _port;
    private readonly int _capacity;
    private readonly bool _useSsl;
    private readonly bool _validateServerCertificate;

    /// <summary>
    ///     Watchdog task to monitor the connection status and reconnect if necessary.
    /// </summary>
    private readonly Task _watchDogTask;

    private readonly CancellationTokenSource _cancellationTokenSource = new();

    private void ClearPool()
    {
        while(_pool.Reader.TryRead(out var connector))
        {
            connector.Dispose();
        }
    }
    

    /// <summary>
    ///     Manage a pool of connectors to the cache server. If the pool is empty the client awaits for a connector to become
    ///     available.
    ///     One pool is created for each cache server.
    /// </summary>
    public ConnectorPool(int capacity, string server, int port, int watchDogFrequencyInMilliseconds = 10_000,
        bool useSsl = false, bool validateServerCertificate = true)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be at least 1");

        if (string.IsNullOrWhiteSpace(server))
            throw new ArgumentNullException(nameof(server), "Server cannot be null or empty");

        if (port is <= 0 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(port), "Port must be between 1 and 65535");

        Address = $"{server}:{port}";

        _server = server;
        _port = port;

        _capacity = capacity;
        _useSsl = useSsl;
        _validateServerCertificate = validateServerCertificate;

        InternalConnect();

        var tk = _cancellationTokenSource.Token;

        // Start the watchdog task to monitor the connection status and reconnect if necessary.
        _watchDogTask = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(watchDogFrequencyInMilliseconds), tk);

                    Debug.Print($"watchdog begin;frequency = {watchDogFrequencyInMilliseconds} ");

                    tk.ThrowIfCancellationRequested();

                    var serverUp = false;

                    if (IsConnected)
                    {
                        Debug.Print("watchdog : is connected check with pooled connector");

                        Connector? testConnector = null;
                        try
                        {
                            testConnector = await Get();
                            await CheckCollectionVersions(testConnector);
                            Debug.Print("watchdog : still connected");
                            serverUp = true;
                        }
                        catch (Exception)
                        {
                            Debug.Print("watchdog : exception while checking connection");
                        }
                        finally
                        {
                            // always return the connector, whether the version check succeeded or not -
                            // otherwise a failed check silently drains the pool by one connector each time
                            if (testConnector != null)
                                Return(testConnector);
                        }
                    }
                    else
                    {
                        Debug.Print("watchdog : not connected check with new connector");

                        try
                        {
                            using var connector = new Connector(_server, _port, _useSsl, _validateServerCertificate);


                            if (connector.Connect())
                            {
                                Debug.Print("watchdog : connected, checking collection versions");
                                try
                                {
                                    await CheckCollectionVersions(connector);
                                    serverUp = true;
                                }
                                catch (Exception)
                                {
                                    Debug.Print("watchdog : exception while checking collection versions on reconnect");
                                }

                                Debug.Print($"watchdog : server is up again:{serverUp}");
                            }
                            else
                            {
                                Debug.Print("watchdog : connect returned false");
                            }
                        }
                        catch (Exception e)
                        {
                            Debug.Print($"watchdog : exception while connecting:{e.Message}");
                            // ignore connection errors, we will try to reconnect later
                        }
                    }


                    if (!serverUp)
                    {
                        IsConnected = false;

                        ClearPool();

                        
                        Debug.Print("watchdog : server is down, clearing pool and marking as not connected");
                    }

                    // If the server is up and the pool is empty, we can try to reconnect
                    if (serverUp && !IsConnected)
                    {
                        Debug.Print("watchdog : reconnect");

                        try
                        {
                            InternalConnect();
                        }
                        catch (Exception)
                        {
                            Debug.Print("watchdog : reconnect failed, pool is still empty");
                        }
                    }

                    Debug.Print("watchdog end");
                }
            }
            catch (OperationCanceledException)
            {
                Debug.Print("watchdog operation canceled exception");
                // ignore
            }
            catch (Exception)
            {
                Debug.Print("watchdog exception");
            }
        }, tk);
    }

    /// <summary>
    /// Fetches the current collections description from the server and raises <see cref="NewVersion"/> for
    /// every collection whose last version differs from the one observed on the previous check.
    /// </summary>
    /// <param name="connector"></param>
    /// <param name="raiseEvents">
    /// When false, versions are recorded but no event is raised even for a collection observed for the first
    /// time. Used for the one-off baseline established right after connecting, so that a freshly (re)connected
    /// pool doesn't fire an event for every pre-existing collection it just discovered. Regular watchdog ticks
    /// always raise events (the default), including for a collection seen for the first time on this pool -
    /// once the baseline exists, a genuinely new name showing up is real news.
    /// </param>
    private async Task CheckCollectionVersions(Connector connector, bool raiseEvents = true)
    {
        var description = await connector.GetCollectionsDescription();

        foreach (var collection in description.Collections)
        {
            if (_lastKnownVersions.TryGetValue(collection.Name, out var previousVersion) &&
                previousVersion == collection.LastVersion)
                continue;

            _lastKnownVersions[collection.Name] = collection.LastVersion;

            if (raiseEvents)
                NewVersion?.Invoke(this, new NewVersionEventArgs(collection.Name, collection.LastVersion));
        }
    }

    private void InternalConnect()
    {
        for (var i = 0; i < _capacity; i++)
        {
            var connector = new Connector(_server, _port, _useSsl, _validateServerCertificate);
            if (connector.Connect()) _pool.Writer.TryWrite(connector);
        }

        IsConnected = _pool.Reader.TryPeek(out _);

        if (!IsConnected)
            return;

        // establish the collection-version baseline immediately, so a caller has usable data as soon as
        // this pool reports connected, instead of waiting for the first watchdog tick
        try
        {
            var connector = Get().GetAwaiter().GetResult();
            try
            {
                CheckCollectionVersions(connector, raiseEvents: false).GetAwaiter().GetResult();
            }
            finally
            {
                Return(connector);
            }
        }
        catch (Exception)
        {
            Debug.Print("InternalConnect : exception while establishing the initial version baseline");
        }
    }


    /// <summary>
    ///     Gets a connector from the pool, waiting if necessary until one becomes available.
    /// </summary>
    /// <returns>A connector from the pool.</returns>
    public async Task<Connector> Get()
    {
        if (!IsConnected)
            throw new InvalidOperationException(
                "The pool is not connected to the server. Please check the connection status.");

        return await _pool.Reader.ReadAsync();
        
    }

    /// <summary>
    ///     Returns a connector back to the pool.
    /// </summary>
    /// <param name="connector">The connector to return.</param>
    public void Return(Connector connector)
    {
        if (connector == null) throw new ArgumentNullException(nameof(connector), "Connector cannot be null");

        if (!connector.IsHealthy)
        {
            connector.Dispose();
            MarkAsNotConnected();
            return;
        }
        
        _pool.Writer.TryWrite(connector);
    }

    private bool _disposed;
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cancellationTokenSource.Cancel();

        _watchDogTask.Wait();

        _watchDogTask.Dispose();

        _cancellationTokenSource.Dispose();

        ClearPool();
    }

    

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _cancellationTokenSource.CancelAsync();

        await _watchDogTask.WaitAsync(CancellationToken.None);

        _watchDogTask.Dispose();

        _cancellationTokenSource.Dispose();

        ClearPool();
    }

    /// <summary>
    ///     Called when a connection loss is detected by external code before the internal watchdog task runs.
    /// </summary>
    public void MarkAsNotConnected()
    {
        
        IsConnected = false;
    
        ClearPool();
    
    }
}