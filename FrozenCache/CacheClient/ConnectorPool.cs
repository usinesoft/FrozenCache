using System.Diagnostics;
using System.Threading.Channels;

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

    private readonly string _server;
    private readonly int _port;
    private readonly int _capacity;

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
    public ConnectorPool(int capacity, string server, int port, int watchDogFrequencyInMilliseconds = 10_000)
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

                        try
                        {
                            var testConnector = await Get();
                            if (await testConnector.Ping())
                            {
                                Debug.Print("watchdog : still connected");
                                serverUp = true;
                                Return(testConnector);
                            }
                        }
                        catch (Exception)
                        {
                            Debug.Print("watchdog : exception while checking connection");
                        }
                    }
                    else
                    {
                        Debug.Print("watchdog : not connected check with new connector");

                        try
                        {
                            using var connector = new Connector(_server, _port);


                            if (connector.Connect())
                            {
                                Debug.Print("watchdog : connected, waiting for ping response");
                                serverUp = await connector.Ping();
                                Debug.Print($"watchdog : server is up again:ping answer is {serverUp}");
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

    private void InternalConnect()
    {
        
            for (var i = 0; i < _capacity; i++)
            {
                var connector = new Connector(_server, _port);
                if (connector.Connect()) _pool.Writer.TryWrite(connector);
            }

            IsConnected = _pool.Reader.TryPeek(out _);
        
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