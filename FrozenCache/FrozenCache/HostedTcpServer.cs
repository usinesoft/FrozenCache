using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Messages;
using Microsoft.Extensions.Options;
using PersistentStore;
using Serilog;
using static System.String;
using ILogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable S6667

namespace FrozenCache;

public class HostedTcpServer(IDataStore store, ILogger<HostedTcpServer> logger, IOptions<ServerSettings> configuration)
    : IHostedService
{
    public IDataStore Store { get; } = store;

    public ILogger Logger { get; } = logger;
    public IOptions<ServerSettings> Configuration { get; } = configuration;

    private readonly CancellationTokenSource _cts = new();

    private Channel<FeedItem>? _internalChannel;
    public int Port { get; private set; }

    private readonly FeedItemBatchSerializer _batchSerializer = new();
    private TcpListener? _listener;

    private bool _disposed;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Starting TCP server...");
        Debug.Print("Starting TCP server");

        var ct = _cts.Token;

        try
        {
            _listener = new TcpListener(IPAddress.Any, Configuration.Value.Port);
            _listener.Server.NoDelay = true; // Disable Nagle's algorithm for low latency

            _listener.Server.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.IPv6Only, 0);


            _listener.Start();

            if (!(_listener.LocalEndpoint is IPEndPoint endpoint))
                throw new NotSupportedException("Can not initialize server");

            Port = endpoint.Port;

            Debug.Print($"TCP server is listening on port {Port}");


            Logger.LogInformation("Server started on port {Port}", Port);


            _ = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var client = await _listener.AcceptTcpClientAsync(ct);
                        client.NoDelay = true; // Disable Nagle's algorithm for low latency


                        // client loop
                        _ = Task.Run(async () => { await ClientLoop(ct, client); }, ct);
                    }

                    Logger.LogWarning("Server stopped");
                }
                catch (OperationCanceledException)
                {
                    Logger.LogWarning("Server stopped");
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Error starting TCP server: {Message}", ex.Message);
                }
            }, ct);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error starting TCP server: {Message}", ex.Message);
            Debug.Print($"Error starting TCP server: {ex.Message}");
        }

        return Task.CompletedTask;
    }

    private async Task ClientLoop(CancellationToken cancellationToken, TcpClient client)
    {
        await using var stream = client.GetStream();
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await stream.ReadMessageAsync(cancellationToken);

                if (message == null)
                {
                    Logger.LogWarning("Client disconnected");
                    break; // Client disconnected 
                }

                // The ping request is a special case, it has no data, so we can respond immediately
                if (message is PingMessage ping)
                {
                    Debug.Print("server ping request received");
                    await stream.WriteMessageAsync(ping, cancellationToken);

                    continue;
                }

                switch (message)
                {
                    case BeginFeedRequest beginFeedRequest:
                        await ProcessFeedSession(beginFeedRequest, stream);
                        break;
                    case CreateCollectionRequest createCollectionRequest:
                        await ProcessCreateCollection(createCollectionRequest, stream, cancellationToken);
                        break;
                    case DropCollectionRequest dropCollectionRequest:
                        await ProcessDropCollection(dropCollectionRequest, stream, cancellationToken);
                        break;
                    case GetCollectionsDescriptionRequest:
                        var collections = Store.GetCollectionInformation();
                        await stream.WriteMessageAsync(collections, cancellationToken);
                        break;
                    case QueryByPrimaryKey queryRequest:
                        await ProcessSimpleQuery(queryRequest, stream, cancellationToken);
                        break;
                    default:
                        Logger.LogWarning("Unknown message type received: {Type}", message.GetType().Name);
                        await stream.WriteMessageAsync(new StatusResponse
                        { Success = false, ErrorMessage = "Unknown message type" }, cancellationToken);
                        break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected or operation was cancelled
            Logger.LogWarning("Cancellation requested");
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = "Operation cancelled" },
                cancellationToken);
        }
        catch (CacheException cacheEx)
        {
            Logger.LogError("Cache error processing client request: {Message}", cacheEx.Message);
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = cacheEx.Message },
                cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error processing client request: {Message}", ex.Message);
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = ex.Message },
                cancellationToken);
        }
        finally
        {
            stream.Close();
            client.Close();
        }
    }

    private async Task ProcessDropCollection(DropCollectionRequest dropCollectionRequest, NetworkStream stream,
        CancellationToken cancellationToken)
    {
        try
        {
            if (IsNullOrWhiteSpace(dropCollectionRequest.CollectionName))
                throw new ArgumentException("Collection name is empty");

            Log.Information("Drop collection message received for collection {Collection}",
                dropCollectionRequest.CollectionName);

            Store.DropCollection(dropCollectionRequest.CollectionName);
            await stream.WriteMessageAsync(new StatusResponse(), cancellationToken);

            Log.Information("Collection {Collection} was dropped", dropCollectionRequest.CollectionName);
        }
        catch (Exception e)
        {
            Logger.LogError("Error while dropping collection:{Message}", e.Message);
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = e.Message },
                cancellationToken);
        }
    }

    private async Task ProcessSimpleQuery(QueryByPrimaryKey queryRequest, Stream stream, CancellationToken ct)
    {
        try
        {
            if (IsNullOrWhiteSpace(queryRequest.CollectionName))
                throw new CacheException("Collection name is required in QueryByPrimaryKey request");

            var result = new ResultWithData { CollectionName = queryRequest.CollectionName };

            List<byte[]> temp = new List<byte[]>();

            foreach (var keyValue in queryRequest.PrimaryKeyValues)
            {
                var items = Store.GetByPrimaryKey(queryRequest.CollectionName, keyValue);

                foreach (var item in items) temp.Add(item.Data);
            }

            result.ObjectsData = temp.ToArray();


            result.SingleAnswer = true; //single message containing multiple items

            await stream.WriteMessageAsync(result, ct);
        }
        catch (Exception e)
        {
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = e.Message }, ct);
        }
    }

    private async Task ProcessCreateCollection(CreateCollectionRequest createRequest, Stream stream,
        CancellationToken ct)
    {
        try
        {
            if (IsNullOrWhiteSpace(createRequest.PrimaryKeyName))
                throw new CacheException("Primary key name is mandatory in CreateCollection request");

            Logger.LogInformation(
                "Creating collection {Collection} with primary key {PrimaryKey} and indexes {Indexes}",
                createRequest.CollectionName, createRequest.PrimaryKeyName, createRequest.OtherIndexes);

            var metadata = new CollectionMetadata(createRequest.CollectionName, createRequest.PrimaryKeyName,
                createRequest.OtherIndexes);


            Store.CreateCollection(metadata);

            await stream.WriteMessageAsync(new StatusResponse(), ct);
        }
        catch (Exception e)
        {
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = e.Message }, ct);
        }
    }


    private IEnumerable<FeedItem> ReadItems(Stream stream)
    {
        var reader = new BinaryReader(stream, Encoding.UTF8, true);

        while (true)
        {
            var msgs = _batchSerializer.Deserialize(reader);

            if (msgs.Count == 0)
                yield break; // End of stream

            foreach (var msg in msgs) yield return msg;
        }
    }

    private async Task ProcessFeedSession(BeginFeedRequest beginRequest, Stream stream)
    {
        try
        {
            if (IsNullOrWhiteSpace(beginRequest.CollectionName))
                throw new CacheException("Collection name is required");

            if (IsNullOrWhiteSpace(beginRequest.CollectionVersion))
                throw new CacheException("Collection version is required");

            var collectionName = beginRequest.CollectionName;
            var collectionVersion = beginRequest.CollectionVersion;

            var metadata = Store.GetCollectionMetadata(collectionName);
            if (metadata == null)
                throw new CacheException($"Collection {collectionName} does not exist");

            var oldVersion = metadata.LastVersion;
            if (oldVersion == collectionVersion)
                throw new CacheException($"Collection {collectionName} already has version {collectionVersion}");

            if (oldVersion != null &&
                Compare(oldVersion, collectionVersion, StringComparison.InvariantCultureIgnoreCase) > 0)
                throw new CacheException($"Collection {collectionName} already has a newer version {oldVersion}");

            // If we reach this point, we can start the feeding process. Let the client know about it
            await stream.WriteMessageAsync(new StatusResponse(), CancellationToken.None);

            Logger.LogInformation("Begin feeding collection {Collection}. New version is {Version}",
                beginRequest.CollectionName, beginRequest.CollectionVersion);

            var feederTask = StartCollectionFeeder(beginRequest.CollectionName, beginRequest.CollectionVersion);


            _internalChannel = Channel.CreateBounded<FeedItem>(1_000_000);

            foreach (var item in ReadItems(stream))
                await _internalChannel!.Writer.WriteAsync(item, CancellationToken.None);

            _internalChannel.Writer.Complete();

            await feederTask;

            Logger.LogInformation("Feeding collection {Collection} completed", beginRequest.CollectionName);

            await stream.WriteMessageAsync(new StatusResponse(), CancellationToken.None);
        }
        catch (Exception e)
        {
            Logger.LogError("Error while processing feed session for collection {Collection} {Version}: {Message}",
                beginRequest.CollectionName, beginRequest.CollectionVersion, e.Message);
            await stream.WriteMessageAsync(new StatusResponse { ErrorMessage = e.Message, Success = false },
                CancellationToken.None);
        }
    }


    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Stopping TCP server...");

        if (_disposed) return;

        // Cancel the internal token source to stop the server
        await _cts.CancelAsync();

        _listener?.Dispose();

        _cts.Dispose();

        _disposed = true;

        await Task.Delay(200, cancellationToken);
    }

    private async IAsyncEnumerable<Item> ItemsFromChannel()
    {
        if (_internalChannel == null)
            throw new InvalidOperationException("Internal channel is not initialized");

        await foreach (var item in _internalChannel.Reader.ReadAllAsync()) yield return new Item(item.Data, item.Keys);
    }

    private Task StartCollectionFeeder(string collectionName, string collectionVersion)
    {
        var task = Task.Run(async () =>
        {
            try
            {
                await Store.FeedCollectionAsync(collectionName, collectionVersion, ItemsFromChannel());
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error while feeding collection {Collection}: {Message}", collectionName, e.Message);
            }
        });

        return task;
    }
}