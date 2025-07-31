using System.Net;
using System.Net.Sockets;
using System.Text;
using Messages;
using Microsoft.Extensions.Options;
using PersistentStore;
using Serilog;
using ILogger = Microsoft.Extensions.Logging.ILogger;

#pragma warning disable S6667

namespace FrozenCache;

public class HostedTcpServer(IDataStore store, ILogger<HostedTcpServer> logger, IOptions<ServerSettings> configuration) : IHostedService
{
    public IDataStore Store { get; } = store;
    
    public ILogger Logger { get; } = logger;
    public IOptions<ServerSettings> Configuration { get; } = configuration;

    readonly CancellationTokenSource _cts = new();

    public int Port { get; private set; }

    public  Task StartAsync(CancellationToken cancellationToken)
    {
        
        Logger.LogInformation("Starting TCP server...");

        var ct = _cts.Token;

        try
        {
            var listener = new TcpListener(IPAddress.Any, Configuration.Value.Port);
            listener.Server.NoDelay = true; // Disable Nagle's algorithm for low latency
            
            listener.Server.SetSocketOption(SocketOptionLevel.IPv6, SocketOptionName.IPv6Only, 0);

            
            listener.Start();

            if (!(listener.LocalEndpoint is IPEndPoint endpoint))
                throw new NotSupportedException("Can not initialize server");

            Port = endpoint.Port;

            Logger.LogInformation("Server started on port {Port}", Port);


            _ = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var client = await listener.AcceptTcpClientAsync(ct);
                        client.NoDelay = true; // Disable Nagle's algorithm for low latency
                        //client.SendBufferSize = 1024 * 1024; // 1 MB send buffer
                        //client.ReceiveBufferSize = 1024 * 1024; // 1 MB receive buffer

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
        }

        return Task.CompletedTask;

    }

    private async Task ClientLoop(CancellationToken cancellationToken, TcpClient client)
    {
        try
        {
            await using var stream = client.GetStream();

            while (!cancellationToken.IsCancellationRequested)
            {

                var message = await stream.ReadMessageAsync(cancellationToken);

                if(message == null)
                {
                    Logger.LogWarning("Client disconnected");
                    break; // Client disconnected 
                }

                // The ping request is a special case, it has no data, so we can respond immediately
                if (message is PingMessage ping)
                {
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
                    case QueryByPrimaryKey queryRequest:
                        await ProcessSimpleQuery(queryRequest, stream, cancellationToken);
                        break;
                }
                        

            }


        }
        catch(OperationCanceledException)
        {
            // Client disconnected or operation was cancelled
            Logger.LogWarning("Cancellation requested");
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error processing client request: {Message}", ex.Message);

        }
        finally
        {
            client.Close();
                    
        }
    }

    private async Task ProcessDropCollection(DropCollectionRequest dropCollectionRequest, NetworkStream stream, CancellationToken cancellationToken)
    {
        try
        {
            
            if (string.IsNullOrWhiteSpace(dropCollectionRequest.CollectionName))
                throw new ArgumentException("Collection name is empty");

            Log.Information("Drop collection message received for collection {Collection}", dropCollectionRequest.CollectionName);

            Store.DropCollection(dropCollectionRequest.CollectionName);
            await stream.WriteMessageAsync(new StatusResponse(), cancellationToken);

            Log.Information("Collection {Collection} was dropped", dropCollectionRequest.CollectionName);
        }
        catch (Exception e)
        {
            Logger.LogError("Error while dropping collection:{Message}", e.Message);
            await stream.WriteMessageAsync(new StatusResponse{Success = false, ErrorMessage = e.Message}, cancellationToken);
        }
    }

    private async Task ProcessSimpleQuery(QueryByPrimaryKey queryRequest, Stream stream, CancellationToken ct)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(queryRequest.CollectionName))
                throw new CacheException("Collection name is required in QueryByPrimaryKey request");

            var result = new ResultWithData{CollectionName = queryRequest.CollectionName};

            List<byte[]> temp = new List<byte[]>();

            foreach (var keyValue in queryRequest.PrimaryKeyValues)
            {
                var item = Store.GetByPrimaryKey(queryRequest.CollectionName, keyValue);
                if(item != null)
                {
                    temp.Add(item.Data);
                }
            
            }

            result.ObjectsData = temp.ToArray();

            result.SingleAnswer = true;
            await stream.WriteMessageAsync(result, ct);
        }
        catch (Exception e)
        {
            await stream.WriteMessageAsync(new StatusResponse { Success = false, ErrorMessage = e.Message }, ct);
        }
    }

    private async Task ProcessCreateCollection(CreateCollectionRequest createRequest, Stream stream, CancellationToken ct)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(createRequest.PrimaryKeyName))
            {
                throw new CacheException("Primary key name is mandatory in CreateCollection request");
            }

            Logger.LogInformation("Creating collection {Collection} with primary key {PrimaryKey} and indexes {Indexes}",
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


    static IEnumerable<Item> ReadItems(Stream stream)
    {
        var reader = new BinaryReader(stream, Encoding.UTF8, true);

        while (true)
        {

            var msg= FeedItem.Deserialize(reader);

            if(msg.IsEndOfStream)
                yield break; // End of stream

            yield return new Item(msg.Data, msg.Keys);
            
        }
        
    }

    private async Task ProcessFeedSession(BeginFeedRequest beginRequest, Stream stream)
    {

        if (string.IsNullOrWhiteSpace(beginRequest.CollectionName))
            throw new CacheException("Collection name is required");

        if (string.IsNullOrWhiteSpace(beginRequest.CollectionVersion))
            throw new CacheException("Collection version is required");

        Logger.LogInformation("Begin feeding collection {Collection}. New version is {Version}", beginRequest.CollectionName, beginRequest.CollectionVersion);

        
        Store.FeedCollection(beginRequest.CollectionName, beginRequest.CollectionVersion, ReadItems(stream));

        await stream.WriteMessageAsync(new StatusResponse(), CancellationToken.None);

    }

    
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("Stopping TCP server...");

        // Cancel the internal token source to stop the server
        await _cts.CancelAsync();

        await Task.Delay(200, cancellationToken);


    }
}