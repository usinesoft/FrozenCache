using System.Net.Sockets;
using Messages;
using PersistentStore;

namespace FrozenCache;

public class HostedTcpServer(IDataStore store, ILogger<HostedTcpServer> logger) : IHostedService
{
    public IDataStore Store { get; } = store;
    
    public ILogger<HostedTcpServer> Logger { get; } = logger;

    

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        
        var listener = new TcpListener(System.Net.IPAddress.Any, 5000);
        listener.Start();


        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await listener.AcceptTcpClientAsync(cancellationToken);

            // client loop
            _ = Task.Run(async () =>
            {
                try
                {
                    await using var stream = client.GetStream();

                    

                    while (true)
                    {

                        var message = await stream.ReadMessageAsync(cancellationToken);

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
                        }
                        

                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
                finally
                {
                    client.Close();
                    
                }

            },cancellationToken);
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


    private async Task ProcessFeedSession(BeginFeedRequest beginRequest, Stream stream)
    {
        Logger.LogInformation("Begin feeding collection {Collection}. New version is {Version}", beginRequest.CollectionName, beginRequest.CollectionVersion);

        throw new NotImplementedException();
    }

    
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}