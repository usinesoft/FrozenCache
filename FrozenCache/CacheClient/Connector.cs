using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Messages;
using PersistentStore;

namespace CacheClient;

/// <param name="host"></param>
/// <param name="port"></param>
/// <param name="useSsl">Wrap the connection in TLS. The server must have <c>ServerSettings:UseSsl</c> enabled too.</param>
/// <param name="validateServerCertificate">
/// When true (the default), the server certificate must be trusted and match <paramref name="host" />. Set to
/// false only for testing against a self-signed/untrusted certificate - it disables all certificate checks.
/// </param>
public sealed class Connector(string host, int port, bool useSsl = false, bool validateServerCertificate = true) : IDisposable
{
    private TcpClient? _client;

    private Stream? _stream;

    private readonly FeedItemBatchSerializer _batchSerializer = new();

    public string Address => $"{host}:{port}";

    public bool IsHealthy => _client?.Connected == true && _stream != null;

    /// <summary>
    /// Describes why the last call to <see cref="Connect"/> returned false. Null after a successful connect,
    /// or before the first call.
    /// </summary>
    public string? LastError { get; private set; }

    public bool Connect()
    {
        if (_client != null) throw new InvalidOperationException("Already connected");

        LastError = null;

        try
        {
            // accept hostname, IPV4 or IPV6 address

            if (!IPAddress.TryParse(host, out var address))
                address = Dns.GetHostEntry(host).AddressList.FirstOrDefault(x=>x.AddressFamily == AddressFamily.InterNetwork);

            if (address == null)
                throw new CacheException($"Unable to resolve host:{address}");


            _client = new TcpClient();


            _client.Connect(address, port);


            _client.NoDelay = true; // Disable Nagle's algorithm for low latency

            if (!_client.Connected)
            {
                LastError = $"Could not connect to {host}:{port}";
                return false;
            }

            var networkStream = _client.GetStream();

            if (useSsl)
            {
                var sslStream = new SslStream(networkStream, false,
                    validateServerCertificate ? null : (_, _, _, _) => true);

                try
                {
                    sslStream.AuthenticateAsClient(host);
                }
                catch (Exception ex) when (ex is AuthenticationException or IOException)
                {
                    // A plain-text server responding to a TLS ClientHello (or a certificate that fails
                    // validation) both surface here. Either way, tell the caller exactly what to check.
                    LastError =
                        $"SSL handshake with {host}:{port} failed: {ex.Message}. This client has useSsl=true - " +
                        "check that the server has ServerSettings:UseSsl enabled with a valid certificate.";
                    sslStream.Dispose();
                    _client.Close();
                    return false;
                }

                _stream = sslStream;
            }
            else
            {
                _stream = networkStream;
            }

            return true;
        }
        catch (SocketException ex)
        {
            LastError = $"Could not connect to {host}:{port}: {ex.Message}";
            return false; // Connection failed, return false
        }
    }

    public async Task CreateCollection(string collectionName, string primaryKey, params string[] otherIndexes)
    {
        var msg = new CreateCollectionRequest
        {
            CollectionName = collectionName,
            PrimaryKeyName = primaryKey,
            OtherIndexes = otherIndexes
        };

        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");


        await _stream.WriteMessageAsync(msg, CancellationToken.None);

        var response = await _stream.ReadMessageAsync(CancellationToken.None);
        if (response is StatusResponse status)
        {
            if (!status.Success) throw new CacheException($"Failed to create collection: {status.ErrorMessage}");
        }
        else
        {
            throw UnexpectedResponse(response);
        }
    }


    public async Task<CollectionsDescription> GetCollectionsDescription()
    {
        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");
        var request = new GetCollectionsDescriptionRequest();
        
        await _stream.WriteMessageAsync(request, CancellationToken.None);

        var response = await _stream.ReadMessageAsync(CancellationToken.None);

        if (response is CollectionsDescription description)
            return description;

        throw UnexpectedResponse(response);
    }


    public async Task FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");

        FeedItem[] batch = [];
        try
        {
            var feedRequest = new BeginFeedRequest(collectionName, newVersion);

            await _stream.WriteMessageAsync(feedRequest, CancellationToken.None);

            // the server will validate the request before sending data
            var result = await _stream.ReadMessageAsync(CancellationToken.None);
            if (result is StatusResponse { Success: false } statusResponse) throw new CacheException(statusResponse.ErrorMessage);
            if (result is not StatusResponse)
                // catch a mismatch (e.g. SSL vs plain-text) here, before uploading the whole batch
                throw UnexpectedResponse(result);


            var writer = new BinaryWriter(_stream, Encoding.UTF8, true);

            const int maxBatchSize = 1_000_000; // 1 MB per batch
            const int maxMessagesPerBatch = 5_000; // 5_000 items per batch

            // Prepare the batch of items to feed
            batch = ArrayPool<FeedItem>.Shared.Rent(maxMessagesPerBatch);

            int batchSize = 0;
            foreach (var item in items)
            {
                var feedItem = new FeedItem
                {
                    Data = item.Data,
                    Keys = item.Keys
                };

                batch[batchSize++] = feedItem;
                if (batchSize >= maxMessagesPerBatch)
                {
                    _batchSerializer.Serialize(writer, batch.AsSpan(0, batchSize), maxBatchSize);
                    batchSize = 0;
                }

            }

            // Write any remaining items in the batch
            _batchSerializer.Serialize(writer, batch.AsSpan(0, batchSize));

            if (batchSize != 0) // if the last one was not empty, we need to write an empty batch as end marker
            {
                // write an empty batch to mark the end of stream
                _batchSerializer.Serialize(writer, Array.Empty<FeedItem>());
            }
        }
        
        finally
        {
            if(batch.Length > 0)
                ArrayPool<FeedItem>.Shared.Return(batch);
        }
        

        var response = await _stream.ReadMessageAsync(CancellationToken.None);

        if (response is StatusResponse status)
        {
            if (!status.Success) throw new CacheException($"Failed to feed collection: {status.ErrorMessage}");
        }
        else
        {
            throw UnexpectedResponse(response);
        }
    }

    public async Task DropCollection(string collectionName, bool ignoreIfNotFound = true)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");
        var request = new DropCollectionRequest
        {
            CollectionName = collectionName
        };
        await _stream.WriteMessageAsync(request, CancellationToken.None);
        var response = await _stream.ReadMessageAsync(CancellationToken.None);

        if (response is StatusResponse status)
        {
            if (!status.Success && !ignoreIfNotFound)
                throw new CacheException($"Failed to drop collection: {status.ErrorMessage}");
        }
        else
        {
            throw UnexpectedResponse(response);
        }
    }

    public async Task<bool> Ping()
    {
        try
        {
            var oldTimeout = _client!.ReceiveTimeout;

            // do not wait too long for the ping answer
            _client.ReceiveTimeout = 100;


            if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");
            var request = new PingMessage();
        
            await _stream.WriteMessageAsync(request, CancellationToken.None);
            var response = await _stream.ReadMessageAsync(CancellationToken.None);

            _client.ReceiveTimeout = oldTimeout;

            if (response is PingMessage)
                return true;

            return false;
        }
        catch (Exception)
        {
            return false;
        }

    }


    /// <summary>
    ///     Query a collection by primary key. Multiple values of primary key may be specified
    /// </summary>
    /// <param name="collection"></param>
    /// <param name="keyValues"></param>
    /// <returns>Results as row data</returns>
    public async Task<List<byte[]>> QueryByPrimaryKey(string collection, params long[] keyValues)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");

        if (keyValues.Length == 0)
            throw new ArgumentException("Value cannot be an empty collection.", nameof(keyValues));

        var query = new QueryByPrimaryKey(collection, keyValues);
        await _stream.WriteMessageAsync(query, CancellationToken.None);

        List<byte[]> results = [];

        var stop = false;

        while (!stop)
        {
            var response = await _stream.ReadMessageAsync(CancellationToken.None);

            if (response is ResultWithData queryResult)
            {
                // no data in the end marker
                if (queryResult.IsEndMarker)
                    break;

                foreach (var t in queryResult.ObjectsData) results.Add(t);

                if (queryResult.SingleAnswer)
                    stop = true; // Single answer means we stop here
            }
            else if (response is StatusResponse status)
            {
                if (!status.Success) throw new CacheException($"Query failed: {status.ErrorMessage}");
                stop = true; // End of query
            }
            else
            {
                throw UnexpectedResponse(response);
            }
        }

        return results;
    }

    /// <summary>
    ///     Streams every document currently in a collection's active version. Uses the same manual big-batch
    ///     framing as a feed session, in reverse: the server acknowledges the request, then streams batches
    ///     terminated by an empty one.
    /// </summary>
    /// <param name="collection">name of an existing, already fed collection</param>
    public async IAsyncEnumerable<Item> StreamAllData(string collection)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");

        var request = new StreamAllDataRequest { CollectionName = collection };
        await _stream.WriteMessageAsync(request, CancellationToken.None);

        var ack = await _stream.ReadMessageAsync(CancellationToken.None);
        if (ack is StatusResponse { Success: false } status)
            throw new CacheException($"Failed to stream collection: {status.ErrorMessage}");
        if (ack is not StatusResponse)
            throw UnexpectedResponse(ack);

        var reader = new BinaryReader(_stream, Encoding.UTF8, true);

        while (true)
        {
            var batch = _batchSerializer.Deserialize(reader);

            if (batch.Count == 0)
                yield break; // end of stream

            foreach (var feedItem in batch)
                yield return new Item(feedItem.Data, feedItem.Keys);
        }
    }

    /// <summary>
    /// Builds a clear diagnostic for a response that wasn't of the expected type. A null response most often
    /// means the server closed the connection right after receiving the request - the most common cause in
    /// this codebase being an SSL/plain-text mismatch between this client and the server.
    /// </summary>
    private static CacheException UnexpectedResponse(IMessage? response)
    {
        if (response == null)
            return new CacheException(
                "No response received from the server (the connection was closed). This often means an " +
                "SSL mismatch between client and server - check that this connector's useSsl setting matches " +
                "ServerSettings:UseSsl on the server.");

        return new CacheException($"Unexpected response type: {response.GetType().Name}");
    }

    private bool _disposed;
    public void Dispose()
    { 
        if (_disposed) return;
        _disposed = true;
        _stream?.Dispose();
        _client?.Dispose();
    }


}