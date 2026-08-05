using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Messages;


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

        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);


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
        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);
        var request = new GetCollectionsDescriptionRequest();
        
        await _stream.WriteMessageAsync(request, CancellationToken.None);

        var response = await _stream.ReadMessageAsync(CancellationToken.None);

        if (response is CollectionsDescription description)
            return description;

        throw UnexpectedResponse(response);
    }


    public async Task FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);

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
        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);
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


            if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);
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
        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);

        if (keyValues.Length == 0)
            throw new ArgumentException("Value cannot be an empty collection.", nameof(keyValues));

        var query = new QueryByPrimaryKey(collection, keyValues);
        await _stream.WriteMessageAsync(query, CancellationToken.None);

        List<byte[]> results = [];

        while (await ReadQueryResponseInto(_stream, results))
        {
        }

        return results;
    }

    /// <summary>
    /// Reads one query response and folds any data into <paramref name="results"/>. Returns true while more
    /// responses are still expected, false once the query is complete (an end marker, a single-answer result,
    /// or a successful status).
    /// </summary>
    private async Task<bool> ReadQueryResponseInto(Stream stream, List<byte[]> results)
    {
        var response = await stream.ReadMessageAsync(CancellationToken.None);

        switch (response)
        {
            case ResultWithData { IsEndMarker: true }:
                return false; // no data in the end marker

            case ResultWithData queryResult:
                results.AddRange(queryResult.ObjectsData);
                return !queryResult.SingleAnswer; // single answer means we stop here

            case StatusResponse { Success: false } status:
                throw new CacheException($"Query failed: {status.ErrorMessage}");

            case StatusResponse:
                return false; // end of query

            default:
                throw UnexpectedResponse(response);
        }
    }

    /// <summary>
    ///     Streams every document currently in a collection's active version, paired with its primary key.
    ///     Uses the same manual big-batch framing as a feed session, in reverse: the server acknowledges the
    ///     request, then streams batches terminated by an empty one. Any other key information the caller
    ///     needs is expected to already be present in the serialized data itself.
    /// </summary>
    /// <param name="collection">name of an existing, already fed collection</param>
    public async IAsyncEnumerable<(long PrimaryKey, byte[] Data)> StreamAllData(string collection)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);

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
                yield return (feedItem.Keys[0], feedItem.Data);
        }
    }

    /// <summary>
    ///     Streams every document in a collection's active version whose value at a named secondary index
    ///     equals <paramref name="keyValue"/>, paired with its primary key. Same framing as
    ///     <see cref="StreamAllData"/>: the server acknowledges the request, then streams batches terminated
    ///     by an empty one. Any other key information the caller needs is expected to already be present in
    ///     the serialized data itself.
    /// </summary>
    /// <param name="collection">name of an existing, already fed collection</param>
    /// <param name="indexName">name of a declared index (primary or secondary) on the collection</param>
    /// <param name="keyValue"></param>
    public async IAsyncEnumerable<(long PrimaryKey, byte[] Data)> StreamBySecondaryIndex(string collection, string indexName, long keyValue)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException(ErrorMessages.NotConnectedToServer);

        var request = new StreamBySecondaryIndexRequest
        {
            CollectionName = collection,
            IndexName = indexName,
            KeyValue = keyValue
        };
        await _stream.WriteMessageAsync(request, CancellationToken.None);

        var ack = await _stream.ReadMessageAsync(CancellationToken.None);
        if (ack is StatusResponse { Success: false } status)
            throw new CacheException($"Failed to stream by secondary index: {status.ErrorMessage}");
        if (ack is not StatusResponse)
            throw UnexpectedResponse(ack);

        var reader = new BinaryReader(_stream, Encoding.UTF8, true);

        while (true)
        {
            var batch = _batchSerializer.Deserialize(reader);

            if (batch.Count == 0)
                yield break; // end of stream

            foreach (var feedItem in batch)
                yield return (feedItem.Keys[0], feedItem.Data);
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