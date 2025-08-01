﻿using System.Buffers;
using System.Net.Sockets;
using System.Text;
using Messages;
using PersistentStore;

namespace CacheClient;

public sealed class Connector(string host, int port) : IDisposable
{
    private TcpClient? _client;

    private Stream? _stream;

    private readonly FeedItemBatchSerializer _batchSerializer = new();

    public bool Connect()
    {
        if (_client != null) throw new InvalidOperationException("Already connected");
        
        _client = new TcpClient(host, port);

        _client.NoDelay = true; // Disable Nagle's algorithm for low latency

        _stream = _client.GetStream();

        if (_client.Connected)
            return true;

        return false;
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
            var responseType = response == null ? "null" : response.GetType().Name;
            throw new CacheException($"Unexpected response type: {responseType}");
        }
    }

    public async Task FeedCollection(string collectionName, string newVersion, IEnumerable<Item> items)
    {
        if (_client == null || _stream == null) throw new InvalidOperationException("Not connected to server");

        var feedRequest = new BeginFeedRequest(collectionName, newVersion);

        await _stream.WriteMessageAsync(feedRequest, CancellationToken.None);

        var writer = new BinaryWriter(_stream, Encoding.UTF8, true);

        // Prepare the batch of items to feed
        FeedItem[] batch = ArrayPool<FeedItem>.Shared.Rent(1000);

        int batchSize = 0;
        foreach (var item in items)
        {
            var feedItem = new FeedItem
            {
                Data = item.Data,
                Keys = item.Keys
            };

            batch[batchSize++] = feedItem;
            if (batchSize >= 1000)
            {
                _batchSerializer.Serialize(writer, batch.AsSpan(0, batchSize), 1_000_000);
                batchSize = 0;
            }

        }

        // Write any remaining items in the batch
        _batchSerializer.Serialize(writer, batch.AsSpan(0, batchSize));

        if (batchSize != 0)// if the last one was not empty, we need to write an empty batch as end marker
        {
            // write an empty batch to mark the end of stream
            _batchSerializer.Serialize(writer, Array.Empty<FeedItem>());
        }
        

        var response = await _stream.ReadMessageAsync(CancellationToken.None);

        ArrayPool<FeedItem>.Shared.Return(batch);

        if (response is StatusResponse status)
        {
            if (!status.Success) throw new CacheException($"Failed to feed collection: {status.ErrorMessage}");
        }
        else
        {
            var responseType = response == null ? "null" : response.GetType().Name;
            throw new CacheException($"Unexpected response type: {responseType}");
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
            throw new CacheException($"Unexpected response type: {response?.GetType().Name}");
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
                var responseType = response == null ? "null" : response.GetType().Name;
                throw new CacheException($"Unexpected response type: {responseType}");
            }
        }

        return results;
    }

    public void Dispose()
    {
        _client?.Dispose();
    }
}