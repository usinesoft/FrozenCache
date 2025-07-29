using Messages;
using PersistentStore;

namespace CacheClient
{

    public sealed class Connector(string host, int port):IDisposable
    {
        private System.Net.Sockets.TcpClient? _client;

        Stream? _stream;

        public bool Connect()
        {
            if (_client != null)
            {
                throw new InvalidOperationException("Already connected");
            }
            _client = new System.Net.Sockets.TcpClient(host, port);

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

            if (_client == null || _stream == null)
            {
                throw new InvalidOperationException("Not connected to server");
            }

            
            
            await _stream.WriteMessageAsync(msg, CancellationToken.None);

            var response = await _stream.ReadMessageAsync(CancellationToken.None);
            if(response is StatusResponse status)
            {
                if(!status.Success)
                {
                    throw new CacheException($"Failed to create collection: {status.ErrorMessage}");
                }

            }
            else
            {
                throw new CacheException($"Unexpected response type: {response.GetType().Name}");
            }
        }

        public async Task FeedCollection(string collectionName, string newVersion, IAsyncEnumerable<Item> items)
        {
            if (_client == null || _stream == null)
            {
                throw new InvalidOperationException("Not connected to server");
            }

            var feedRequest = new BeginFeedRequest(collectionName, newVersion);
            
            await _stream.WriteMessageAsync(feedRequest, CancellationToken.None);
            
            await foreach (var item in items)
            {
                var feedItem = new FeedItem
                {
                    Data = item.Data,
                    Keys = item.Keys
                };
                await _stream.WriteMessageAsync(feedItem, CancellationToken.None);
            }
            
            var endFeedRequest = new EndFeedRequest();
            await _stream.WriteMessageAsync(endFeedRequest, CancellationToken.None);
            
            var response = await _stream.ReadMessageAsync(CancellationToken.None);
            

            if (response is StatusResponse status)
            {
                if (!status.Success)
                {
                    throw new CacheException($"Failed to feed collection: {status.ErrorMessage}");
                }
            }
            else
            {
                throw new CacheException($"Unexpected response type: {response.GetType().Name}");
            }
        }

        /// <summary>
        /// Query a collection by primary key. Multiple values of primary key may be specified
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="keyValues"></param>
        /// <returns>Results as row data</returns>
        public async Task<List<byte[]>> QueryByPrimaryKey(string collection, params long[] keyValues)
        {
            if (_client == null || _stream == null)
            {
                throw new InvalidOperationException("Not connected to server");
            }

            if (keyValues.Length == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(keyValues));

            var query = new QueryByPrimaryKey(collection, keyValues);
            await _stream.WriteMessageAsync(query, CancellationToken.None);

            List<byte[]> results = [];

            bool stop = false;

            while (!stop)
            {
                var response = await _stream.ReadMessageAsync(CancellationToken.None);
                
                if(response is ResultWithData queryResult)
                {
                    // no data in the end marker
                    if (queryResult.IsEndMarker)
                        break;

                    foreach (var t in queryResult.ObjectsData)
                    {
                        results.Add(t);
                    }

                    if(queryResult.SingleAnswer)
                        stop = true; // Single answer means we stop here
                }
                else if (response is StatusResponse status)
                {
                    if (!status.Success)
                    {
                        throw new CacheException($"Query failed: {status.ErrorMessage}");
                    }
                    stop = true; // End of query
                }
                else
                {
                    throw new CacheException($"Unexpected response type: {response.GetType().Name}");
                }
            }
            
            return results;

        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
