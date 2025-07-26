using Messages;
using PersistentStore;

namespace CacheClient
{

    public sealed class Connector(string host, int port):IDisposable
    {
        private System.Net.Sockets.TcpClient? _client;

        Stream? _stream;

        public void Connect()
        {
            if (_client != null)
            {
                throw new InvalidOperationException("Already connected");
            }
            _client = new System.Net.Sockets.TcpClient(host, port);

            _client.NoDelay = true; // Disable Nagle's algorithm for low latency

            _stream = _client.GetStream();
            
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

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
