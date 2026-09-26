using System.Diagnostics;
using CacheClient;
using Messages;

#pragma warning disable S112

namespace PerfTest
{
    internal class Program
    {
        private const string DefaultServer = "localhost:5123";
        private const string DefaultCollection = "big";
        private const int DefaultSize = 2_000_000;

        private static readonly string Usage = $"""
            Usage: PerfTest [feed|read|stream] [options]

              feed     create the collection (if needed) and feed a new version named after the current time (yyyyMMdd-HHmmss)
              read     query random primary keys in batches of 1, 5, 10 and 100 (default action)
              stream   stream the whole collection

            Options:
              --server <host:port>   server to connect to (default {DefaultServer})
              --collection <name>    collection name (default {DefaultCollection})
              --size <count>         number of items to feed; feed only (default {DefaultSize})
              --ssl                  connect over TLS (server certificate is not validated)
            """;

        static async Task<int> Main(string[] args)
        {
            string action = "read";
            string server = DefaultServer;
            string collection = DefaultCollection;
            int size = DefaultSize;
            bool useSsl = false;

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];

                if (!arg.StartsWith("--"))
                {
                    action = arg.ToLowerInvariant();
                    continue;
                }

                switch (arg.ToLowerInvariant())
                {
                    case "--ssl":
                        useSsl = true;
                        break;
                    case "--server" when i + 1 < args.Length:
                        server = args[++i];
                        break;
                    case "--collection" when i + 1 < args.Length:
                        collection = args[++i];
                        break;
                    case "--size" when i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedSize) && parsedSize > 0:
                        size = parsedSize;
                        i++;
                        break;
                    default:
                        Console.WriteLine($"Invalid or incomplete option: {arg}");
                        Console.WriteLine(Usage);
                        return 1;
                }
            }

            if (action is not ("feed" or "read" or "stream"))
            {
                Console.WriteLine($"Unknown action: {action}");
                Console.WriteLine(Usage);
                return 1;
            }

            if (!TryParseServer(server, out var host, out var port))
            {
                Console.WriteLine($"Invalid server '{server}', expected host:port");
                return 1;
            }

            Console.WriteLine($"Action:{action}, Server:{host}:{port}, Collection:{collection}, Ssl:{useSsl}");

            // certificate validation is skipped: this tool is meant to run against a local/dev server,
            // typically using a self-signed certificate
            var connector = new Connector(host, port, useSsl: useSsl, validateServerCertificate: false);

            if (!connector.Connect())
            {
                Console.WriteLine($"Failed to connect to server: {connector.LastError}");
                return 1;
            }

            Console.WriteLine("Connected to server");

            if (action == "feed")
            {
                await FeedData(connector, collection, size);
            }
            else if (action == "stream")
            {
                await StreamData(connector, collection);
            }
            else
            {
                await ReadData(connector, collection);
            }

            return 0;
        }

        // accepts "host:port", or "host" alone (default port); the last ':' separates the port so that
        // bracketed IPv6 literals like [::1]:5123 also work
        private static bool TryParseServer(string server, out string host, out int port)
        {
            port = 5123;
            host = server;

            int separator = server.LastIndexOf(':');
            if (separator > 0 && server.IndexOf(']', separator) < 0)
            {
                host = server[..separator];
                if (!int.TryParse(server[(separator + 1)..], out port) || port is <= 0 or > 65535)
                    return false;
            }

            host = host.Trim('[', ']');
            return host.Length > 0;
        }

        private static async Task ReadData(Connector connector, string collection)
        {
            // items are fed with primary keys 0..count-1, so the collection size gives the key range to query
            var description = await connector.GetCollectionsDescription();
            var info = description.Collections.FirstOrDefault(c => c.Name == collection);
            if (info == null || info.Count == 0)
            {
                Console.WriteLine($"Collection '{collection}' not found or empty");
                return;
            }

            int maxId = info.Count;
            Console.WriteLine($"Collection '{collection}' version {info.LastVersion} contains {maxId} items");

            await QueryByBatch(connector, collection, 1, 100, maxId);
            var watch = Stopwatch.StartNew();
            await QueryByBatch(connector, collection, 1, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 objects one by one took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, collection, 5, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, collection, 5, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 5 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, collection, 10, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, collection, 10, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 10 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, collection, 100, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, collection, 100, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 100 objects took {watch.ElapsedMilliseconds} ms");
        }

        private static async Task StreamData(Connector connector, string collection)
        {
            var watch = Stopwatch.StartNew();

            long count = 0;

            await foreach (var _ in connector.StreamAllData(collection))
            {
                count++;

                if (count % 100_000 == 0)
                    Console.Write('.');
            }

            watch.Stop();

            Console.WriteLine();
            Console.WriteLine($"Streamed {count} items in {watch.ElapsedMilliseconds} ms");
        }

        private static async Task QueryByBatch(Connector connector, string collection, int batchSize, int iterations, int maxId)
        {
            Random random = new Random();
            
            long[] ids = new long[batchSize];

            for (int i = 0; i < batchSize; i++)
                ids[i] = random.NextInt64(maxId);


            var watch = Stopwatch.StartNew();
            for (int i = 0; i < iterations; i++)
            {
               var result =  await connector.QueryByPrimaryKey(collection, ids);
               if (result.Count != batchSize)
               {
                   throw new Exception($"Expected {batchSize} items, but got {result.Count} items");
               }
            }

            watch.Stop();

        }


        static IEnumerable<Item> GetItems(int count, int smallObjectSize, int largeObjectSize)
        {
            for (int i = 0; i < count; i++)
            {
                var data = new byte[i % 2 == 0 ? smallObjectSize : largeObjectSize];
                new Random().NextBytes(data);
                yield return new Item(data, i, i*10);
            }
        }

        private static async Task FeedData(Connector connector, string collection, int count)
        {

            try
            {
                await connector.CreateCollection(collection, "id", "name");

                // a time-based version is unique per run, so repeated feeds never collide with an existing version
                string version = DateTime.Now.ToString("yyyyMMdd-HHmmss");
                Console.WriteLine($"Feeding {count} items into '{collection}' as version {version}");

                var watch = Stopwatch.StartNew();
                await connector.FeedCollection(collection, version, GetItems(count, 100, 500));

                watch.Stop();
                Console.WriteLine($"Feeding {count} items items took {watch.ElapsedMilliseconds} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                
            }

        }
    }
}
