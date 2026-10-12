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
        private const int DefaultQueries = 200;

        /// <summary>
        /// Secondary index of the collections fed by this tool. Historical name, kept so that existing collections
        /// remain compatible: it holds the item's group (id % <see cref="SecondaryKeyGroups"/>), so that each value
        /// matches many items.
        /// </summary>
        private const string SecondaryIndex = "name";
        private const int SecondaryKeyGroups = 1000;

        /// <summary>
        /// Concurrent queries in the parallel secondary-key run, and connection pool size.
        /// </summary>
        private const int Concurrency = 8;

        private static readonly string Usage = $"""
            Usage: PerfTest [feed|read|stream|secondary] [options]

              feed       create the collection (if needed) and feed a new version named after the current time (yyyyMMdd-HHmmss)
              read       query random primary keys in batches of 1, 5, 10 and 100 (default action)
              stream     stream the whole collection
              secondary  query random secondary key values through the Aggregator, sequentially then {Concurrency} at a time;
                         each value matches about size/{SecondaryKeyGroups} items

            Options:
              --server <host:port>   server to connect to (default {DefaultServer})
              --collection <name>    collection name (default {DefaultCollection})
              --size <count>         number of items to feed; feed only (default {DefaultSize})
              --queries <count>      number of queries per run; secondary only (default {DefaultQueries})
              --ssl                  connect over TLS (server certificate is not validated)
            """;

        static async Task<int> Main(string[] args)
        {
            string action = "read";
            string server = DefaultServer;
            string collection = DefaultCollection;
            int size = DefaultSize;
            int queries = DefaultQueries;
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
                    case "--queries" when i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedQueries) && parsedQueries > 0:
                        queries = parsedQueries;
                        i++;
                        break;
                    default:
                        Console.WriteLine($"Invalid or incomplete option: {arg}");
                        Console.WriteLine(Usage);
                        return 1;
                }
            }

            if (action is not ("feed" or "read" or "stream" or "secondary"))
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

            try
            {
                if (action == "feed")
                {
                    await FeedData(connector, collection, size);
                }
                else if (action == "stream")
                {
                    await StreamData(connector, collection);
                }
                else if (action == "secondary")
                {
                    await QueryBySecondaryKey(connector, host, port, useSsl, collection, queries);
                }
                else
                {
                    await ReadData(connector, collection);
                }
            }
            catch (CacheException e)
            {
                // protocol-level failures (e.g. an SSL mismatch) are expected operator errors, not crashes
                Console.WriteLine(e.Message);
                return 1;
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

        /// <summary>
        /// The collection's description, or null (with a message) if it doesn't exist or is empty.
        /// </summary>
        private static async Task<CollectionInformation?> GetNonEmptyCollection(Connector connector, string collection)
        {
            var description = await connector.GetCollectionsDescription();
            var info = description.Collections.FirstOrDefault(c => c.Name == collection);
            if (info == null || info.Count == 0)
            {
                Console.WriteLine($"Collection '{collection}' not found or empty");
                return null;
            }

            Console.WriteLine($"Collection '{collection}' version {info.LastVersion} contains {info.Count} items");
            return info;
        }

        private static async Task ReadData(Connector connector, string collection)
        {
            // items are fed with primary keys 0..count-1, so the collection size gives the key range to query
            var info = await GetNonEmptyCollection(connector, collection);
            if (info == null)
                return;

            int maxId = info.Count;

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

        private static async Task QueryBySecondaryKey(Connector connector, string host, int port, bool useSsl,
            string collection, int queries)
        {
            var info = await GetNonEmptyCollection(connector, collection);
            if (info == null)
                return;

            // items are fed with secondary key id % SecondaryKeyGroups, so these are the values that exist
            var groups = Math.Min(SecondaryKeyGroups, info.Count);
            Console.WriteLine($"Each '{SecondaryIndex}' value matches about {info.Count / groups:N0} items");

            var aggregator = new Aggregator(Concurrency, useSsl, false, 10_000, (host, port));

            await RunSecondaryKeyQueries(aggregator, collection, groups, Math.Max(1, queries / 10), 1); // warm-up

            var sequential = await RunSecondaryKeyQueries(aggregator, collection, groups, queries, 1);
            sequential.Print("sequential");

            var parallel = await RunSecondaryKeyQueries(aggregator, collection, groups, queries, Concurrency);
            parallel.Print($"{Concurrency} concurrent");

            if (sequential.Items == 0)
                Console.WriteLine($"No item matched: this collection was probably fed by an older PerfTest, " +
                                  $"which used a unique '{SecondaryIndex}' per item. Feed it again.");
        }

        private sealed record SecondaryKeyRun(int Queries, long Items, long Bytes, TimeSpan Elapsed, double[] LatenciesMs)
        {
            public void Print(string label)
            {
                Array.Sort(LatenciesMs);
                double Percentile(double p) => LatenciesMs[(int)Math.Ceiling(p * LatenciesMs.Length) - 1];

                var seconds = Elapsed.TotalSeconds;
                Console.WriteLine($"Secondary key, {label}: {Queries} queries, {Items:N0} items, {Bytes / 1e6:N1} MB " +
                                  $"in {Elapsed.TotalMilliseconds:N0} ms");
                Console.WriteLine($"  latency per query: p50 {Percentile(0.5):N2} ms, p95 {Percentile(0.95):N2} ms, " +
                                  $"max {LatenciesMs[^1]:N2} ms");
                Console.WriteLine($"  throughput: {Queries / seconds:N0} queries/s, {Items / seconds:N0} items/s, " +
                                  $"{Bytes / 1e6 / seconds:N1} MB/s");
            }
        }

        private static async Task<SecondaryKeyRun> RunSecondaryKeyQueries(Aggregator aggregator, string collection,
            int groups, int queries, int parallelism)
        {
            long items = 0, bytes = 0;
            var latencies = new double[queries];

            var watch = Stopwatch.StartNew();

            await Parallel.ForAsync(0, queries, new ParallelOptions { MaxDegreeOfParallelism = parallelism },
                async (query, cancellationToken) =>
                {
                    var start = Stopwatch.GetTimestamp();
                    long queryItems = 0, queryBytes = 0;

                    await foreach (var (_, data) in aggregator.StreamRawDataBySecondaryKey(collection, SecondaryIndex,
                                       Random.Shared.Next(groups), cancellationToken))
                    {
                        queryItems++;
                        queryBytes += data.Length;
                    }

                    latencies[query] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    Interlocked.Add(ref items, queryItems);
                    Interlocked.Add(ref bytes, queryBytes);
                });

            return new SecondaryKeyRun(queries, items, bytes, watch.Elapsed, latencies);
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
                yield return new Item(data, i, i % SecondaryKeyGroups);
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
