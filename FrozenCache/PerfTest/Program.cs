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

              feed       create the collection (if needed) and feed a new version named after the current time (UTC, yyyyMMdd_HHmmss)
              read       query random primary keys in batches of 1, 5, 10 and 100 (default action)
              stream     stream the whole collection (from one replica with --servers)
              secondary  query random secondary key values through the Aggregator, sequentially then {Concurrency} at a time;
                         each value matches about size/{SecondaryKeyGroups} items

            Options:
              --server <host:port>          server to connect to (default {DefaultServer})
              --servers <host:port+...>     a cluster of replicas, used through the Aggregator: feeds go to every
                                            replica, queries and streams are spread across them (instead of --server)
              --collection <name>           collection name (default {DefaultCollection})
              --size <count>                number of items to feed; feed only (default {DefaultSize})
              --queries <count>             number of queries per run; secondary only (default {DefaultQueries})
              --ssl                         connect over TLS (server certificate is not validated)
            """;

        static async Task<int> Main(string[] args)
        {
            string action = "read";
            string? server = null;
            string? servers = null;
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
                    case "--servers" when i + 1 < args.Length:
                        servers = args[++i];
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

            if (server != null && servers != null)
            {
                Console.WriteLine("Use either --server or --servers, not both");
                return 1;
            }

            if (servers != null)
            {
                if (!TryParseServers(servers, out var cluster))
                {
                    Console.WriteLine($"Invalid servers '{servers}', expected host1:port1+host2:port2+...");
                    return 1;
                }

                Console.WriteLine($"Action:{action}, Servers:{string.Join("+", cluster.Select(Address))}, " +
                                  $"Collection:{collection}, Ssl:{useSsl}");

                return await RunOnCluster(action, cluster, useSsl, collection, size, queries);
            }

            server ??= DefaultServer;

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
                    await StreamData(connector.StreamAllData(collection));
                }
                else
                {
                    var info = await GetNonEmptyCollection(connector, collection);
                    if (info == null)
                        return 0;

                    if (action == "secondary")
                        await QueryBySecondaryKey([(host, port)], useSsl, collection, info, queries);
                    else
                        await ReadData(ids => connector.QueryByPrimaryKey(collection, ids), info.Count);
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
        /// "host1:port1+host2:port2+...", each part as accepted by <see cref="TryParseServer"/>; at least one, no
        /// duplicates.
        /// </summary>
        private static bool TryParseServers(string servers, out (string Host, int Port)[] cluster)
        {
            var parsed = new List<(string Host, int Port)>();

            foreach (var part in servers.Split('+', StringSplitOptions.TrimEntries))
            {
                if (!TryParseServer(part, out var host, out var port))
                {
                    cluster = [];
                    return false;
                }
                parsed.Add((host, port));
            }

            cluster = parsed.ToArray();
            return cluster.Length > 0 &&
                   cluster.Distinct().Count() == cluster.Length;
        }

        private static string Address((string Host, int Port) server) => $"{server.Host}:{server.Port}";

        /// <summary>
        /// The same actions on a cluster of replicas, through the Aggregator (without local cache).
        /// </summary>
        private static async Task<int> RunOnCluster(string action, (string Host, int Port)[] cluster, bool useSsl,
            string collection, int size, int queries)
        {
            try
            {
                // certificate validation is skipped, as for a single server
                var aggregator = new Aggregator(Concurrency, useSsl, false, 10_000, cluster);

                if (action == "feed")
                    return await FeedCluster(aggregator, cluster, collection, size) ? 0 : 1;

                var info = await DescribeCluster(aggregator, cluster, collection);
                if (info == null)
                    return 1;

                if (action == "stream")
                    await StreamData(aggregator.StreamAllData(collection)); // from one replica, like any client
                else if (action == "secondary")
                    await QueryBySecondaryKey(cluster, useSsl, collection, info, queries);
                else
                    await ReadData(ids => aggregator.QueryRawDataByPrimaryKey(collection, ids), info.Count);

                return 0;
            }
            catch (Exception e) when (e is CacheException or InvalidOperationException)
            {
                Console.WriteLine(e.Message);
                return 1;
            }
        }

        /// <summary>
        /// Prints what every replica reports for the collection, and returns the description to test against:
        /// null (with a message) if no replica is connected, or the collection is missing or empty.
        /// </summary>
        private static async Task<CollectionInformation?> DescribeCluster(Aggregator aggregator,
            (string Host, int Port)[] cluster, string collection)
        {
            // one description per replica, in the order of the servers; null for a replica that isn't connected
            var descriptions = await aggregator.GetCollectionsDescription();

            CollectionInformation? reference = null;
            var connected = 0;
            var inSync = true;

            for (var i = 0; i < cluster.Length; i++)
            {
                var name = Address(cluster[i]);
                var description = descriptions[i];
                if (description == null)
                {
                    Console.WriteLine($"  {name}: not connected");
                    continue;
                }

                connected++;
                var info = description.Collections.FirstOrDefault(c => c.Name == collection);
                if (info == null)
                {
                    Console.WriteLine($"  {name}: no collection '{collection}'");
                    inSync = false;
                    continue;
                }

                Console.WriteLine($"  {name}: version {info.LastVersion}, {info.Count:N0} items");

                if (reference == null)
                    reference = info;
                else if (info.LastVersion != reference.LastVersion || info.Count != reference.Count)
                    inSync = false;
            }

            if (connected == 0)
            {
                Console.WriteLine("Failed to connect to any server");
                return null;
            }

            if (connected < cluster.Length)
                Console.WriteLine($"Warning: {cluster.Length - connected} of {cluster.Length} servers are not connected");

            if (!inSync)
                Console.WriteLine("Warning: the replicas don't have the same version of the collection");

            if (reference == null || reference.Count == 0)
            {
                Console.WriteLine($"Collection '{collection}' not found or empty");
                return null;
            }

            return reference;
        }

        /// <summary>
        /// Feeds every connected replica in parallel through the Aggregator, which names the version (UTC,
        /// yyyyMMdd_HHmmss). False if no replica is connected.
        /// </summary>
        private static async Task<bool> FeedCluster(Aggregator aggregator, (string Host, int Port)[] cluster,
            string collection, int count)
        {
            var connected = (await aggregator.GetCollectionsDescription()).Count(d => d != null);
            if (connected == 0)
            {
                Console.WriteLine("Failed to connect to any server");
                return false;
            }

            await aggregator.DeclareCollection(collection, "id", SecondaryIndex);

            // the Aggregator feeds typed objects: Item itself, serialized as its raw data, keyed by its own keys
            aggregator.RegisterTypedCollection<Item>(collection,
                item => item.Data,
                data => new Item(data),
                item => item.Keys[0],
                item => item.Keys[1]);

            Console.WriteLine($"Feeding {count} items into '{collection}' on {connected} of {cluster.Length} servers");

            var watch = Stopwatch.StartNew();
            await aggregator.FeedCollection(collection, GetItems(count, 100, 500));
            watch.Stop();
            Console.WriteLine($"Feeding {count} items items took {watch.ElapsedMilliseconds} ms");

            await DescribeCluster(aggregator, cluster, collection);
            return true;
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

        /// <param name="query">A query by primary keys: through a Connector, or through the Aggregator.</param>
        /// <param name="maxId">Items are fed with primary keys 0..count-1, so the collection size gives the key range.</param>
        private static async Task ReadData(Func<long[], Task<List<byte[]>>> query, int maxId)
        {
            await QueryByBatch(query, 1, 100, maxId);
            var watch = Stopwatch.StartNew();
            await QueryByBatch(query, 1, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 objects one by one took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(query, 5, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(query, 5, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 5 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(query, 10, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(query, 10, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 10 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(query, 100, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(query, 100, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 100 objects took {watch.ElapsedMilliseconds} ms");
        }

        /// <param name="servers">One server, or the replicas of a cluster.</param>
        private static async Task QueryBySecondaryKey((string Host, int Port)[] servers, bool useSsl, string collection,
            CollectionInformation info, int queries)
        {
            // items are fed with secondary key id % SecondaryKeyGroups, so these are the values that exist
            var groups = Math.Min(SecondaryKeyGroups, info.Count);
            Console.WriteLine($"Each '{SecondaryIndex}' value matches about {info.Count / groups:N0} items");

            var aggregator = new Aggregator(Concurrency, useSsl, false, 10_000, servers);

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

        /// <param name="documents">The collection's documents: through a Connector, or through the Aggregator.</param>
        private static async Task StreamData(IAsyncEnumerable<(long PrimaryKey, byte[] Data)> documents)
        {
            var watch = Stopwatch.StartNew();

            long count = 0;

            await foreach (var _ in documents)
            {
                count++;

                if (count % 100_000 == 0)
                    Console.Write('.');
            }

            watch.Stop();

            Console.WriteLine();
            Console.WriteLine($"Streamed {count} items in {watch.ElapsedMilliseconds} ms");
        }

        private static async Task QueryByBatch(Func<long[], Task<List<byte[]>>> query, int batchSize, int iterations,
            int maxId)
        {
            Random random = new Random();

            long[] ids = new long[batchSize];

            for (int i = 0; i < batchSize; i++)
                ids[i] = random.NextInt64(maxId);

            for (int i = 0; i < iterations; i++)
            {
                var result = await query(ids);
                if (result.Count != batchSize)
                {
                    throw new Exception($"Expected {batchSize} items, but got {result.Count} items");
                }
            }
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

                // a time-based version is unique per run, so repeated feeds never collide with an existing version;
                // UTC and "_", like the Aggregator: one collection can be fed both ways, and the server orders
                // versions by name
                string version = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
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
