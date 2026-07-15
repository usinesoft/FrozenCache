using System.Diagnostics;
using CacheClient;
using PersistentStore;
#pragma warning disable S112

namespace PerTest
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            // --ssl is a flag, not positional, so it can be added anywhere without disturbing the
            // existing action/count/version positional arguments
            bool useSsl = args.Any(a => a.Equals("--ssl", StringComparison.OrdinalIgnoreCase));
            string[] positional = args.Where(a => !a.StartsWith("--")).ToArray();

            string action = positional.Length > 0 ? positional[0] : "read";

            int count = positional.Length > 1 ? int.Parse(positional[1]) : 2_000_000;

            string? version = positional.Length > 2 ? positional[2] : null;

            Console.WriteLine($"Action:{action}, Ssl:{useSsl}");

            // certificate validation is skipped: this tool is meant to run against a local/dev server,
            // typically using a self-signed certificate
            var connector = new Connector("localhost", 5123, useSsl: useSsl, validateServerCertificate: false);

            if (!connector.Connect())
            {
                Console.WriteLine("Failed to connect to server");
                return;
            }

            Console.WriteLine("Connected to server");

            if (action == "feed")
            {
                await FeedData(connector, count, version);
            }
            else
            {
                await ReadData(connector, count);
            }


        }



        private static async Task ReadData(Connector connector, int maxId)
        {
            await QueryByBatch(connector, 1, 100, maxId);
            var watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 1, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 objects one by one took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, 5, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 5, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 5 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, 10, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 10, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 10 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, 100, 100, maxId);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 100, 100, maxId);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 100 objects took {watch.ElapsedMilliseconds} ms");
        }

        private static async Task QueryByBatch(Connector connector, int batchSize, int iterations, int maxId)
        {
            Random random = new Random();
            
            long[] ids = new long[batchSize];

            for (int i = 0; i < batchSize; i++)
                ids[i] = random.NextInt64(maxId);


            var watch = Stopwatch.StartNew();
            for (int i = 0; i < iterations; i++)
            {
               var result =  await connector.QueryByPrimaryKey("big", ids);
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

        private static async Task FeedData(Connector connector, int count, string? version)
        {

            try
            {
                if (string.IsNullOrWhiteSpace(version))
                {
                    await connector.DropCollection("big");

                    await connector.CreateCollection("big", "id", "name");
                }

                version ??= "v1";


                var watch = Stopwatch.StartNew();
                await connector.FeedCollection("big", version, GetItems(count, 100, 500));

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
