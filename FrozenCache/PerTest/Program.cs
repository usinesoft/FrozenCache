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
            string action = args.Length > 0 ? args[0] : "read";

            Console.WriteLine($"Action:{action}");

            var connector = new Connector("localhost", 5123);

            if (!connector.Connect())
            {
                Console.WriteLine("Failed to connect to server");
                return;
            }

            Console.WriteLine("Connected to server");

            if (action == "feed")
            {
                await FeedData(connector);
            }
            else
            {
                await ReadData(connector);
            }


        }



        private static async Task ReadData(Connector connector)
        {
            await QueryByBatch(connector, 1, 100);
            var watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 1, 100);
            watch.Stop();
            Console.WriteLine($"Reading 100 objects one by one took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, 5, 100);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 5, 100);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 5 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, 10, 100);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 10, 100);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 10 objects took {watch.ElapsedMilliseconds} ms");

            await QueryByBatch(connector, 100, 100);
            watch = Stopwatch.StartNew();
            await QueryByBatch(connector, 100, 100);
            watch.Stop();
            Console.WriteLine($"Reading 100 times 100 objects took {watch.ElapsedMilliseconds} ms");
        }

        private static async Task QueryByBatch(Connector connector, int batchSize, int iterations)
        {
            long[] ids = Enumerable.Range(0, batchSize).Select(x=> (long)x*10).ToArray();
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

        private static async Task FeedData(Connector connector)
        {
            //try
            //{
            //    await connector.CreateCollection("test", "id", "name");
            //}
            //catch (Exception )
            //{
            //    // Ignore if collection already exists
            //}

            //var watch = Stopwatch.StartNew();
            //await connector.FeedCollection("test", "v1", GetItems(1000, 100, 500).ToAsyncEnumerable());

            //watch.Stop();
            //Console.WriteLine($"Feeding 1000 items took {watch.ElapsedMilliseconds} ms");

            await connector.CreateCollection("big", "id", "name");

            var watch = Stopwatch.StartNew();
            await connector.FeedCollection("big", "v1", GetItems(2_000_000, 100, 500).ToAsyncEnumerable());

            watch.Stop();
            Console.WriteLine($"Feeding two million items items took {watch.ElapsedMilliseconds} ms");

        }
    }
}
