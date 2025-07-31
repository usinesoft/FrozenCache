using CacheClient;
using PersistentStore;
using System.Diagnostics;
using FrozenCache;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;



namespace ProfilingTool
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var dataStore = new NullDataStore();

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("LoggingConsoleApp.Program", LogLevel.Debug)
                    .AddConsole();
            });
            ILogger<HostedTcpServer> logger = loggerFactory.CreateLogger<HostedTcpServer>();

            logger.LogInformation("Profiling collection feeding through TCP");

            

            // dynamic port for integration tests

            var options = Options.Create(new ServerSettings { Port = 0 });

            var server = new HostedTcpServer(dataStore, logger, options);


            await server.StartAsync(CancellationToken.None);

            await Task.Delay(400);// waiting for server start

            using var client = new Connector("localhost", server.Port);
            client.Connect();
            await client.CreateCollection("testCollection", "id", "name", "age");

            for (int i = 0; i < 10; i++)
            {
                const int count = 1_000_000;

                var watch = Stopwatch.StartNew();
                await client.FeedCollection("testCollection", "v1", GetItems(count, 100, 200));
                watch.Stop();
                Console.WriteLine($"Fed {count} items in {watch.ElapsedMilliseconds} ms");
            }
            

            await server.StopAsync(CancellationToken.None);
        }
         
        public static IEnumerable<Item> GetItems(int count, int smallObjectSize, int largeObjectSize)
        {
            var smallData = new byte[smallObjectSize];
            var largeData = new byte[largeObjectSize];

            for (int i = 0; i < count; i++)
            {
                yield return i % 2 == 0
                    ? new Item(smallData, i, i + 1)
                    : new Item(largeData, i, i + 1);
            }
        }

        
    }
}
