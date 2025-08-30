using System.Diagnostics;
using CacheClient;
using FrozenCache;
using MessagePack;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;
using UnitTests.TestData;

namespace UnitTests;

public class MultiServerIntegrationTest
{
    private const string StoreName = "teststore";

    private readonly List<HostedTcpServer> _servers = [];

    private Mock<ILogger<HostedTcpServer>> _logger;


    private readonly List<DataStore> _stores = [];

    private static readonly int ServerCount = 3;


    [TearDown]
    public void Clean()
    {
        foreach (var server in _servers)
            server.StopAsync(CancellationToken.None).GetAwaiter().GetResult();

        foreach (var store in _stores)
            store.Dispose();

        _servers.Clear();
        _stores.Clear();

        for (var i = 0; i < ServerCount; i++) DataStore.Drop($"{StoreName}{i + 1}");
    }

    [SetUp]
    public void Setup()
    {
        for (var i = 0; i < ServerCount; i++) DataStore.Drop($"{StoreName}{i + 1}");

        _logger = new Mock<ILogger<HostedTcpServer>>();

        for (var i = 0; i < ServerCount; i++)
        {
            // create stores
            var store = new DataStore($"{StoreName}{i + 1}");

            store.Open();

            _stores.Add(store);


            // start servers

            // dynamic port for integration tests
            var configuration = new Mock<IOptions<ServerSettings>>();
            configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });

            var server = new HostedTcpServer(store, _logger.Object, configuration.Object);

            server.StartAsync(CancellationToken.None);

            _servers.Add(server);
        }

        // wait for servers to start
        Task.Delay(500).Wait();
    }

    [Test]
    public async Task FeedCollectionOnMultipleServers()
    {
        var servers = _servers.Select(s => ("localhost", s.Port)).ToArray();

        var aggregator = new Aggregator(3, servers);

        await aggregator.DeclareCollection("invoices", "id");

        aggregator.RegisterTypedCollection("invoices",
            x => MessagePackSerializer.Serialize(x),
            x => MessagePackSerializer.Deserialize<Invoice>(x),
            x => x.Id
        );

        var count = 1_000_000;

        var watch = Stopwatch.StartNew();
        var invoices = Invoice.GenerateInvoices(count).ToArray();
        watch.Stop();

        Console.WriteLine($"Feeding {count} objects took {watch.ElapsedMilliseconds} milliseconds");

        await aggregator.FeedCollection("invoices", invoices);

        var result = await aggregator.QueryByPrimaryKey<Invoice>("invoices", 2, 4, 5);
        Assert.That(result, Is.Not.Null, "Result should not be null");
        Assert.That(result.Count, Is.EqualTo(2), "One object should be returned");

        var ids = new long[10];

        var rg = new Random();


        watch = Stopwatch.StartNew();
        for (var j = 0; j < 100; j++)
        {
            for (var i = 0; i < 10; i++) ids[i] = rg.Next(1, 100_000) * 2;

            _ = await aggregator.QueryByPrimaryKey<Invoice>("invoices", ids);
        }

        watch.Stop();
        Console.WriteLine($"retrieving 10 objects 100 times took {watch.ElapsedMilliseconds} milliseconds");


        watch = Stopwatch.StartNew();
        await Parallel.ForAsync(0, 100, async (_, _) =>
        {
            for (var i = 0; i < 10; i++) ids[i] = rg.Next(1, 100_000) * 2;

            _ = await aggregator.QueryByPrimaryKey<Invoice>("invoices", ids);
        });

        watch.Stop();
        Console.WriteLine($"retrieving 10 objects with 100 clients in parallel took {watch.ElapsedMilliseconds} milliseconds");
    }
}