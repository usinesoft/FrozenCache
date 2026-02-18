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

#pragma warning disable CS8618      

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
    [TestCase(true)]
    [TestCase(false)]
    public async Task FeedCollectionOnMultipleServers(bool useLocalCache)
    {
        var servers = _servers.Select(s => ("localhost", s.Port)).ToArray();

        var aggregator = new Aggregator(3, servers);

        await aggregator.DeclareCollection("invoices", "id");

        aggregator.RegisterTypedCollection("invoices",
            x => MessagePackSerializer.Serialize(x),
            x => MessagePackSerializer.Deserialize<Invoice>(x),
            x => x.Id
        );


        if (useLocalCache)
        {
            aggregator.ConfigureLocalCache("invoices", 1_000_000);
        }

        var count = 1_000_000;

        var watch = Stopwatch.StartNew();
        var invoices = Invoice.GenerateInvoices(count).ToArray();
        watch.Stop();

        Console.WriteLine($"Feeding {count} objects took {watch.ElapsedMilliseconds} milliseconds");

        await aggregator.FeedCollection("invoices", invoices);

        // check the collections description on each server
        var collectionsByServer = await aggregator.GetCollectionsDescription();
        Assert.That(collectionsByServer.Count, Is.EqualTo(3), "There should be 3 servers");
        string? lastVersion = null;
        foreach (var collectionInfo in collectionsByServer)
        {
            Assert.That(collectionInfo, Is.Not.Null);
            Assert.That(collectionInfo!.CollectionInformation.Count, Is.EqualTo(1), "There should be one collection");
            var info = collectionInfo.CollectionInformation.Single();
            Assert.That(info.Key, Is.EqualTo("invoices"), "Collection name should be 'invoices'");
            Assert.That(info.Value.Count, Is.EqualTo(count), $"Collection should contain {count} objects ");
            if (lastVersion != null)
                Assert.That(info.Value.LastVersion, Is.EqualTo(lastVersion), "All servers should have the same version");
            
            lastVersion ??= info.Value.LastVersion;
        }


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

        // stop one server 
        await _servers[0].StopAsync(CancellationToken.None);
        await Task.Delay(500);
        Console.WriteLine("Stopped server 1");

        // try to query again
        for (var i = 0; i < 10; i++) ids[i] = rg.Next(1, 100_000) * 2;

        collectionsByServer = await aggregator.GetCollectionsDescription();
        Assert.That(collectionsByServer[0], Is.Null, "Should only return collections information for connected servers");
        Assert.That(collectionsByServer[1], Is.Not.Null, "Should only return collections information for connected servers");
        Assert.That(collectionsByServer[2], Is.Not.Null, "Should only return collections information for connected servers");

        var invoices2 = await aggregator.QueryByPrimaryKey<Invoice>("invoices", ids);
        Assert.That(invoices2, Is.Not.Null, "Result should not be null");
        Console.WriteLine($"Queried {invoices2.Count} objects after one server was stopped");

        // stop another server
        await _servers[1].StopAsync(CancellationToken.None);
        await Task.Delay(500);
        Console.WriteLine("Stopped server 2");

        // try to query again
        for (var i = 0; i < 10; i++) ids[i] = rg.Next(1, 100_000) * 2;
        var invoices3 = await aggregator.QueryByPrimaryKey<Invoice>("invoices", ids);
        Assert.That(invoices3, Is.Not.Null, "Result should not be null");
        Console.WriteLine($"Queried {invoices3.Count} objects after two servers were stopped");

        if (useLocalCache)
        {
            var stats = aggregator.GetStatistics();
            foreach (var stat in stats)
            {
                Console.WriteLine(stat.Key);
                Console.WriteLine();
                Console.WriteLine(stat.Value);
            }
        }

    }
}