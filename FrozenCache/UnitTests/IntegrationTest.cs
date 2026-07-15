using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using CacheClient;
using FrozenCache;
using MessagePack;
using Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;
using UnitTests.TestData;

namespace UnitTests;

// initialized by setup
#pragma warning disable CS8618

    /// <summary>
    /// Check integration between <see cref="Connector"/> <see cref="HostedTcpServer"/> and <see cref="DataStore"/>
    /// </summary>
    public class IntegrationTest
{
    const string StoreName = "teststore";

    private HostedTcpServer? _server;

    private Mock<ILogger<HostedTcpServer>> _logger;


    private DataStore _store;



    [TearDown]
    public void Clean()
    {
        _server?.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
        _store.Dispose();

        DataStore.Drop(StoreName);
    }

    [SetUp]
    public void Setup()
    {
        DataStore.Drop(StoreName);

        _store = new DataStore(StoreName, IndexType.Dictionary);

        _store.Open();

        _logger = new Mock<ILogger<HostedTcpServer>>();

        // dynamic port for integration tests
        var configuration = new Mock<IOptions<ServerSettings>>();
        configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });

        _server = new HostedTcpServer(_store, _logger.Object, configuration.Object);

        _server.StartAsync(CancellationToken.None);
    }

    [Test]
    public async Task FeedCollection()
    {
        var connector = new Connector("localhost", _server!.Port);

        connector.Connect();

        await connector.CreateCollection("testCollection", "id", "name", "age");

        var items = new[]
            {
                new Item(new byte[100], 1, 10, 100),
                new Item(new byte[200], 2, 20, 200),
                new Item(new byte[300], 3, 30, 300)
            }
            ;

        var itemsv2 = new[]
            {
                new Item(new byte[100], 1, 10, 100),
                new Item(new byte[200], 2, 20, 200),
                new Item(new byte[300], 3, 30, 300),
                new Item(new byte[400], 4, 40, 400)
            }
            ;

        await connector.FeedCollection("testCollection", "v1", items);

        var result = await connector.QueryByPrimaryKey("testCollection", 2);
        Assert.That(result, Is.Not.Null, "Result should not be null");
        Assert.That(result.Count, Is.EqualTo(1), "One object should be returned");
        Assert.That(result[0].Length, Is.EqualTo(200), "200 bytes of data were expected");

        // feed a new version of the collection with an additional item
        await connector.FeedCollection("testCollection", "v2", itemsv2);
        result = await connector.QueryByPrimaryKey("testCollection", 4);
        Assert.That(result, Is.Not.Null, "Result should not be null");
        Assert.That(result.Count, Is.EqualTo(1), "One object should be returned");
        Assert.That(result[0].Length, Is.EqualTo(400), "200 bytes of data were expected");

        // feeding the same version should throw an exception
        Assert.ThrowsAsync<CacheException>(async () =>
            await connector.FeedCollection("testCollection", "v2", itemsv2), "Feeding twice the same version should throw an exception");

        // feeding an older version should throw an exception
        Assert.ThrowsAsync<CacheException>(async () =>
            await connector.FeedCollection("testCollection", "v1", itemsv2), "Feeding an older version should throw an exception");
    }

    [Test]
    public async Task FeedAndRetrieveWithDuplicateKeys()
    {
        var connector = new Connector("localhost", _server!.Port);

        connector.Connect();

        await connector.CreateCollection("testCollection", "id", "name", "age");

        var items = new[]
            {
                new Item(new byte[100], 1, 10, 100),
                new Item(new byte[200], 2, 20, 200),
                new Item(new byte[201], 2, 21, 201),
                new Item(new byte[300], 3, 30, 300),
                new Item(new byte[301], 3, 31, 301),
                new Item(new byte[302], 3, 32, 302)
            }
            ;

        await connector.FeedCollection("testCollection", "v1", items);

        var result2 = await connector.QueryByPrimaryKey("testCollection", 2);
        Assert.That(result2.Count, Is.EqualTo(2));
        Assert.That(result2[0].Length, Is.EqualTo(200), "200 bytes of data were expected");
        Assert.That(result2[1].Length, Is.EqualTo(201), "201 bytes of data were expected");

        var result3 = await connector.QueryByPrimaryKey("testCollection", 3);
        Assert.That(result3.Count, Is.EqualTo(3));
        Assert.That(result3[0].Length, Is.EqualTo(300), "300 bytes of data were expected");
        Assert.That(result3[1].Length, Is.EqualTo(301), "301 bytes of data were expected");
        Assert.That(result3[2].Length, Is.EqualTo(302), "302 bytes of data were expected");

        var all = await connector.QueryByPrimaryKey("testCollection", 1,2,3);
        Assert.That(all.Count, Is.EqualTo(6));
    }

    [Test]
    public async Task FeedTypedCollection()
    {
        var aggregator = new Aggregator(3, ("localhost", _server!.Port));

        await aggregator.DeclareCollection("invoices", "id");
        
        aggregator.RegisterTypedCollection<Invoice>("invoices", 
            x=> MessagePackSerializer.Serialize(x),
            x => MessagePackSerializer.Deserialize<Invoice>(x),
            x => x.Id
            );

        int count = 1_000_000;

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
        for (int j = 0; j < 100; j++)
        {
            for (int i = 0; i < 10; i++)
            {
                ids[i] = rg.Next(1, 100_000) * 2;
            }

            var r = await aggregator.QueryByPrimaryKey<Invoice>("invoices", ids);

        }

        watch.Stop();
        Console.WriteLine($"retrieving 10 objects 100 times took {watch.ElapsedMilliseconds} milliseconds");
    }

    /// <summary>
    /// Reproduces the production incident: a client disconnects mid-feed. The partially-written version
    /// must be aborted and cleaned up immediately (not left behind to be picked up as valid data), and the
    /// previously fed, complete version must remain queryable throughout.
    /// </summary>
    [Test]
    public async Task ClientDisconnectingMidFeedAbortsThePartialVersion()
    {
        var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");

        // a first, fully completed version
        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var crashedVersionPath = Path.Combine(StoreName, "persons", "v002");

        // start a second feed and disconnect mid-stream, without ever sending the end-of-batch marker
        // or reading the final response, simulating a client process crashing during a feed session
        using (var client = new TcpClient())
        {
            client.Connect("localhost", _server.Port);
            var stream = client.GetStream();

            await stream.WriteMessageAsync(new BeginFeedRequest("persons", "v002"), CancellationToken.None);

            var ack = await stream.ReadMessageAsync(CancellationToken.None);
            Assert.That((ack as StatusResponse)?.Success, Is.True, "Server should acknowledge the feed request");

            var writer = new BinaryWriter(stream, Encoding.UTF8, true);
            var batchSerializer = new FeedItemBatchSerializer();

            var batch = new[] { new FeedItem { Data = new byte[10], Keys = [2] } };
            batchSerializer.Serialize(writer, batch);

            // connection is closed here (client crash) without completing the protocol
        }

        // give the server a moment to detect the disconnect and abort the feed
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline && Directory.Exists(crashedVersionPath))
            await Task.Delay(100);

        Assert.That(Directory.Exists(crashedVersionPath), Is.False,
            "The partially-fed version should have been aborted and removed after the client disconnected");

        // the previously completed version must still be queryable
        var result = await connector.QueryByPrimaryKey("persons", 1);
        Assert.That(result.Count, Is.EqualTo(1), "Data from the completed version should still be queryable");

        // feeding the same version again afterward must succeed: no leftover directory should block it
        await connector.FeedCollection("persons", "v002", new[] { new Item(new byte[20], 2) });
        var result2 = await connector.QueryByPrimaryKey("persons", 2);
        Assert.That(result2.Count, Is.EqualTo(1), "The retried feed of the same version should succeed");
    }

    /// <summary>
    /// A graceful disconnect (the client shuts down its send side, or the process exits cleanly) makes
    /// Stream.Read return 0 rather than throw. If that happens mid-way through the body of a batch that
    /// was announced with a larger declared size, the read loop must not spin forever waiting for bytes
    /// that will never arrive - it must fail like any other disconnect.
    /// </summary>
    [Test]
    public async Task ClientGracefullyDisconnectingMidBatchBodyAbortsThePartialVersion()
    {
        var connector = new Connector("localhost", _server!.Port);
        connector.Connect();

        await connector.CreateCollection("persons", "id");

        await connector.FeedCollection("persons", "v001", new[] { new Item(new byte[10], 1) });

        var crashedVersionPath = Path.Combine(StoreName, "persons", "v003");

        using (var client = new TcpClient())
        {
            client.Connect("localhost", _server.Port);
            var stream = client.GetStream();

            await stream.WriteMessageAsync(new BeginFeedRequest("persons", "v003"), CancellationToken.None);

            var ack = await stream.ReadMessageAsync(CancellationToken.None);
            Assert.That((ack as StatusResponse)?.Success, Is.True, "Server should acknowledge the feed request");

            var writer = new BinaryWriter(stream, Encoding.UTF8, true);

            // announce a batch bigger than what will actually be sent, then stop mid-body
            writer.Write(1000); // declared body size in bytes
            writer.Write(1); // declared item count
            writer.Write(new byte[100]); // only part of the announced 1000 bytes
            writer.Flush();

            // half-close the send side: the server sees a graceful end-of-stream (Read returns 0)
            // instead of an exception, in the middle of reading the declared batch body
            client.Client.Shutdown(SocketShutdown.Send);
        }

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline && Directory.Exists(crashedVersionPath))
            await Task.Delay(100);

        Assert.That(Directory.Exists(crashedVersionPath), Is.False,
            "The partially-fed version should have been aborted after the client disconnected mid-batch-body");

        var result = await connector.QueryByPrimaryKey("persons", 1);
        Assert.That(result.Count, Is.EqualTo(1), "Data from the completed version should still be queryable");
    }

}

