using CacheClient;
using FrozenCache;
using Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;

namespace UnitTests;


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
        _store?.Dispose();

        DataStore.Drop(StoreName);
    }

    [SetUp]
    public void Setup()
    {
        DataStore.Drop(StoreName);

        _store = new DataStore(StoreName);

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

        var items = new Item[]
            {
                new Item(new byte[100], 1, 10, 100),
                new Item(new byte[200], 2, 20, 200),
                new Item(new byte[300], 3, 30, 300)
            }
            ;

        var itemsv2 = new Item[]
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

}

