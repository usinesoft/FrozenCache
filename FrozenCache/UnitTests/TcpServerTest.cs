using System.Diagnostics;
using CacheClient;
using FrozenCache;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using PersistentStore;


namespace UnitTests;

public class TcpServerTest
{

    public static void CheckLog<T>(Mock<ILogger<T>> logger,  LogLevel level, string caseInsensitiveTextInMessage)
    {
        logger.Verify(
            x => x.Log(
                level,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains(caseInsensitiveTextInMessage,StringComparison.CurrentCultureIgnoreCase)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }


    [Test]
    public async Task TestServerStartAndStop()
    {
        var dataStore = new Mock<IDataStore>();
        dataStore.Setup(x => x.CreateCollection(It.IsAny<CollectionMetadata>(), It.IsAny<int>()));

        var logger = new Mock<ILogger<HostedTcpServer>>();

        // dynamic port for integration tests
        var configuration = new Mock<IOptions<ServerSettings>>();
        configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });

        
        var server = new HostedTcpServer(dataStore.Object, logger.Object, configuration.Object);
        
        
        await server.StartAsync(CancellationToken.None);

        await Task.Delay(400);

        using var client = new Connector("localhost", server.Port);
        client.Connect();
        await client.CreateCollection("testCollection", "id", "name", "age");

        dataStore.Verify(x => x.CreateCollection(It.IsAny<CollectionMetadata>(), It.IsAny<int>()), Times.Once);

        // perf test
        Stopwatch stopwatch = Stopwatch.StartNew();
        for (int i = 0; i < 1000; i++)
        {
            await client.CreateCollection($"testCollection{i}", "id", "name", "age");
        }
        stopwatch.Stop();

        Console.WriteLine($"Created 1000 collections in {stopwatch.ElapsedMilliseconds} ms");


        // Stop the server
        await server.StopAsync(CancellationToken.None);

        await Task.Delay(500);

        CheckLog(logger, LogLevel.Warning, "server stopped");

        
    }

    [Test]
    public async Task FeedCollectionThroughTcp()
    {
        var dataStore = new NullDataStore();
        

        var logger = new Mock<ILogger<HostedTcpServer>>();

        // dynamic port for integration tests
        var configuration = new Mock<IOptions<ServerSettings>>();
        configuration.Setup(x => x.Value).Returns(new ServerSettings { Port = 0 });


        var server = new HostedTcpServer(dataStore, logger.Object, configuration.Object);


        await server.StartAsync(CancellationToken.None);

        await Task.Delay(400);

        using var client = new Connector("localhost", server.Port);
        client.Connect();
        await client.CreateCollection("testCollection", "id", "name", "age");

        var watch = Stopwatch.StartNew();
        await client.FeedCollection("testCollection", "v1", GetItems(10_000, 100, 1000));
        watch.Stop();
        Console.WriteLine($"Fed 10_000 items in {watch.ElapsedMilliseconds} ms");

        
        var result = await client.QueryByPrimaryKey("test", 12);
        Assert.That(result, Is.Not.Null, "Result should not be null");
        Assert.That(result.Count, Is.EqualTo(1), "One object should be returned");
        Assert.That(result[0].Length, Is.EqualTo(121), "121 bytes of data were expected");
        
    }


    public static IEnumerable<Item> GetItems(int count, int smallObjectSize, int largeObjectSize)
    {
        for (int i = 0; i < count; i++)
        {
            yield return i % 2 == 0
                ? new Item(new byte[smallObjectSize], i, i + 1)
                : new Item(new byte[largeObjectSize], i, i + 1);
        }
    }

}