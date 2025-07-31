using System.Text;
using Messages;
using NUnit.Framework;

namespace UnitTests;

public class MessageStreamingTest
{

    public static IEnumerable<IMessage> GenerateMessages()
    {
        yield return new PingMessage();
        yield return new BeginFeedRequest("persons", "v1");
        yield return new CreateCollectionRequest { PrimaryKeyName = "id", CollectionName = "persons", OtherIndexes = ["name"] };
        yield return new StatusResponse { Success = true};
        yield return new StatusResponse { Success = false, ErrorMessage = "very bad"};
        yield return new QueryByPrimaryKey {CollectionName = "test", PrimaryKeyValues = [1,2,3]};
        yield return new ResultWithData { CollectionName = "test", ObjectsData = [new byte[500], new byte[200]]};


    }

    [Test]
    [TestCaseSource(nameof(GenerateMessages))]
    public async Task TestMessageStreaming(IMessage message)
    {
        var stream = new MemoryStream();
        var cancellationToken = CancellationToken.None;

        await stream.WriteMessageAsync(message, cancellationToken);

        stream.Seek(0, SeekOrigin.Begin);
        var readMessage = await stream.ReadMessageAsync(cancellationToken);
        Assert.That(readMessage, Is.Not.Null);

        Assert.That(message.ToString(), Is.EqualTo(readMessage!.ToString()));
    }

    /// <summary>
    /// Feed item used a manual serialization, so we need to test it separately
    /// </summary>
    /// <returns></returns>
    [Test]
    public void TestFeedItemMessageStreaming()
    {
        var feedItem = new FeedItem
        {
            Data = new byte[100],
            Keys = [1, 2]
        };

        var stream = new MemoryStream();
        
        feedItem.Serialize(new BinaryWriter(stream, Encoding.UTF8, true));
        stream.Seek(0, SeekOrigin.Begin);
        var readItem = FeedItem.Deserialize(new BinaryReader(stream, Encoding.UTF8, true));
        Assert.That(readItem, Is.Not.Null);
        Assert.That(readItem.Data.Length, Is.EqualTo(feedItem.Data.Length));
        Assert.That(readItem.Keys.Length, Is.EqualTo(feedItem.Keys.Length));
        Assert.That(readItem.Data, Is.EqualTo(feedItem.Data));
        Assert.That(readItem.Keys, Is.EqualTo(feedItem.Keys));
        Assert.That(readItem.ToString(), Is.EqualTo(feedItem.ToString()));


    }




}