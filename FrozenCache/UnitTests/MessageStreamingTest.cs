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


        FeedItem[] messages = new FeedItem[1000];
        for (int i = 0; i < 1000; i++)
        {
            messages[i] = new FeedItem
            {
                Data = new byte[100],
                Keys = [i, i + 1]
            };
        }

        var stream = new MemoryStream();

        var batchSerializer = new FeedItemBatchSerializer();

        var batches = batchSerializer.Serialize(new BinaryWriter(stream, Encoding.UTF8, true), messages.AsSpan(), 50_000);
        // add an empty batch to mark the end of the stream
        batchSerializer.Serialize(new BinaryWriter(stream, Encoding.UTF8, true), Array.Empty<FeedItem>().AsSpan());

        Assert.That(batches, Is.GreaterThan(1), "Must have been divided in multiple batches as the buffer size is too small");

        stream.Seek(0, SeekOrigin.Begin);

        var count = 0;

        while (true)
        {
            var items = batchSerializer.Deserialize(new BinaryReader(stream, Encoding.UTF8, true));
            count += items.Count;
            if (items.Count == 0)
                break;
        }
        
        
        Assert.That(count, Is.EqualTo(1000));
        

    }




}