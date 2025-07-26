using Messages;
using NUnit.Framework;

namespace UnitTests;

public class MessageStreamingTest
{

    public static IEnumerable<IMessage> GenerateMessages()
    {
        yield return new PingMessage();
        yield return new BeginFeedRequest("persons", "v1");
        yield return new FeedItem { Keys = [1, 2], Data = [0x01, 0x02] };
        yield return new EndFeedRequest();
        yield return new CreateCollectionRequest { PrimaryKeyName = "id", CollectionName = "persons", OtherIndexes = ["name"] };
        yield return new StatusResponse { Success = true};
        yield return new StatusResponse { Success = false, ErrorMessage = "very bad"};

        
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

        Assert.That(message.ToString(), Is.EqualTo(readMessage.ToString()));
    }




}