using Messages;
using NUnit.Framework;
using PersistentStore;
using PersistentStore.Statistics;

namespace UnitTests;

public class StatisticsTest
{
    [Test]
    public void CircularQueueReturnsTheMostRecentItemsInTheCorrectOrder()
    {
        var queue = new CircularQueue<string>(3);
        queue.Enqueue("A");
        queue.Enqueue("B");

        Assert.That(queue.Last, Is.EqualTo("B"));

        queue.Enqueue("C");
        Assert.That(queue.GetContent(), Is.EqualTo(["A", "B", "C"]));
        queue.Enqueue("D");
        Assert.That(queue.GetContent(), Is.EqualTo(["B", "C", "D"]));
        queue.Enqueue("E");
        Assert.That(queue.GetContent(), Is.EqualTo(["C", "D", "E"]));
        queue.Enqueue("F");
        Assert.That(queue.GetContent(), Is.EqualTo(["D", "E", "F"]));

        Assert.That(queue.Last, Is.EqualTo("F"));
    }

    /// <summary>
    /// Wall clock and timestamps both driven by <see cref="Now"/>, one timestamp unit per 100 ns tick.
    /// </summary>
    private sealed class ManualTimeProvider(DateTime start) : TimeProvider
    {
        public DateTime Now { get; set; } = start;

        public override DateTimeOffset GetUtcNow() => new(Now, TimeSpan.Zero);

        public override long GetTimestamp() => Now.Ticks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    }

    private static readonly DateTime Start = new(2026, 10, 11, 10, 15, 0, DateTimeKind.Utc);

    [Test]
    public void LookupsInTheSameMinuteAreAggregatedInOneEntry()
    {
        var time = new ManualTimeProvider(Start.AddSeconds(5));
        var monitor = new CollectionMonitor(time);

        monitor.Record(1, TimeSpan.FromMicroseconds(10));
        time.Now = Start.AddSeconds(30).AddTicks(1_234_567);
        monitor.Record(2, TimeSpan.FromMicroseconds(20));
        time.Now = Start.AddMinutes(1).AddTicks(-1);
        monitor.Record(0, TimeSpan.FromMicroseconds(30));

        var history = monitor.GetHistory();

        Assert.That(history, Has.Length.EqualTo(1));
        Assert.That(history[0].Timestamp, Is.EqualTo(Start));
        Assert.That(history[0].Calls, Is.EqualTo(3));
        Assert.That(history[0].RetrievedObjects, Is.EqualTo(3));
        Assert.That(history[0].TotalTicks, Is.EqualTo(TimeSpan.FromMicroseconds(60).Ticks));
    }

    [Test]
    public void EachMinuteWithLookupsGetsItsOwnEntryAndIdleMinutesAreSkipped()
    {
        var time = new ManualTimeProvider(Start);
        var monitor = new CollectionMonitor(time);

        monitor.Record(1, TimeSpan.FromMicroseconds(10));
        time.Now = Start.AddMinutes(1).AddSeconds(10);
        monitor.Record(1, TimeSpan.FromMicroseconds(10));
        monitor.Record(1, TimeSpan.FromMicroseconds(10));
        time.Now = Start.AddMinutes(5);
        monitor.Record(1, TimeSpan.FromMicroseconds(10));

        var history = monitor.GetHistory();

        Assert.That(history.Select(h => h.Timestamp),
            Is.EqualTo(new[] { Start, Start.AddMinutes(1), Start.AddMinutes(5) }));
        Assert.That(history.Select(h => h.Calls), Is.EqualTo(new long[] { 1, 2, 1 }));
    }

    [Test]
    public void HistoryOnlyCoversTheLast24Hours()
    {
        var time = new ManualTimeProvider(Start);
        var monitor = new CollectionMonitor(time);

        monitor.Record(1, TimeSpan.FromMicroseconds(10));
        time.Now = Start.AddHours(23);
        monitor.Record(1, TimeSpan.FromMicroseconds(10));

        Assert.That(monitor.GetHistory(), Has.Length.EqualTo(2));

        time.Now = Start.AddHours(24).AddMinutes(1);

        var history = monitor.GetHistory();
        Assert.That(history, Has.Length.EqualTo(1));
        Assert.That(history[0].Timestamp, Is.EqualTo(Start.AddHours(23)));
    }

    [Test]
    public void ConcurrentLookupsAreAllCounted()
    {
        var monitor = new CollectionMonitor();

        Parallel.For(0, 100_000, _ => monitor.Record(2, TimeSpan.FromTicks(1)));

        var history = monitor.GetHistory();
        Assert.That(history.Sum(h => h.Calls), Is.EqualTo(100_000));
        Assert.That(history.Sum(h => h.RetrievedObjects), Is.EqualTo(200_000));
        Assert.That(history.Sum(h => h.TotalTicks), Is.EqualTo(100_000));
    }

    [Test]
    public void DataStoreKeepsTheHistoryAcrossVersionsAndForgetsItOnDrop()
    {
        const string storeName = "teststore_monitoring";
        DataStore.Drop(storeName);

        using (var store = new DataStore(storeName, IndexType.Dictionary))
        {
            store.Open();
            store.CreateCollection(new CollectionMetadata("persons", "id"));

            Assert.That(store.GetMonitoring("persons"), Is.Empty);
            Assert.That(store.GetMonitoring("unknown"), Is.Null);

            store.FeedCollection("persons", "v001", [new Item(new byte[10], 1)]);
            store.GetByPrimaryKey("persons", 1);
            store.GetByPrimaryKey("persons", 2);

            store.FeedCollection("persons", "v002", [new Item(new byte[10], 1)]);
            store.GetByPrimaryKey("persons", 1);

            var history = store.GetMonitoring("persons")!;
            Assert.That(history.Sum(h => h.Calls), Is.EqualTo(3));
            Assert.That(history.Sum(h => h.RetrievedObjects), Is.EqualTo(2));

            store.DropCollection("persons");
            Assert.That(store.GetMonitoring("persons"), Is.Null);
        }

        DataStore.Drop(storeName);
    }
}