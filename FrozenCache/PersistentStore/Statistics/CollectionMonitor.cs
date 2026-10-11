namespace PersistentStore.Statistics;

/// <summary>
/// Thread-safe, per-minute lookup statistics for one collection, covering the last 24 hours.
/// <para>
/// <see cref="Record"/> is on the read hot path, so it never allocates or locks: calls are accumulated with
/// <see cref="Interlocked"/> operations into the current minute's counters. Those counters are moved into the
/// <see cref="MonitoringQueue"/> under a lock only once per minute, by the first call (or snapshot) that notices
/// the minute has ended.
/// </para>
/// <para>
/// One instance is shared by all the versions of a collection (see <see cref="DataStore"/>), so the history
/// survives new feeds.
/// </para>
/// </summary>
public sealed class CollectionMonitor
{
    private static readonly TimeSpan HistoryDuration = TimeSpan.FromHours(24);

    private readonly TimeProvider _time;

    /// <summary>
    /// Monotonic milliseconds used to detect the end of the current minute. <see cref="Environment.TickCount64"/>
    /// by default: much cheaper than a high-resolution timestamp, which matters at millions of calls per second,
    /// and its 10-16 ms resolution is irrelevant at a one-minute granularity.
    /// </summary>
    private readonly Func<long> _milliseconds;

    /// <summary>
    /// Protects <see cref="_history"/>, <see cref="_currentMinuteTicks"/> and the transition from one minute to the next.
    /// </summary>
    private readonly Lock _lock = new();

    private readonly MonitoringQueue _history = new();

    /// <summary>
    /// Start of the current minute, in UTC ticks.
    /// </summary>
    private long _currentMinuteTicks;

    /// <summary>
    /// <see cref="_milliseconds"/> value at which the current minute ends.
    /// </summary>
    private long _currentMinuteEnd;

    private long _calls;
    private long _retrievedObjects;
    private long _elapsedTicks;

    /// <param name="timeProvider">Clock override for tests; the system clock if null.</param>
    public CollectionMonitor(TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;

        _milliseconds = timeProvider == null
            ? static () => Environment.TickCount64
            : () => (long)((Int128)timeProvider.GetTimestamp() * 1000 / timeProvider.TimestampFrequency);

        StartNewMinute();
    }

    /// <summary>
    /// Records one lookup. A call racing with the end of a minute may be counted in the next one.
    /// </summary>
    public void Record(int retrievedObjects, TimeSpan elapsed)
    {
        if (_milliseconds() >= Volatile.Read(ref _currentMinuteEnd))
        {
            lock (_lock)
            {
                // another thread may have done it while this one was waiting for the lock
                RollOverIfMinuteEnded();
            }
        }

        Interlocked.Increment(ref _calls);
        Interlocked.Add(ref _retrievedObjects, retrievedObjects);
        Interlocked.Add(ref _elapsedTicks, elapsed.Ticks);
    }

    /// <summary>
    /// Statistics for each minute of the last 24 hours that had at least one lookup, oldest first, including
    /// the current (partial) minute. Minutes without lookups are absent. The returned items are copies.
    /// </summary>
    public MonitoringItem[] GetHistory()
    {
        lock (_lock)
        {
            RollOverIfMinuteEnded();

            var cutoff = _time.GetUtcNow().UtcDateTime - HistoryDuration;

            var result = new List<MonitoringItem>();

            foreach (var item in _history.GetContent())
            {
                if (item != null && item.Timestamp > cutoff)
                    result.Add(new MonitoringItem(item.Timestamp, item.Calls, item.RetrievedObjects, item.TotalTicks));
            }

            var calls = Interlocked.Read(ref _calls);
            if (calls > 0)
                result.Add(new MonitoringItem(new DateTime(_currentMinuteTicks, DateTimeKind.Utc), calls,
                    Interlocked.Read(ref _retrievedObjects), Interlocked.Read(ref _elapsedTicks)));

            return result.ToArray();
        }
    }

    /// <summary>
    /// Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private void RollOverIfMinuteEnded()
    {
        if (_milliseconds() < _currentMinuteEnd)
            return;

        var calls = Interlocked.Exchange(ref _calls, 0);
        var retrievedObjects = Interlocked.Exchange(ref _retrievedObjects, 0);
        var elapsedTicks = Interlocked.Exchange(ref _elapsedTicks, 0);

        if (calls > 0)
            _history.AddMeasurement(new MonitoringItem(new DateTime(_currentMinuteTicks, DateTimeKind.Utc), calls,
                retrievedObjects, elapsedTicks));

        StartNewMinute();
    }

    private void StartNewMinute()
    {
        var nowTicks = _time.GetUtcNow().UtcTicks;
        var minuteStartTicks = nowTicks - nowTicks % TimeSpan.TicksPerMinute;
        var remaining = TimeSpan.FromTicks(minuteStartTicks + TimeSpan.TicksPerMinute - nowTicks);

        _currentMinuteTicks = minuteStartTicks;

        // rounded up, so that the minute never ends early and its last calls are not attributed to the next one
        Volatile.Write(ref _currentMinuteEnd, _milliseconds() + (long)Math.Ceiling(remaining.TotalMilliseconds));
    }
}
