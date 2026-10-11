using System.Diagnostics;
using System.Text;

namespace PersistentStore.Statistics
{
    [DebuggerDisplay("{ToString()}")]
    public class CircularQueue<T> where T:class
    {
        private readonly T?[] _data;

        private int _startPosition = 0;
        private int _endPosition = 0;

        public CircularQueue(int size)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(size, 2, nameof(size));

            Size = size;
            _data = new T[size];
        }

        public void Enqueue(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            if (_data[_endPosition] == null) // buffer is not full
            {
                _data[_endPosition] = item;
                _endPosition = (_endPosition + 1) % Size;
            }
            else
            {
                _data[_endPosition] = item;
                _startPosition = (_startPosition + 1) % Size;
                _endPosition = (_endPosition + 1) % Size;
                
            }

            
            
        }

        public T?[] GetContent()
        {
            var content = new T?[Size];
            var position = _startPosition;
            for (int i = 0; i < Size; i++)
            {
                content[i] = _data[position];
                position = (position + 1) % Size;
                if(position == _endPosition)
                {
                    break;
                }
            }
            return content;
        }

        public int Size { get; }

        public override string ToString()
        {
            var result = new StringBuilder();
            for (int i = 0; i < Size; i++)
            {
                result.Append(_data[(_startPosition + i) % Size]).Append(" ");
            }
            return result.ToString().Trim();
        }

        public T? Last => _data[(_endPosition - 1 + Size) % Size];
    }

    /// <summary>
    /// Lookup statistics for one minute.
    /// </summary>
    /// <param name="timestamp">Start of the minute, UTC.</param>
    /// <param name="calls">Number of lookups.</param>
    /// <param name="retrievedObjects">Total number of objects returned by those lookups.</param>
    /// <param name="totalTicks">Total time spent in those lookups, in <see cref="TimeSpan"/> ticks (100 ns).</param>
    public class MonitoringItem(DateTime timestamp, long calls, long retrievedObjects, long totalTicks)
    {
        public DateTime Timestamp { get; init; } = timestamp;
        public long Calls { get; set; } = calls;
        public long RetrievedObjects { get; set; } = retrievedObjects;
        public long TotalTicks { get; set; } = totalTicks;
    }

    /// <summary>
    /// A specialized circular queue for monitoring items, with a fixed size of 24 hours (24*60 minutes).
    /// 
    /// </summary>
    public class MonitoringQueue() : CircularQueue<MonitoringItem>(MaxSize)
    {
        public const int MaxSize = 24 * 60;

        public void AddMeasurement(MonitoringItem item)
        {
            var last = Last;

            if(last != null && last.Timestamp == item.Timestamp)
            {
                // Update the existing item
                last.Calls += item.Calls;
                last.RetrievedObjects += item.RetrievedObjects;
                last.TotalTicks += item.TotalTicks;
            }
            else
            {
                Enqueue(item);
            }
        }
    }
}
