using MessagePack;

namespace Messages
{

    public enum MessageType
    {
        Ping = 1,
        BeginFeedRequest = 2,
        FeedItem = 3,
        EndFeedRequest = 4,
        CreateCollectionRequest = 5,
        StatusResponse = 6,
    }



    [Union(0, typeof(BeginFeedRequest))]
    [Union(1, typeof(FeedItem))]
    [Union(2, typeof(FeedItem))]
    public interface IMessage
    {
        public MessageType Type { get; }
    }


    /// <summary>
    /// Empty message (no need to be serializable)
    /// </summary>
    public class PingMessage : IMessage
    {
        public MessageType Type => MessageType.Ping;
        public override string ToString()
        {
            return "Ping";
        }
    }

    [MessagePackObject]
    public class BeginFeedRequest(string collectionName, string? collectionVersion) : IMessage
    {
        [Key(0)]
        public string CollectionName { get; set; } = collectionName;

        [Key(1)]
        public string? CollectionVersion { get; set; } = collectionVersion;

        [IgnoreMember]
        public virtual MessageType Type => MessageType.BeginFeedRequest;

        public override string ToString()
        {
            return
                $"{nameof(CollectionName)}: {CollectionName}, {nameof(CollectionVersion)}: {CollectionVersion}, {nameof(Type)}: {Type}";
        }
    }


    [MessagePackObject]
    public class EndFeedRequest : IMessage
    {

        [IgnoreMember]
        public MessageType Type => MessageType.EndFeedRequest;


        [Key(0)]
        public bool Success { get; set; } = true;

        public override string ToString()
        {
            return $"{nameof(Type)}: {Type}, {nameof(Success)}: {Success}";
        }
    }

    [MessagePackObject]
    public class FeedItem : IMessage
    {
        [Key(0)]
        public long[] Keys { get; set; }

        [Key(1)]
        public byte[] Data { get; set; }


        [IgnoreMember]
        public MessageType Type => MessageType.FeedItem;

        public override string ToString()
        {
            return $"{nameof(Keys)}: {Keys}, {nameof(Data)}: {Data}, {nameof(Type)}: {Type}";
        }
    }

    [MessagePackObject]
    public class CreateCollectionRequest : IMessage
    {
        [IgnoreMember]
        public MessageType Type => MessageType.CreateCollectionRequest;

        [Key(0)]
        public string CollectionName { get; set; } = string.Empty;

        [Key(1)]
        public string? PrimaryKeyName { get; set; } = null;

        [Key(2)]
        public string[] OtherIndexes { get; set; } = [];

        public override string ToString()
        {
            return
                $"{nameof(Type)}: {Type}, {nameof(CollectionName)}: {CollectionName}, {nameof(PrimaryKeyName)}: {PrimaryKeyName}, {nameof(OtherIndexes)}: {OtherIndexes}";
        }
    }

    /// <summary>
    /// Response containing only a status. No data
    /// If not successful it also contains an error message
    /// </summary>
    [MessagePackObject]
    public class StatusResponse : IMessage
    {
        [Key(0)]
        public bool Success { get; set; } = true;

        [Key(1)]
        public string? ErrorMessage { get; set; }

        [IgnoreMember]
        public MessageType Type => MessageType.StatusResponse;

        public override string ToString()
        {
            return $"{nameof(Success)}: {Success}, {nameof(ErrorMessage)}: {ErrorMessage}, {nameof(Type)}: {Type}";
        }
    }
}
