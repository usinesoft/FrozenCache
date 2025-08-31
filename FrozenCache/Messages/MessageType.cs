namespace Messages;

public enum MessageType
{
    Ping = 1,
    BeginFeedRequest = 2,
    FeedItem = 3,
    CreateCollectionRequest = 5,
    StatusResponse = 6,
    QueryByPrimaryKeyRequest = 7,
    QueryResponse = 8,
    DropCollectionRequest = 9,
    GetCollectionsDescriptionRequest = 10,
    CollectionsDescription = 11
}