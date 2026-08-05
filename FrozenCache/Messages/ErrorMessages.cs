namespace Messages;

/// <summary>
/// Central home for error message text repeated across multiple call sites (client, storage engine, server),
/// so wording changes happen in exactly one place instead of drifting between copies.
/// </summary>
public static class ErrorMessages
{
    public const string NotConnectedToServer = "Not connected to server";

    public const string CollectionNameIsRequired = "Collection name is required";

    public const string CollectionStoreNotSealed =
        "Cannot query a collection store before it has been sealed (call EndOfFeed first)";

    public const string FailedToDeserializeCollectionMetadata = "Failed to deserialize collection metadata";

    public static string MetadataFileNotFound(string path) => $"Metadata file not found in {path}";

    public static string CollectionDoesNotExist(string collectionName) => $"Collection {collectionName} does not exist";

    public static string CollectionHasNoDataToStream(string collectionName) => $"Collection {collectionName} has no data to stream";

    public static string CollectionHasNoIndexNamed(string collectionName, string indexName) =>
        $"Collection {collectionName} has no index named {indexName}";
}
