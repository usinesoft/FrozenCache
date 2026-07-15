namespace PersistentStore;

public static class Consts
{
    public const string BinaryFilePattern = "*.bin";

    public const int DefaultBinaryFileDataSize = 1_000_000_000;

    public const int DefaultMaxDocumentsInOneFile = 1_000_000;

    /// <summary>
    /// Empty marker directory written inside a version directory once its data and index have been fully
    /// written and flushed. A version directory without this marker is the result of a feed that never
    /// completed (e.g. the feeding client crashed or disconnected) and must not be used.
    /// </summary>
    public const string CompletedMarkerDirectoryName = ".complete";
}