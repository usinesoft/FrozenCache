using MessagePack;

namespace Messages;

[MessagePackObject]
public class CollectionsDescription
{
    [Key(0)]
    public Dictionary<string, CollectionInformation> CollectionInformation { get; set; } = [];
}

[MessagePackObject]
public class CollectionInformation
{
    [Key(0)]
    public int Count { get; set; }

    [Key(1)]
    public long SizeInBytes { get; set; }

    [IgnoreMember]
    public double AvgObjectSize => Count == 0 ? 0 : (double)SizeInBytes / Count;

    [Key(2)]
    public string LastVersion { get; set; } = string.Empty;

    [Key(3)]
    public string[] Keys { get; set; } = [];

    [Key(4)]
    public int SegmentFileSize { get; set; }
    
    [Key(5)]
    public int MaxObjectsPerSegment { get; set; }

}