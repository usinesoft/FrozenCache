using MessagePack;

namespace Messages;

/// <summary>
/// Query a collection by its primary key. Multiple primary key values can be specified.
/// </summary>
[MessagePackObject]
public class QueryByPrimaryKey:IMessage
{
    /// <summary>
    /// Mostly for serialization purposes.
    /// </summary>
    public QueryByPrimaryKey()
    {
    }

    public QueryByPrimaryKey(string collectionName, params long[] primaryKeyValues)
    {
        CollectionName = collectionName;
        PrimaryKeyValues = primaryKeyValues.ToArray();
    }

    [Key(0)]
    public long[] PrimaryKeyValues { get; set; } = [];

    [Key(1)]
    public string? CollectionName { get; set; }

    public override string ToString()
    {
        return $"{nameof(PrimaryKeyValues)}: {PrimaryKeyValues}";
    }

    [IgnoreMember]
    public MessageType Type => MessageType.QueryByPrimaryKeyRequest;
}