namespace PersistentStore;

/// <summary>
///     A pointer uniquely identifies an object in a low lever binary data store
/// </summary>
public class Pointer
{
    public Pointer(int fileIndex, int documentIndex)
    {
        if (fileIndex < 0) throw new ArgumentException("File index should be zero or a positive integer");

        if (documentIndex < 0) throw new ArgumentException("Document index should be zero or a positive integer");

        FileIndex = fileIndex;

        DocumentIndex = documentIndex;
    }

    public int FileIndex { get; }

    public int DocumentIndex { get; }


    private bool Equals(Pointer other)
    {
        return FileIndex == other.FileIndex && DocumentIndex == other.DocumentIndex;
    }

    public override string ToString()
    {
        return $"{FileIndex}, {DocumentIndex}";
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        return obj.GetType() == GetType() && Equals((Pointer)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = FileIndex;
            hashCode = (hashCode * 397) ^ DocumentIndex;
            return hashCode;
        }
    }
}