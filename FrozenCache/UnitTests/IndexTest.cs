using NUnit.Framework;
using PersistentStore;

namespace UnitTests;

public class IndexTest
{
    [Test]
    [TestCase(0, 0, new long[] { 1, 2 })]
    [TestCase(3, 0, new long[] { 1, 2 })]
    [TestCase(1, 1, new long[] { 1, 2 })]
    [TestCase(1, 3, new long[] { 1, 1, 1, 2, -1, 4, 4 })]
    [TestCase(4, 2, new long[] { 1, 1, 1, 2, -1, 4, 4 })]
    [TestCase(-1, 1, new long[] { 1, 1, 1, 2, -1, 4, 4 })]
    public void TestDictionaryIndex(long key, int expectedCount, long[] keys)
    {
        var index = new DictionaryIndex();
        foreach (var k in keys)
        {
            index.Add(k, new IndexEntry());
        }

        index.PostProcess();

        var entriesForKey1 = index.Get(key);
        Assert.That(entriesForKey1.Count, Is.EqualTo(expectedCount), $"Key {key} should have {expectedCount} entries");

    }



    [Test]
    [TestCase(0, 0, new long[] { 1, 2 })]
    [TestCase(3, 0, new long[] { 1, 2 })]
    [TestCase(1, 1, new long[] { 1, 2 })]
    [TestCase(1, 3, new long[] { 1, 1, 1, 2, -1, 4, 4 })]
    [TestCase(4, 2, new long[] { 1, 1, 1, 2, -1, 4, 4 })]
    [TestCase(-1, 1, new long[] { 1, 1, 1, 2, -1, 4, 4 })]
    public void TestOrderedIndex(long key, int expectedCount, long[] keys)
    {
        var index = new OrderedIndex();
        foreach (var k in keys)
        {
            index.Add(k, new IndexEntry());
        }

        index.PostProcess();

        var entriesForKey1 = index.Get(key);
        Assert.That(entriesForKey1.Count, Is.EqualTo(expectedCount), $"Key {key} should have {expectedCount} entries");
        
    }
}