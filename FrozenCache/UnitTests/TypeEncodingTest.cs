using CacheClient;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;

namespace UnitTests;

#pragma warning disable CS8618
public class TypeEncodingTest
{
    [Test]
    public void EncodeStringAsLong()
    {
        var key1 = "today".EncodeString();
        var key2 = "Today".EncodeString();

        Assert.That(key1, Is.EqualTo(key2), "String encoding should be case-insensitive");

        var key3 = "one1".EncodeString();
        var key4 = "one2".EncodeString();
        Assert.That(key3, Is.Not.EqualTo(key4), "Different strings should produce different encodings");

        key3 = "another-one1".EncodeString();
        key4 = "another-one2".EncodeString();
        Assert.That(key3, Is.EqualTo(key4), "Only the first 10 characters are encoded");
    }

    [Test]
    public void EncodeStringAsKey()
    {
        var key1 = "today".FromString();
        var key2 = "Today".FromString();

        Assert.That(key1, Is.Not.EqualTo(key2), "String keys must be case sensitive");

        var key3 = "one1".FromString();
        var key4 = "one2".FromString();
        Assert.That(key3, Is.Not.EqualTo(key4), "Different strings should produce different keys");

        key3 = "another-one1".FromString();
        key4 = "another-one2".FromString();
        Assert.That(key3, Is.Not.EqualTo(key4), "Different strings should produce different keys");
    }

    [Test]
    public void LongKeysPreserveOrder()
    {
        var a = 10;
        var b = 20;
        var c = 20.15;

        var ka = a.FromInt();
        var kb = b.FromInt();
        var kc = c.FromDouble();

        Assert.That(ka, Is.LessThan(kb), "Integer keys should preserve order");
        Assert.That(kb, Is.LessThan(kc), "Numeric keys should preserve order");

        Assert.That("abc".FromString(), Is.LessThan("abd".FromString()), "String keys should preserve order for the first 5 chars");

        var date1 = DateTime.Now;
        
        var date2 = DateOnly.FromDateTime( date1.AddDays(1));
        
        var date3 = date1.Date;

        var keyDate1 = date1.FromDateTime();
        var keyDate2 = date2.FromDateOnly();
        var keyDate3 = date3.FromDateTime();

        Assert.That(keyDate1, Is.LessThan(keyDate2), "Date keys should preserve order");
        Assert.That(keyDate3, Is.LessThan(keyDate1), "Date keys should preserve order");



    }

}