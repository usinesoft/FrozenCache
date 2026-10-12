using FrozenCache.Server;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace UnitTests;

public class HttpEndpointsTest
{
    private static IConfiguration Config(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();

    [Test]
    public void KestrelEndpointOnTheTcpPortIsAConflictWhateverItsHost()
    {
        var config = Config(
            ("Kestrel:Endpoints:Http:Url", "http://localhost:5123"),
            ("Kestrel:Endpoints:Other:Url", "http://*:5124"));

        Assert.That(HttpEndpoints.UsingPort(config, 5123), Is.EqualTo(new[] { "http://localhost:5123" }));
        Assert.That(HttpEndpoints.UsingPort(config, 5124), Is.EqualTo(new[] { "http://*:5124" }));
        Assert.That(HttpEndpoints.UsingPort(config, 5213), Is.Empty);
    }

    [Test]
    public void KestrelEndpointsReplaceUrls()
    {
        var config = Config(
            ("Kestrel:Endpoints:Http:Url", "http://localhost:5213"),
            ("urls", "http://localhost:5123"));

        Assert.That(HttpEndpoints.GetUrls(config), Is.EqualTo(new[] { "http://localhost:5213" }));
        Assert.That(HttpEndpoints.UsingPort(config, 5123), Is.Empty);
    }

    [Test]
    public void UrlsAndPortVariablesAreUsedWhenThereAreNoKestrelEndpoints()
    {
        Assert.That(HttpEndpoints.UsingPort(Config(("urls", "http://+:5000; https://[::]:5123")), 5123),
            Is.EqualTo(new[] { "https://[::]:5123" }));

        Assert.That(HttpEndpoints.UsingPort(Config(("http_ports", "8080;5123")), 5123),
            Is.EqualTo(new[] { "http://*:5123" }));
    }

    [Test]
    public void KestrelDefaultIsUsedWhenNothingIsConfigured()
    {
        Assert.That(HttpEndpoints.GetUrls(Config()), Is.EqualTo(new[] { "http://localhost:5000" }));
        Assert.That(HttpEndpoints.UsingPort(Config(), 5000), Has.Count.EqualTo(1));
    }

    [TestCase("http://localhost:5123", 5123)]
    [TestCase("http://*:5123", 5123)]
    [TestCase("https://+:5123", 5123)]
    [TestCase("http://[::]:5123", 5123)]
    [TestCase("http://localhost", 80)]
    [TestCase("https://localhost", 443)]
    [TestCase("http://unix:/tmp/kestrel.sock", null)]
    [TestCase("not a url", null)]
    public void PortIsExtractedFromKestrelUrls(string url, int? expected)
    {
        Assert.That(HttpEndpoints.GetPort(url), Is.EqualTo(expected));
    }
}
