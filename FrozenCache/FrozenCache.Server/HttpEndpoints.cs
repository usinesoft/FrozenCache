using Microsoft.Extensions.Configuration;

namespace FrozenCache.Server;

/// <summary>
/// Reads the HTTP endpoints Kestrel will listen on from configuration, to detect a clash with
/// <see cref="ServerSettings.Port"/> before anything starts. Both can bind the same port without error (the TCP
/// listener on all interfaces, Kestrel on loopback), after which local clients silently reach the HTTP server
/// instead of the cache.
/// </summary>
public static class HttpEndpoints
{
    /// <summary>
    /// Kestrel's default when no endpoint is configured at all.
    /// </summary>
    private const string DefaultUrl = "http://localhost:5000";

    /// <summary>
    /// The configured HTTP endpoint URLs, following Kestrel's precedence: <c>Kestrel:Endpoints</c> replaces
    /// <c>urls</c> (<c>--urls</c>, <c>ASPNETCORE_URLS</c>), which replaces <c>http_ports</c>/<c>https_ports</c>
    /// (<c>ASPNETCORE_HTTP_PORTS</c>/<c>ASPNETCORE_HTTPS_PORTS</c>), which replace the default.
    /// </summary>
    public static IReadOnlyList<string> GetUrls(IConfiguration configuration)
    {
        var endpointUrls = configuration.GetSection("Kestrel:Endpoints").GetChildren()
            .Select(endpoint => endpoint["Url"])
            .OfType<string>()
            .ToList();

        if (endpointUrls.Count > 0)
            return endpointUrls;

        var urls = Split(configuration["urls"]);
        if (urls.Count > 0)
            return urls;

        var portUrls = Split(configuration["http_ports"]).Select(port => $"http://*:{port}")
            .Concat(Split(configuration["https_ports"]).Select(port => $"https://*:{port}"))
            .ToList();

        return portUrls.Count > 0 ? portUrls : [DefaultUrl];
    }

    /// <summary>
    /// The configured HTTP endpoint URLs that use <paramref name="port"/>, whatever their host.
    /// </summary>
    public static IReadOnlyList<string> UsingPort(IConfiguration configuration, int port) =>
        GetUrls(configuration).Where(url => GetPort(url) == port).ToList();

    /// <summary>
    /// The port of a Kestrel URL (scheme default if omitted), or null for one without a TCP port (Unix socket,
    /// named pipe) or one that can't be parsed.
    /// </summary>
    public static int? GetPort(string url)
    {
        if (url.Contains("unix:", StringComparison.OrdinalIgnoreCase) ||
            url.Contains("pipe:", StringComparison.OrdinalIgnoreCase))
            return null;

        // Kestrel accepts wildcard hosts that are not valid URI hosts
        var normalized = url.Replace("://*", "://localhost").Replace("://+", "://localhost");

        return Uri.TryCreate(normalized, UriKind.Absolute, out var uri) ? uri.Port : null;
    }

    private static List<string> Split(string? value) =>
        value?.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList() ?? [];
}
