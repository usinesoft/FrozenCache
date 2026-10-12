using System.Text.Json.Serialization;
using FrozenCache.Server;
using Messages;
using Microsoft.Extensions.Options;
using PersistentStore;
using Serilog;

namespace FrozenCache;

internal static class Program
{
    public static void Main(string[] args)
    {

        
        // when run in Windows service mode, by default the working directory is C:\Windows\System32
        var currentDirectory = AppContext.BaseDirectory;
        Directory.SetCurrentDirectory(currentDirectory);

        // get the configuration before building the app
        IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables()
            .Build();

        
        var dataPath = config.GetSection("PathSettings")["DataPath"] ?? "data";
        var logPath = config.GetSection("PathSettings")["LogPath"] ?? "logs";
        var primaryIndexType = config.GetSection("ServerSettings")["PrimaryIndexType"] ?? "Dictionary";

        var indexType = Enum.Parse<IndexType>(primaryIndexType, ignoreCase: true);

        // create or open the datastore before application starts
        var store = new DataStore(dataPath, indexType);


        ////////////////////////////////
        // Dependency injection

        var builder = WebApplication.CreateSlimBuilder(args);

        builder.Host.UseSerilog((context, configuration) =>
            configuration.MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Information)
                .MinimumLevel.Override("System", Serilog.Events.LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .WriteTo.File($"{logPath}/log.txt", rollingInterval: RollingInterval.Day, retainedFileCountLimit: 7)
            );
        

        builder.Services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, FrozenCache.AppJsonSerializerContext.Default);
        });

        builder.Host.UseWindowsService(o =>
        {
            o.ServiceName = "FrozenCache Service";
        }).UseSystemd();

        builder.Services.AddHostedService<HostedTcpServer>();

        builder.Services.AddSingleton<IDataStore>(store);

        builder.Services.AddHealthChecks();

        builder
            .Services
            .Configure<ServerSettings>(builder.Configuration.GetSection("ServerSettings"))
            ;

        var app = builder.Build();

        // Checked after Build() so that the message also reaches the log file (there is no console when running
        // as a service), but before opening the store or binding any port.
        var tcpPort = app.Services.GetRequiredService<IOptions<ServerSettings>>().Value.Port;
        var conflictingUrls = HttpEndpoints.UsingPort(app.Configuration, tcpPort);
        if (conflictingUrls.Count > 0)
        {
            app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("FrozenCache.Startup").LogCritical(
                "Configuration error: the HTTP endpoint {HttpUrls} uses port {Port}, which is also ServerSettings:Port, " +
                "the port of the cache TCP protocol. Local clients would reach the HTTP server instead of the cache. " +
                "Change Kestrel:Endpoints or ServerSettings:Port in appsettings.json so that they differ. " +
                "The server will not start.",
                string.Join(", ", conflictingUrls), tcpPort);

            // flushes the log file
            app.DisposeAsync().AsTask().GetAwaiter().GetResult();

            Environment.ExitCode = 1;
            return;
        }

        var logger = app.Services.GetService<ILogger<DataStore>>();

        
        store.Open(logger);

        
        // Collection information
        var collectionsApi = app.MapGroup("/collections");
        collectionsApi.MapGet("/", (IDataStore dataStore ) =>
        {
            var collections = dataStore.GetCollectionInformation().Collections;
            return Results.Ok(collections);
        });

        // Per-minute lookup statistics of a collection over the last 24 hours
        collectionsApi.MapGet("/{name}/monitoring", (string name, IDataStore dataStore) =>
        {
            var history = dataStore.GetMonitoring(name);
            if (history == null)
                return Results.NotFound();

            var minutes = history
                .Select(m => new MinuteStatistics(m.Timestamp, m.Calls, m.RetrievedObjects,
                    m.Calls == 0 ? 0 : TimeSpan.FromTicks(m.TotalTicks).TotalMicroseconds / m.Calls))
                .ToArray();

            return Results.Ok(new CollectionMonitoring(name, minutes));
        });

        // Default health checks
        app.MapHealthChecks("/health");

        app.Run();
    }

}


/// <summary>
/// Response of <c>GET /collections/{name}/monitoring</c>.
/// </summary>
/// <param name="Collection">Collection name.</param>
/// <param name="Minutes">One entry per minute with at least one lookup, oldest first; the last one may be partial.</param>
internal sealed record CollectionMonitoring(string Collection, MinuteStatistics[] Minutes);

/// <param name="Minute">Start of the minute, UTC.</param>
/// <param name="Calls">Number of lookups by primary key.</param>
/// <param name="RetrievedObjects">Number of objects returned by those lookups.</param>
/// <param name="AverageMicroseconds">Average server-side duration of one lookup.</param>
internal sealed record MinuteStatistics(DateTime Minute, long Calls, long RetrievedObjects, double AverageMicroseconds);

[JsonSerializable(typeof(CollectionsDescription))]
[JsonSerializable(typeof(CollectionInformation))]
[JsonSerializable(typeof(CollectionMonitoring))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{

}
