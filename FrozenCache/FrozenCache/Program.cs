using System.Text.Json.Serialization;
using Messages;
using PersistentStore;
using Serilog;
using Microsoft.Extensions.Hosting.WindowsServices;

namespace FrozenCache;

internal static class Program
{
    public static void Main(string[] args)
    {

        
        // when run in windows service mode, by default the working directory is C:\Windows\System32
        var currentDirectory = AppContext.BaseDirectory;
        Directory.SetCurrentDirectory(currentDirectory);

        // get the configuration before building the app
        IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables()
            .Build();

        
        var dataPath = config.GetSection("PathSettings")["DataPath"] ?? "data";
        var logPath = config.GetSection("PathSettings")["LogPath"] ?? "logs";

        // create or open the datastore before application starts
        var store = new DataStore(dataPath);


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


        var logger = app.Services.GetService<ILogger<DataStore>>();

        
        store.Open(logger);

        
        // Collection information
        var collectionsApi = app.MapGroup("/collections");
        collectionsApi.MapGet("/", (IDataStore dataStore ) =>
        {
            var collections = dataStore.GetCollectionInformation().Collections;
            return Results.Ok(collections);
        });

        // Default health checks
        app.MapHealthChecks("/health");

        app.Run();
    }

}

public record Todo(int Id, string? Title, DateOnly? DueBy = null, bool IsComplete = false);

[JsonSerializable(typeof(Todo[]))]
[JsonSerializable(typeof(CollectionsDescription))]
[JsonSerializable(typeof(CollectionInformation))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{

}
