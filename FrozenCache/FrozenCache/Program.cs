using System.Text.Json.Serialization;
using PersistentStore;

namespace FrozenCache;

internal static class Program
{
    public static void Main(string[] args)
    {

        // create or open the datastore before application starts
        var store = new DataStore("data");

        store.Notification += (sender, eventArgs) =>
        {
            Console.WriteLine($"{eventArgs.Message} at {DateTime.Now}");
        };


        store.Open();

        var builder = WebApplication.CreateSlimBuilder(args);

        builder.Services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, FrozenCache.AppJsonSerializerContext.Default);
        });

        builder.Services.AddHostedService<HostedTcpServer>();

        builder.Services.AddSingleton<IDataStore>(store);

        builder
            .Services
            .Configure<ServerSettings>(builder.Configuration.GetSection("ServerSettings"));

        var app = builder.Build();

        
        var sampleTodos = new Todo[] {
            new(1, "Walk the dog"),
            new(2, "Do the dishes", DateOnly.FromDateTime(DateTime.Now)),
            new(3, "Do the laundry", DateOnly.FromDateTime(DateTime.Now.AddDays(1))),
            new(4, "Clean the bathroom"),
            new(5, "Clean the car", DateOnly.FromDateTime(DateTime.Now.AddDays(2)))
        };

        var todosApi = app.MapGroup("/todos");
        todosApi.MapGet("/", () => sampleTodos);
        todosApi.MapGet("/{id}", (int id) =>
            sampleTodos.FirstOrDefault(a => a.Id == id) is { } todo
                ? Results.Ok(todo)
                : Results.NotFound());

        app.Run();
    }

}

public record Todo(int Id, string? Title, DateOnly? DueBy = null, bool IsComplete = false);

[JsonSerializable(typeof(Todo[]))]
[JsonSerializable(typeof(CollectionMetadata))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{

}
