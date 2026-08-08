# FrozenCache

FrozenCache is a feed-and-read cache server. Data is fed into a *collection* as an immutable,
versioned snapshot; once fed, that version can be read concurrently by many clients but never
mutated. Feeding a new version atomically replaces the previous one for all subsequent reads.

It's a custom TCP protocol plus a memory-mapped-file storage engine — not a wrapper around an
existing database — built for workloads where data changes in full batches (a nightly load, a
recomputed index, a snapshot from another system) and needs to be served back with very low read
latency in between.

- **Immutable, versioned data** — a version is either fully fed or not visible at all; no partial
  writes, no read tearing, no locking on the read path.
- **Fast reads** — memory-mapped storage, primary-key and secondary-index lookups, and streaming
  for full-collection or filtered scans.
- **AOT-published server** — starts fast, small footprint; published as a self-contained native
  executable for win-x64 and linux-x64 by CI.
- **.NET client** with connection pooling, multi-replica fan-out, and an optional local LRU cache
  (`CacheClient`).

## Quick start

### 1. Run the server

```bash
git clone https://github.com/usinesoft/FrozenCache.git
cd FrozenCache/FrozenCache
dotnet run --project FrozenCache/FrozenCache.csproj
```

By default the server listens for the TCP protocol on port `5123` (configurable via
`appsettings.json` → `ServerSettings:Port`) and exposes `GET /health` and `GET /collections` over
HTTP. The default config also enables SSL and points at a certificate that won't exist on a fresh
clone — for local testing, disable it with an environment variable override instead of editing
`appsettings.json`:

```bash
ServerSettings__UseSsl=false dotnet run --project FrozenCache/FrozenCache.csproj
```

### 2. Connect, create a collection, and feed data

```csharp
using CacheClient;
using Messages;

using var connector = new Connector("localhost", 5123);
connector.Connect();

// primary key "id", plus a secondary index on "customerId"
await connector.CreateCollection("orders", "id", "customerId");

var orders = new[]
{
    new Item(Encode(new { Id = 1, CustomerId = 42, Total = 19.90 }), 1, 42),
    new Item(Encode(new { Id = 2, CustomerId = 42, Total = 5.00 }), 2, 42),
    new Item(Encode(new { Id = 3, CustomerId = 7, Total = 100.00 }), 3, 7)
};

await connector.FeedCollection("orders", "v1", orders);

// however you want to serialize a document - System.Text.Json is a convenient default
static byte[] Encode(object value) => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value);
```

`Item` bundles a document's raw bytes with the keys that index it: the first key is always the
primary key, and any keys after it correspond to the secondary indexes declared on
`CreateCollection`, in the same order.

### 3. Read it back

```csharp
// point lookup by primary key - returns one byte[] per match
var byId = await connector.QueryByPrimaryKey("orders", 1);

// everything for customer 42, via the secondary index - streamed, paired with the primary key
await foreach (var (primaryKey, data) in connector.StreamBySecondaryIndex("orders", "customerId", 42))
    Console.WriteLine($"order {primaryKey}: {data.Length} bytes");

// the whole collection
await foreach (var (primaryKey, data) in connector.StreamAllData("orders"))
    Console.WriteLine($"order {primaryKey}");
```

### 4. Feed a new version

Feeding again under a new version name atomically replaces the previous one once the feed
completes — readers never see a partial update, and anyone still streaming the old version keeps
seeing a consistent snapshot of it until they're done:

```csharp
await connector.FeedCollection("orders", "v2", updatedOrders);
```

## Where to go next

- [`CLAUDE.md`](CLAUDE.md) documents the project layout, wire protocol, and storage engine
  internals in more depth.
- `CacheClient.Aggregator` is the production-oriented client: it pools connections across
  multiple server replicas, fans feeds out to all of them in parallel, and can register typed
  collections (`RegisterTypedCollection<T>`) to work with your own types instead of raw `byte[]`.
- `ProfilingTool` and `PerTest` are example console apps that drive a running server for
  load/perf testing.

## License

MIT — see [LICENSE](LICENSE).
