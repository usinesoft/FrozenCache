## Why Frozen Cache
Modern systems rely on fast access to large reference datasets: product catalogs, financial referentials, pricing tables, risk parameters, and more. These collections are huge, read constantly, and updated only a few times per day. 

In this very common scenario, traditional distributed caches are powerful—but they also carry complexity you don’t actually need.

Frozen Cache focuses on one specific pattern: feed the entire dataset, then read it at extreme speed until the next update. By narrowing the scope, it unlocks optimizations that general purpose caches simply cannot offer.

What makes it different
-	**Lock free access**, by design Many distributed caches fight hard to avoid locks—some even run single threaded to guarantee it. Frozen Cache doesn’t need tricks: when the whole dataset is replaced at once, reads are naturally lock free. Every lookup is instantaneous.
-	Immutable, **ultra optimized indexes** Because data never changes between updates, indexes can be built once and kept in memory as highly tuned, immutable structures. No incremental updates, no fragmentation, no performance drift.
-	**Effortless replication** Replication becomes trivial: feed all replicas at the same time, then let them serve traffic independently. No dynamic synchronization, no consistency issues, no replication storms.
-	**Guaranteed consistency** There is always exactly one active version of the dataset. A new version becomes visible only after it has been fully loaded, validated, and indexed—using atomic operations, not locks. Clients never see partial updates or inconsistent states.

## The result

A cache that is **blazing fast**, predictable, and operationally simple, precisely because it doesn’t try to solve every caching problem. For workloads built around large, immutable datasets with periodic refreshes, **Frozen Cache is not just an optimization—it’s the right tool**.

## A Conservative Design Choice
Frozen Cache takes a deliberately conservative stance on one core aspect: object serialization. All objects are stored as **raw byte[] blobs**, and the server makes **zero assumptions about the serialization format**. JSON, Protobuf, MessagePack, custom binary—it's entirely up to the client.

This keeps the server simple, predictable, and future proof. No hidden coupling. No forced serialization strategy. No surprises.

## Some Radical Design Choices
### Where should the data live?
Most distributed caches give an instinctive answer: in memory. But this answer comes with consequences:
-	You must have enough RAM to hold the entire dataset.
-	Large datasets require sharding, which complicates architecture and operations.
-	Memory pressure becomes a constant concern.

Modern servers equipped with NVMe SSDs open a different path—one that Frozen Cache embraces:
-	Store the data on disk, as a sequence of memory mapped files
-	Store the indexes in memory

This hybrid approach delivers the best of both worlds.

#### Why memory mapped files?
Memory mapped files behave very differently from traditional stream based file access:
-	Much faster reads — often up to 10× faster, depending on block size
-	Automatic caching — once a page is mapped into physical memory, the OS keeps it until memory pressure requires eviction
-	Zero code for cache management — the OS effectively provides a built in LRU cache
-	Predictable performance — no manual buffering, no custom paging logic

In practice, memory mapped files give you near RAM performance with disk level capacity.

### What data type should indexes use?
Most systems use multiple index types: strings, integers, dates, floats. It feels natural—but it introduces complexity:
-	Variable size values slow down lookups
-	Storage becomes fragmented
-	Index structures become harder to optimize

Frozen Cache takes a more radical approach:

**All index values are int64**.

A single, fixed size type that matches the CPU word size. This yields:
-	Fastest possible lookups
-	Fixed size index blocks
-	Simplified memory layout
-	Highly optimized CPU level operations

#### How do we convert everything to int64?
Most data types map naturally:
-	Timestamps → ticks
-	Floats → multiply by a fixed precision
-	Booleans / Enums → direct cast
-	Numeric identifiers → direct cast

Strings are the only challenge. Frozen Cache uses a double hashing algorithm that produces an almost unique 64 bit representation.

Collision probability is extremely low—and even if it happens, **data integrity is never at risk**:
-	The real attribute value is stored inside the serialized object
-	A collision simply means the lookup returns one extra candidate object
-	The client still receives correct data

This keeps indexes fast, compact, and predictable.
## The Result
By combining conservative choices (client controlled serialization) with radical ones (memory mapped storage, uniform int64 indexing), Frozen Cache achieves a rare balance:
-	Simplicity
-	Predictability
-	Extreme performance
-	Operational ease

>It’s not a general purpose cache. It’s a cache engineered for a very specific, very demanding workload—and it excels precisely because of these design decisions.




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
