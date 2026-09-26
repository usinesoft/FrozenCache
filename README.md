# FrozenCache

A feed-and-read cache server for large reference datasets. Data is fed into a *collection* as an
immutable, versioned snapshot; once fed, a version is served lock-free to any number of concurrent
readers and never mutated. Feeding a new version atomically replaces the previous one.

FrozenCache is a custom TCP protocol on top of a memory-mapped storage engine — not a wrapper
around an existing database. Server and client are .NET (server: .NET 10, client libraries:
.NET 8 and .NET 10).

## Contents

- [Overview](#overview)
- [Design](#design)
- [Architecture](#architecture)
- [Getting started](#getting-started)
- [Server configuration](#server-configuration)
- [Client API](#client-api)
  - [Aggregator with typed data](#aggregator-with-typed-data)
  - [Low-level `Connector`](#low-level-connector)
  - [Keys and `KeyProducers`](#keys-and-keyproducers)
- [Versions and consistency](#versions-and-consistency)
- [HTTP endpoints](#http-endpoints)
- [Deployment](#deployment)
- [Tools](#tools)
- [Building and testing](#building-and-testing)
- [Limitations](#limitations)
- [License](#license)

---

## Overview

Modern systems rely on fast access to large reference datasets: product catalogs, financial
referentials, pricing tables, risk parameters. These collections are large, read constantly, and
updated only a few times per day. General-purpose distributed caches handle this, but carry
complexity the pattern doesn't need.

FrozenCache targets exactly that pattern: **feed the entire dataset, then read it at high speed
until the next update**. Narrowing the scope enables optimizations a general-purpose cache can't
offer:

| Property | How |
|---|---|
| **Lock-free reads** | The whole dataset is replaced at once, so readers never contend with writers. |
| **Immutable, pre-built indexes** | Data never changes between feeds, so indexes are built once at the end of a feed and never updated incrementally — no fragmentation, no performance drift. |
| **Trivial replication** | Feed all replicas with the same data, then let them serve independently. No synchronization protocol between servers. |
| **Guaranteed consistency** | Exactly one version is active at a time. A new version becomes visible only after it is fully written and indexed; clients never observe a partial feed. |

It is not a general-purpose cache: there are no per-item writes, TTLs, or incremental updates.

## Design

### Serialization is the client's business

Every document is stored as an opaque **`byte[]` blob**. The server makes no assumption about the
format — JSON, MessagePack, Protobuf or a custom binary layout are all equally valid. This keeps
the server simple and removes any coupling between server version and client data model.

### Data on disk, indexes in memory

Instead of requiring enough RAM to hold the full dataset (and sharding once it doesn't fit),
FrozenCache stores documents on disk as a sequence of **memory-mapped files** and keeps only the
**indexes in memory**.

Memory-mapped files, on NVMe storage, give:

- reads that are often several times faster than stream-based file access;
- automatic caching: pages stay in physical memory until the OS needs to evict them, effectively a
  built-in LRU cache with no cache-management code;
- predictable performance, with no manual buffering or paging logic.

The result is close to in-memory read performance with disk-level capacity.

### All index values are `int64`

Rather than supporting strings, dates, floats etc. as index types, **every index value is a
64-bit integer**. A single fixed-size type that matches the CPU word size gives the fastest
comparisons, fixed-size index blocks and a simple memory layout.

Other types are converted on the client:

| Source type | Conversion |
|---|---|
| Timestamps, dates | ticks |
| Floating point / decimal | multiplied by a fixed precision factor |
| Booleans, enums, numeric ids | direct cast |
| Strings | a 64-bit hash-based encoding |

A hash collision cannot corrupt data: the real value is still inside the stored document, so a
collision only means a lookup returns an extra candidate that the client can filter out.

---

## Architecture

```
               ┌────────────────────────── client process ──────────────────────────┐
               │  Aggregator  ── typed (de)serialization, local LRU cache, replicas │
               │     │                                                              │
               │  ConnectorPool (one per replica, watchdog + reconnect)            │
               │     │                                                              │
               │  Connector (one TCP connection)                                    │
               └─────┼──────────────────────────────────────────────────────────────┘
                     │  custom TCP protocol (MessagePack bodies, optional TLS)
               ┌─────▼──────────── FrozenCache server (one per replica) ────────────┐
               │  HostedTcpServer  ── one ClientLoop per connection                 │
               │     │                                                              │
               │  DataStore  ── one CollectionStore per collection (latest version) │
               │     │                                                              │
               │  memory-mapped *.bin files on disk  +  in-memory primary index     │
               └────────────────────────────────────────────────────────────────────┘
```

### Projects

All source is under `FrozenCache/`; the solution is `FrozenCache/FrozenCache.sln`.

| Project | Role | Depends on |
|---|---|---|
| `Messages` | Wire protocol: message DTOs, framing helpers | — |
| `PersistentStore` | Storage engine: memory-mapped files, indexes | Messages |
| `FrozenCache.Server` | TCP server engine (`HostedTcpServer`, `ServerSettings`) | Messages, PersistentStore |
| `FrozenCache` | Server executable (ASP.NET Core host, Native AOT) | FrozenCache.Server, Messages, PersistentStore |
| `CacheClient` | Client library: `Connector`, `ConnectorPool`, `Aggregator` | Messages |
| `PerfTest` | Load/perf console app against a running server | CacheClient, Messages |
| `ProfilingTool` | Profiling console app hosting the server in-process | CacheClient, FrozenCache.Server, … |
| `UnitTests` | NUnit tests | CacheClient, FrozenCache.Server, Messages, PersistentStore |

A client application only needs `CacheClient` (which brings in `Messages`).

### Storage layout

```
<DataPath>/
  <collection>/
    metadata.json          # keys, file sizes, versions to keep
    <version>/
      0000.bin             # memory-mapped segment: header region + document bytes
      0001.bin
      .complete/           # marker written once the version is fully fed and indexed
```

- Each segment file is up to 1 GB / 1,000,000 documents. It starts with a fixed-size header per
  document slot (offset, length, key values), followed by the raw document bytes.
- Only the newest complete version of each collection is mapped. Older versions are pruned,
  keeping `MaxVersionsToKeep` (default 2) versions on disk.
- A version directory without the `.complete` marker is an interrupted feed and is ignored.

### Wire protocol

Each message is an 8-byte header — a 4-byte message type and a 4-byte body length — followed by a
MessagePack-serialized body. A feed session switches the connection into a streaming mode where
documents are sent in batches until an end-of-feed marker. Connections can optionally be wrapped
in TLS.

---

## Getting started

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) to build.
- To run a prebuilt release, see [Deployment](#deployment).

### 1. Run the server

```bash
git clone https://github.com/usinesoft/FrozenCache.git
cd FrozenCache/FrozenCache
dotnet run --project FrozenCache/FrozenCache.csproj
```

The shipped `appsettings.json` enables TLS and expects a certificate at
`C:\FrozenCache\dev-cert.pfx`, and stores data under `C:\FrozenCache\data`. For a quick local run,
override those with environment variables instead of editing the file:

```bash
# bash
ServerSettings__UseSsl=false PathSettings__DataPath=./data PathSettings__LogPath=./logs \
  dotnet run --project FrozenCache/FrozenCache.csproj
```

```powershell
# PowerShell
$env:ServerSettings__UseSsl = "false"; $env:PathSettings__DataPath = ".\data"; $env:PathSettings__LogPath = ".\logs"
dotnet run --project FrozenCache/FrozenCache.csproj
```

To test TLS locally, export a development certificate and point the server at it:

```bash
dotnet dev-certs https -ep C:\FrozenCache\dev-cert.pfx -p <password>
```

### 2. Reference the client

Add a project reference to `FrozenCache/CacheClient/CacheClient.csproj`. The typed examples below
use [MessagePack](https://github.com/MessagePack-CSharp/MessagePack-CSharp) for serialization, but
any serializer works.

```bash
dotnet add reference path/to/FrozenCache/CacheClient/CacheClient.csproj
dotnet add package MessagePack
```

### 3. Feed and query

See [Aggregator with typed data](#aggregator-with-typed-data).

---

## Server configuration

Settings come from `appsettings.json` next to the executable, overridable by environment
variables (`Section__Key`, e.g. `ServerSettings__Port=6000`).

| Setting | Default in `appsettings.json` | Description |
|---|---|---|
| `ServerSettings:Port` | `5123` | TCP port for the cache protocol. Listens on all interfaces. |
| `ServerSettings:PrimaryIndexType` | `Ordered` | `Dictionary` (fastest lookups, more memory) or `Ordered` (sorted array, less memory). `Dictionary` if unset. |
| `ServerSettings:UseSsl` | `true` | Wrap every TCP connection in TLS. Clients must use the same setting. |
| `ServerSettings:SslCertificatePath` | `C:\FrozenCache\dev-cert.pfx` | PFX certificate used when `UseSsl` is `true`. |
| `ServerSettings:SslCertificatePassword` | — | Password for the PFX file. |
| `PathSettings:DataPath` | `C:\FrozenCache\data` | Root directory for collections. `data` (relative to the executable) if unset. |
| `PathSettings:LogPath` | `C:\FrozenCache\logs` | Directory for daily rolling log files (7 kept). `logs` if unset. |
| `Kestrel:Endpoints` | — | HTTP endpoint for `/health` and `/collections`. **Must use a different port than `ServerSettings:Port`.** |

Relative paths are resolved against the executable's directory: the server sets its working
directory to its own location at startup, because Windows services otherwise start in
`C:\Windows\System32`.

---

## Client API

The client library (`CacheClient`) has three layers:

| Class | Use it for |
|---|---|
| `Aggregator` | Application code. Talks to one or more replicas, handles typed data, reconnection, replica selection and an optional local cache. |
| `ConnectorPool` | A pool of connections to one server with a background watchdog. Used internally by `Aggregator`. |
| `Connector` | A single TCP connection; raw `byte[]` API. Useful for tools, admin tasks and secondary-index queries. |

### Aggregator with typed data

The typed API works in four steps: **declare** the collection on the servers, **register** how to
serialize your type and extract its keys, **feed**, then **query**.

#### Define the document type

```csharp
using MessagePack;

[MessagePackObject]
public class Product
{
    [Key(0)] public long Id { get; set; }
    [Key(1)] public string Name { get; set; } = "";
    [Key(2)] public long CategoryId { get; set; }
    [Key(3)] public decimal Price { get; set; }
    [Key(4)] public DateOnly ValidFrom { get; set; }
}
```

#### Connect, declare and register

```csharp
using CacheClient;
using MessagePack;

// Pool capacity 4 per server; one entry per replica.
// The constructor connects to every replica before returning.
var aggregator = new Aggregator(4, ("cache-01", 5123), ("cache-02", 5123));

// Creates the collection on every connected replica. No-op if it already exists with the
// same keys; fails if it exists with different keys.
// First key = primary key; the others are secondary indexes, in this order.
await aggregator.DeclareCollection("products", "id", "categoryId", "validFrom");

// Serializer, deserializer, then one key generator per declared key, in the same order.
aggregator.RegisterTypedCollection<Product>("products",
    p => MessagePackSerializer.Serialize(p),
    bytes => MessagePackSerializer.Deserialize<Product>(bytes),
    p => p.Id,                             // "id"         (primary key)
    p => p.CategoryId,                     // "categoryId"
    p => p.ValidFrom.FromDateOnly());      // "validFrom"  (see KeyProducers)
```

`RegisterTypedCollection` is client-side only — it doesn't contact the server — but it requires at
least one replica to be connected. Registration is keyed by the CLR type, so register each type
once, for one collection.

#### Feed a new version

```csharp
IEnumerable<Product> products = LoadProductsFromDatabase();   // streamed, not materialized

await aggregator.FeedCollection("products", products);
```

`FeedCollection<T>`:

- serializes each item once and streams it to **all connected replicas in parallel**;
- enumerates the source lazily with bounded buffering, so feeding millions of items doesn't require
  holding them all in memory;
- names the new version from the current UTC time;
- makes the new version visible on each replica only once that replica has received and indexed
  all of it.

#### Query by primary key

```csharp
List<Product> one  = await aggregator.QueryByPrimaryKey<Product>("products", 42);
List<Product> many = await aggregator.QueryByPrimaryKey<Product>("products", 1, 2, 3, 500);

// Keys that don't exist are simply absent from the result.
Product? product = one.FirstOrDefault();
```

The primary key doesn't have to be unique: a key returns every document fed with it.

Reads are round-robined across connected replicas, preferring replicas known to have the latest
version of the collection. If a replica fails mid-query, it is marked as disconnected and the
query is retried on another one; the watchdog reconnects it in the background.

Raw bytes are also available when you want to deserialize yourself:

```csharp
List<byte[]> raw = await aggregator.QueryRawDataByPrimaryKey("products", 42);
```

#### Enable a local cache

For hot keys, an in-process LRU cache avoids a network round-trip. It also caches "not found"
results.

```csharp
aggregator.ConfigureLocalCache("products", capacity: 100_000);

var p = await aggregator.QueryByPrimaryKey<Product>("products", 42);   // server
p     = await aggregator.QueryByPrimaryKey<Product>("products", 42);   // local cache

foreach (var (collection, stats) in aggregator.GetStatistics())
    Console.WriteLine($"{collection}: {stats.FoundInLocalCache}/{stats.Calls} hits, " +
                      $"{stats.AverageMillisecondsForExternalCache:F2} ms per server call");
```

- The local cache is cleared automatically when the watchdog detects a new version, so reads can
  be stale for at most one watchdog interval (10 s by default).
- It stores **one document per key**, so only use it for collections whose primary key is unique.

#### React to new versions

```csharp
aggregator.NewVersion += (_, e) =>
    Console.WriteLine($"{e.CollectionName} is now at version {e.NewVersion}");

string? current = aggregator.GetLastVersion("products");
```

`NewVersion` fires when any replica reports a newer version of a collection. By the time it
fires, the local cache for that collection is cleared and `GetLastVersion` returns the new value.

#### TLS and watchdog frequency

```csharp
var aggregator = new Aggregator(
    capacity: 4,
    useSsl: true,
    validateServerCertificate: true,           // false only for self-signed test certificates
    watchDogFrequencyInMilliseconds: 5_000,
    ("cache-01", 5123), ("cache-02", 5123));
```

#### Administration

```csharp
// One entry per replica; null for a replica that is unreachable.
CollectionsDescription?[] perReplica = await aggregator.GetCollectionsDescription();

foreach (var info in perReplica.OfType<CollectionsDescription>().SelectMany(d => d.Collections))
    Console.WriteLine($"{info.Name} v{info.LastVersion}: {info.Count} items, {info.SizeInBytes} bytes");

// Removes every version and the metadata. Requires ALL replicas to be connected.
await aggregator.DropCollection("products");
```

#### Complete example

```csharp
using CacheClient;
using MessagePack;

var aggregator = new Aggregator(4, ("localhost", 5123));

await aggregator.DeclareCollection("products", "id", "categoryId");

aggregator.RegisterTypedCollection<Product>("products",
    p => MessagePackSerializer.Serialize(p),
    b => MessagePackSerializer.Deserialize<Product>(b),
    p => p.Id,
    p => p.CategoryId);

var products = Enumerable.Range(1, 1_000_000).Select(i => new Product
{
    Id = i,
    Name = $"Product {i}",
    CategoryId = i % 100,
    Price = 9.99m + i % 50,
    ValidFrom = new DateOnly(2026, 1, 1)
});

await aggregator.FeedCollection("products", products);

aggregator.ConfigureLocalCache("products", capacity: 10_000);

foreach (var p in await aggregator.QueryByPrimaryKey<Product>("products", 1, 2, 3))
    Console.WriteLine($"{p.Id}: {p.Name} ({p.Price})");

[MessagePackObject]
public class Product
{
    [Key(0)] public long Id { get; set; }
    [Key(1)] public string Name { get; set; } = "";
    [Key(2)] public long CategoryId { get; set; }
    [Key(3)] public decimal Price { get; set; }
    [Key(4)] public DateOnly ValidFrom { get; set; }
}
```

### Low-level `Connector`

`Connector` is a single connection with a raw `byte[]` API. It is not thread-safe: use one
connector per concurrent operation. Unlike `Aggregator`, it lets you choose the version name and
exposes secondary-index queries and full-collection streaming.

```csharp
using CacheClient;
using Messages;

using var connector = new Connector("localhost", 5123, useSsl: false);
if (!connector.Connect())
    throw new InvalidOperationException(connector.LastError);

// primary key "id", secondary index "customerId"
await connector.CreateCollection("orders", "id", "customerId");

// Item = raw bytes + key values: primary key first, then secondary keys in declaration order
var orders = new[]
{
    new Item(Encode(new { Id = 1, CustomerId = 42, Total = 19.90 }), 1, 42),
    new Item(Encode(new { Id = 2, CustomerId = 42, Total = 5.00 }), 2, 42),
    new Item(Encode(new { Id = 3, CustomerId = 7, Total = 100.00 }), 3, 7)
};

await connector.FeedCollection("orders", "20260926-120000", orders);

// by primary key
List<byte[]> byId = await connector.QueryByPrimaryKey("orders", 1);

// by secondary index - streamed, paired with the primary key
await foreach (var (primaryKey, data) in connector.StreamBySecondaryIndex("orders", "customerId", 42))
    Console.WriteLine($"order {primaryKey}: {data.Length} bytes");

// whole collection
await foreach (var (primaryKey, data) in connector.StreamAllData("orders"))
    Console.WriteLine($"order {primaryKey}");

static byte[] Encode(object value) => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value);
```

Secondary-index queries are not exposed by `Aggregator` yet. To get typed results, stream through
a `Connector` and deserialize yourself:

```csharp
await foreach (var (_, data) in connector.StreamBySecondaryIndex("products", "categoryId", 12))
{
    var product = MessagePackSerializer.Deserialize<Product>(data);
    // ...
}
```

Other `Connector` operations: `GetCollectionsDescription()`, `DropCollection(name)`, `Ping()`.

### Keys and `KeyProducers`

Key generators must return `long`. Integral ids can be returned directly (`p => p.Id`). For other
types, `CacheClient.KeyProducers` provides extension methods:

| Method | Conversion |
|---|---|
| `DateTime.FromDateTime()` / `DateTimeOffset.FromDateTimeOffset()` | UTC ticks |
| `DateOnly.FromDateOnly()` | ticks at midnight |
| `double.FromDouble()` / `float.FromFloat()` / `decimal.FromDecimal()` | value × 10,000 (4 decimal places) |
| `int.FromInt()` / `long.FromLong()` | value × 10,000, so integer and decimal keys compare consistently |
| `string.FromString()` | first 5 characters (order-preserving, case-insensitive) + hash |
| `KeyProducers.FromNull()` | `long.MinValue` |

The same conversion must be applied when feeding and when querying. For example, if the key
generator is `p => p.ValidFrom.FromDateOnly()`, query with `date.FromDateOnly()`, not with the raw
value.

---

## Versions and consistency

- A collection has at most one **active version** per server: the newest *complete* version.
- Versions are ordered by **name, as an ordinal string comparison**. Use names that sort
  chronologically, such as zero-padded timestamps (`20260926-143000`); with `v1` … `v10`, `v10`
  sorts before `v2`.
- A feed writes a new version directory. When the feed completes, the index is built, the version
  is marked complete, and it atomically replaces the active version. Readers switch on their next
  request; a stream already in progress keeps reading the old version consistently.
- If a feed is interrupted (client crash, disconnect), the incomplete version is discarded and the
  previous version stays active.
- Older versions beyond `MaxVersionsToKeep` (default 2) are deleted.
- Replicas are fed independently. A replica that is down during a feed doesn't receive the new
  version. `Aggregator` routes reads to replicas known to be on the latest version where possible.

---

## HTTP endpoints

The server also exposes a small HTTP API on the Kestrel endpoint (not on the TCP port):

| Endpoint | Description |
|---|---|
| `GET /health` | Returns `Healthy` when the server is up. |
| `GET /collections` | JSON array describing each collection: name, keys, item count, size in bytes, last version, segment file limits. |

All data operations (create, feed, query, drop) go over the TCP protocol.

---

## Deployment

### Release packages

GitHub releases contain three packages:

| Package | Contents | Requirements |
|---|---|---|
| `FrozenCache-<version>-win-x64.zip` | Native AOT executables | Windows x64, no .NET runtime needed |
| `FrozenCache-<version>-linux-x64.zip` | Native AOT executables | Linux x64, no .NET runtime needed |
| `FrozenCache-<version>-portable.zip` | Framework-dependent assemblies | [ASP.NET Core Runtime 10](https://dotnet.microsoft.com/download/dotnet/10.0) (server) / .NET Runtime 10 (PerfTest), any OS and architecture |

Each package contains a `FrozenCache/` (server) and a `PerfTest/` directory. The portable build
is started with `dotnet FrozenCache.dll`.

### Windows service

The server integrates with the Windows Service Control Manager:

```powershell
sc.exe create FrozenCache binPath= "C:\FrozenCache\bin\FrozenCache.exe" start= auto
sc.exe start FrozenCache
```

### Linux (systemd)

The server supports `Type=notify`:

```ini
# /etc/systemd/system/frozencache.service
[Unit]
Description=FrozenCache
After=network.target

[Service]
Type=notify
ExecStart=/opt/frozencache/FrozenCache
WorkingDirectory=/opt/frozencache
Environment=PathSettings__DataPath=/var/lib/frozencache
Environment=PathSettings__LogPath=/var/log/frozencache
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

On Linux, set `DataPath`/`LogPath` explicitly: the defaults in `appsettings.json` are Windows
paths.

---

## Tools

### PerfTest

Load test against a running server.

```
PerfTest [feed|read|stream] [options]

  feed     create the collection (if needed) and feed a new version named yyyyMMdd-HHmmss
  read     query random primary keys in batches of 1, 5, 10 and 100 (default)
  stream   stream the whole collection

  --server <host:port>   server to connect to (default localhost:5123)
  --collection <name>    collection name (default big)
  --size <count>         number of items to feed; feed only (default 2000000)
  --ssl                  connect over TLS (server certificate is not validated)
```

```bash
PerfTest feed --server cache-01:5123 --collection perf --size 10000000 --ssl
PerfTest read --server cache-01:5123 --collection perf --ssl
```

### ProfilingTool

Hosts the TCP server in-process with a no-op data store and feeds it through the real TCP path,
to profile protocol and serialization overhead in isolation from storage.

---

## Building and testing

Run from `FrozenCache/` (the directory containing `FrozenCache.sln`):

```bash
dotnet build FrozenCache.sln
dotnet test UnitTests/UnitTests.csproj
dotnet test UnitTests/UnitTests.csproj --filter "FullyQualifiedName~IntegrationTest"
```

Tests use NUnit. The server is published with Native AOT:

```bash
dotnet publish FrozenCache/FrozenCache.csproj -c Release -r linux-x64 -f net10.0 --self-contained
```

JSON types used by the server's HTTP endpoints must be registered on the source-generated
`JsonSerializerContext` in `FrozenCache/Program.cs`, since the executable is AOT-compiled.

CI (`.github/workflows/dotnet-desktop.yml`) runs the tests in Debug and Release, then publishes
the AOT and portable builds. The `Release` workflow is started manually with a version number:
it tags the commit of the latest successful `main` build and attaches that build's packages to a
GitHub release.

---

## Limitations

- **No partial updates.** A collection changes only by feeding a complete new version.
- **Secondary indexes** are queryable through `Connector.StreamBySecondaryIndex` only, not yet
  through `Aggregator`.
- **Integer keys only.** Non-numeric values must be converted to `long` on the client (see
  [Keys and `KeyProducers`](#keys-and-keyproducers)).
- **Replica drift.** Replicas are fed independently; if one is down during a feed, it stays on the
  previous version until the next feed.
- **`DropCollection`** through `Aggregator` requires all replicas to be connected.

## License

MIT — see [LICENSE](LICENSE).
