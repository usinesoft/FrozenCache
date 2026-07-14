# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

FrozenCache is a feed-and-read cache server. Data is fed into a *collection* as an immutable, versioned
snapshot; once fed, a version can be read concurrently by many clients but never mutated. Feeding a new
version atomically replaces the previous one for subsequent reads. It's a custom TCP protocol + memory-mapped
file storage engine, not a wrapper around an existing database.

All source lives under `FrozenCache/` (the repo root has only this file and `README.md`); the solution file is
`FrozenCache/FrozenCache.sln`.

## Build, test, run

Always run these from `FrozenCache/FrozenCache/` (the directory containing the `.sln`).

```
dotnet build FrozenCache.sln
dotnet test UnitTests/UnitTests.csproj
dotnet test UnitTests/UnitTests.csproj --filter "FullyQualifiedName~IntegrationTest"
dotnet test UnitTests/UnitTests.csproj --filter "Name=FeedCollection"
dotnet run --project FrozenCache/FrozenCache.csproj
```

Tests use NUnit (not xUnit/MSTest) — filter with `Name=`, `FullyQualifiedName~`, or `TestCategory=` accordingly.
Test targets `net10.0`; other projects multi-target `net8.0;net10.0`. `PersistentStore` and `ProfilingTool` use
`AllowUnsafeBlocks` (memory-mapped file pointer access).

There is no separate lint step; `dotnet build` surfaces analyzer warnings (Roslyn + SonarAnalyzer-style `S`-prefixed
rule suppressions appear inline, e.g. `#pragma warning disable S6667`).

The server host project (`FrozenCache/FrozenCache.csproj`) is AOT-published (`PublishAot=true`) and uses
`WebApplication.CreateSlimBuilder` — avoid adding reflection-heavy APIs or non-source-generated JSON serialization
to that project; JSON types must be registered on the `JsonSerializerContext` in `Program.cs`.

## Project layout and dependency direction

```
Messages          <- wire protocol (MessagePack DTOs, MessageType enum, TCP framing helpers). No project deps.
PersistentStore    <- storage engine (memory-mapped files, indexes). Depends on Messages.
FrozenCache        <- server host (ASP.NET Core minimal API + a raw TCP listener as an IHostedService).
                       Depends on Messages, PersistentStore, CacheClient (client used by ProfilingTool via this project).
CacheClient        <- client library (connection pooling, aggregation across replicas, local LRU cache).
                       Depends on Messages, PersistentStore (for the Item type).
ProfilingTool       <- perf-testing console app, feeds data through the real TCP path.
PerTest             <- another standalone perf/load console app driving a running server on localhost:5123.
UnitTests           <- NUnit tests; depends on every other project (only project allowed to reference everything).
```

`Messages` and `PersistentStore` must stay free of dependencies on `FrozenCache`/`CacheClient` — the dependency
graph is intentionally one-directional (protocol/storage at the bottom, server and client on top).

## Wire protocol (`Messages`)

- Every message implements `IMessage` with a `MessageType` (`Messages/MessageType.cs`). Framing is manual:
  an 8-byte header (4-byte `MessageType` int, 4-byte body length) followed by a MessagePack-serialized body,
  written/read via `StreamingHelper.WriteMessageAsync`/`ReadMessageAsync` (`Messages/StreamingHelper.cs`).
- Adding a new message type means: add the enum value, add the MessagePack DTO, and add a case to **both**
  the serialize switch and the deserialize switch in `StreamingHelper` — the two are not derived from each
  other and will silently mismatch if only one is updated.
- `FeedItemBatchSerializer`/`IBatchSerializer` handle the bulk item-streaming path used during a feed session,
  separate from the single-message framing above.
- `PingMessage` and a few request types have no body — they're special-cased in `StreamingHelper` since a
  zero-length MessagePack payload is skipped entirely for latency.

## Storage engine (`PersistentStore`)

- `DataStore` owns all collections under a root path; each collection is a subdirectory containing
  `metadata.json` (a `CollectionMetadata`, versioned via `CollectionMetadata.IsCompatibleWith` for schema checks)
  plus one subdirectory per version. Only the newest version directory is memory-mapped at any time
  (`DataStore.Open`/`EndFeed`); older versions are pruned per `MaxVersionsToKeep`.
- `CollectionStore` is the per-version engine: data is split across `*.bin` memory-mapped files (default 1 GB /
  1M documents each, see `Consts.cs`). Each file starts with a fixed-size header region (one `PersistentObjectHeader`
  per document slot: offset, length, and index key values) followed by raw document bytes. Reading a document is a
  two-step lookup: index → `IndexEntry` (file index, offset, length) → memory-mapped read.
- Indexing is on the first key of `Item.Keys` only (the primary key); `IIndex` has two implementations chosen via
  `IndexType`/`ServerSettings.PrimaryIndexType`: `DictionaryIndex` (fast, more memory) and `OrderedIndex` (slower,
  less memory). Non-primary keys in `Item.Keys` are stored in the header but are not currently independently
  indexed/queryable.
- Feeding is append-only and single-pass: `CollectionStore.StoreNewDocument` writes sequentially, and
  `EndOfFeed`/`CreateIndexes` runs index post-processing only once at the end — collections are not incrementally
  updatable mid-feed.
- Unsafe pointer access to memory-mapped views (`ReadBytes`/`WriteBytes`/`ReadFileMap`) assumes the caller has
  already validated offsets/lengths; there are no bounds checks at that layer.

## Server (`FrozenCache/HostedTcpServer.cs`)

- One `IHostedService` runs a raw `TcpListener` alongside the normal ASP.NET Core HTTP pipeline (started in
  `Program.cs`). HTTP is used only for `/health` and `GET /collections`; all data operations (create/drop
  collection, feed, query) go over the custom TCP protocol on the port from `ServerSettings.Port`
  (`appsettings.json` → `ServerSettings:Port`, default 5123).
- Each accepted TCP connection runs its own `ClientLoop` task reading one `IMessage` at a time and dispatching
  by type; a feed session (`BeginFeedRequest`) switches the same connection into a long-lived streaming mode via
  `ReadItems`/an internal bounded `Channel<FeedItem>` until the client sends an end-of-batch marker.
- Working directory is force-reset to `AppContext.BaseDirectory` at startup because Windows Service hosting
  defaults to `C:\Windows\System32`.

## Client (`CacheClient`)

- `Connector` is a single TCP connection wrapper (one request/response or one feed session at a time).
  `ConnectorPool` pools multiple `Connector`s to one server and runs a background watchdog that pings and
  reconnects on failure, flipping `IsConnected`. `Aggregator` fans a logical operation out across multiple
  `ConnectorPool`s (replicas) — reads round-robin across connected pools, feeds go to *all* pools in parallel,
  and `DropCollection` requires all replicas to be up.
- `Aggregator.RegisterTypedCollection<T>` + `FeedCollection<T>`/`QueryByPrimaryKey<T>` layer typed
  serialize/deserialize/key-extraction on top of the raw `byte[]`/`Item` protocol — register a type before
  feeding/querying it by type.
- `LruLocalCache` (per-collection, opt-in via `Aggregator.ConfigureLocalCache`) sits in front of server queries
  and also caches negative lookups (a "not found" marker) to avoid repeated round-trips for missing keys.
