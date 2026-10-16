# FrozenCache Rust client

A Rust client for the FrozenCache server, wire-compatible with the .NET server and the .NET `CacheClient`.
It works with the existing servers as they are: the same servers, the same collections, the same protocol, and
the same version names, so a collection can be fed and read by both clients.

| Crate | What it is | .NET counterpart |
|---|---|---|
| [`frozencache-client`](frozencache-client) | Client library: `Connector` (one connection), `Aggregator` (a cluster of replicas) | `CacheClient` (+ `Messages`) |
| [`perftest`](perftest) | Load/perf test tool, for one server or a cluster | `PerfTest` |

```toml
[dependencies]
frozencache-client = { path = "path/to/FrozenCache/rust/frozencache-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Aggregator: a cluster of replicas

```rust
use frozencache_client::{Aggregator, AggregatorOptions, CacheObject};

struct Product { id: i64, category: i64, name: String }

// how a type is stored: its bytes, and its keys (primary key first, then the secondary keys in declaration order)
impl CacheObject for Product {
    fn keys(&self) -> Vec<i64> { vec![self.id, self.category] }
    fn to_bytes(&self) -> Vec<u8> { /* any serialization: MessagePack, JSON, bincode... */ todo!() }
    fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> { todo!() }
}

#[tokio::main]
async fn main() -> frozencache_client::Result<()> {
    let servers = [("cache-01", 5123), ("cache-02", 5123)];
    let cluster = Aggregator::connect(&servers, AggregatorOptions::default()).await?;

    cluster.declare_collection("products", "id", &["category"]).await?;

    // to every replica, as one version (named after the current UTC time)
    let version = cluster.feed_collection("products", products()).await?; // any iterator of Product

    // round-robin across the replicas that have the latest version
    let some: Vec<Product> = cluster.query_by_primary_key("products", &[1, 2, 3]).await?;
    let category: Vec<Product> = cluster.query_by_secondary_key("products", "category", 12).await?;

    // streamed from one replica
    let mut all = cluster.stream_all_data("products");
    while let Some(product) = all.next_as::<Product>().await {
        let product = product?;
    }

    // an LRU cache in front of the servers for primary-key queries, cleared by each new version
    cluster.configure_local_cache("products", 100_000);

    // new versions, as the replicas report them
    let mut versions = cluster.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = versions.recv().await {
            println!("{} now has {} version {}", event.server, event.collection, event.version);
        }
    });
    Ok(())
}
```

| .NET `Aggregator` | Rust `Aggregator` |
|---|---|
| `new Aggregator(capacity, useSsl, validate, watchdogMs, servers)` | `Aggregator::connect(&servers, AggregatorOptions { pool_capacity, connect, watchdog_interval })` |
| `RegisterTypedCollection<T>(name, serialize, deserialize, keys...)` | `impl CacheObject for T` |
| `DeclareCollection` / `DropCollection` | `declare_collection` / `drop_collection` |
| `FeedCollection<T>(name, items)` | `feed_collection(name, objects)` (typed) or `feed_items(name, items)` (raw); both return the version |
| `QueryByPrimaryKey<T>` / `QueryRawDataByPrimaryKey` | `query_by_primary_key::<T>` / `query_raw_by_primary_key` |
| `QueryBySecondaryKey<T>` | `query_by_secondary_key::<T>` |
| `StreamBySecondaryKey<T>` / `StreamRawDataBySecondaryKey` | `stream_by_secondary_key(...)`, read with `next_as::<T>()` / `next()` |
| `StreamAllData` | `stream_all_data` |
| `ConfigureLocalCache` / `GetStatistics` | `configure_local_cache` / `local_cache_statistics` |
| `GetCollectionsDescription` | `get_collections_description` |
| `GetLastVersion` / `NewVersion` event | `last_version` / `subscribe()` (a broadcast channel) |
| — | `replicas()`: each server and whether it is connected |

### How it behaves

- **Reads** (queries, streams) go to one replica, round-robin among the connected ones that have the collection's
  latest version. When a replica's connection fails, it is marked down and the request moves to another one; a
  stream does so only before its first document (a retry would return documents twice), while
  `query_by_secondary_key` restarts on another replica. **Errors reported by a server** (unknown collection or
  index...) are returned as they are, never retried.
- **Feeds** go to every connected replica in parallel, under one version name. Each batch is encoded once and the
  same bytes go to every replica. Every replica validates the feed before any data is sent; if one fails during
  the upload, the feed is aborted on all of them, without the end marker, so no replica commits a partial version,
  and the error names the replica (`Error::Replica`).
- **Each server has a pool** of connections and a **watchdog** that reconnects it when it comes back and refreshes
  the versions it reports. A connection that was closed while lent (a stream not read to its end, a failed request)
  is replaced, so the pool keeps its size. A request on a server that is down fails at once instead of waiting.
- **`drop_collection`** needs every replica to be connected; otherwise the missing replica would bring the
  collection back.
- **The local cache** holds one document per key, so it is for collections with a unique primary key. Keys missing
  on the server are cached too. A cache miss is one request per key: the server's answers don't say which key a
  document belongs to.

### Differences from the .NET Aggregator

Deliberate, where the .NET behavior is a problem:

- **Primary-key queries retry only connection failures.** The .NET `QueryRawDataByPrimaryKey` retries every
  exception and marks the replica down each time, so a query on an unknown collection takes every replica down in
  turn.
- **The local cache doesn't block.** The .NET cache fetches missing keys synchronously under a global lock,
  serializing every query. Its statistics are always current; in .NET they only change on a cache miss. And an
  empty document is not mistaken for a missing one: .NET uses an empty array as its "not found" marker.
- **Versions known at once.** After a feed, each replica's new version is recorded immediately, rather than at the
  next watchdog check (up to 10 s later), so reads go straight to the new version. Version changes that happen
  while a server is down are reported when it reconnects.
- **No pool overgrowth:** a connection lent before a reconnection is dropped when returned, rather than added to an
  already refilled pool.
- `KeyProducers` (converting strings, dates or decimals to `long` keys) is not ported.

## Connector: one connection

```rust
use frozencache_client::{ConnectOptions, Connector, Item};

let mut connector = Connector::connect("localhost", 5123).await?;

connector.create_collection("orders", "id", &["customerId"]).await?;
let orders = (1..=1000).map(|id| Item::new(format!("order {id}").into_bytes(), [id, id % 50]));
connector.feed_collection("orders", "20261014_120000", orders).await?;

let documents: Vec<Vec<u8>> = connector.query_by_primary_key("orders", &[1, 2, 3]).await?;

let mut stream = connector.stream_by_secondary_index("orders", "customerId", 42).await?;
while let Some(document) = stream.next().await {
    let document = document?;
    println!("order {}: {} bytes", document.primary_key, document.data.len());
}

// TLS (the server must have ServerSettings:UseSsl enabled)
let options = ConnectOptions { use_tls: true, ..Default::default() };
let _secure = Connector::connect_with("cache-01", 5123, &options).await?;
```

| Operation | `Connector` method |
|---|---|
| Create a collection | `create_collection(collection, primary_key, other_indexes)` |
| Feed a new version | `feed_collection(collection, version, items)` |
| Query by primary key | `query_by_primary_key(collection, keys)` |
| Stream a collection | `stream_all_data(collection)` |
| Stream by secondary index | `stream_by_secondary_index(collection, index, key)` |
| List collections | `get_collections_description()` |
| Drop a collection | `drop_collection(collection, ignore_if_not_found)` |
| Check the server | `ping()`, `ping_with_timeout(duration)` |

- **One request at a time per connection, checked by the compiler.** Every operation takes `&mut self`, and a
  stream borrows the connector until it is dropped. Use one connector per concurrent operation, or the Aggregator.
- **A connection left in an unknown state is closed and refuses further requests** (`Error::Unusable`): an I/O
  error or timeout part way through a request, a ping timeout, or a stream dropped before its end. Closing it
  immediately also lets the server stop streaming and release the collection. Errors reported by the server leave
  the connection usable.
- **`Error::is_connection_failure()`** separates failures of the connection, where another replica may succeed,
  from errors reported by the server, which would occur on any replica.
- **TLS** uses rustls (pure Rust, no OpenSSL) and checks certificates against the operating system's trust store,
  like .NET's `SslStream`. `validate_server_certificate: false` disables all certificate checks, for self-signed
  test certificates only. The connect timeout (10 s by default) covers the TLS handshake: a TLS client on a server
  without TLS would otherwise wait forever.
- **Limits of the protocol:** a request body is limited to 1 MB by the server (about 100,000 keys in one query by
  primary key), and so is a response, by both clients. Version names must sort after the collection's current one;
  the Aggregators name them after the current time in seconds (UTC, `yyyyMMdd_HHmmss`), so two feeds of one
  collection within the same second collide.

## PerfTest

The same command line and output as the .NET PerfTest, so both can be run against the same servers and compared:

```text
perftest [feed|read|stream|secondary] [--server host:port | --servers host1:port1+host2:port2+...]
         [--collection name] [--size count] [--queries count] [--ssl]
```

```bash
cargo run --release -p perftest -- feed --server localhost:5123 --size 2000000
cargo run --release -p perftest -- secondary --servers cache-01:5123+cache-02:5123 --queries 1000
```

With `--servers`, every action goes through the Aggregator: `feed` feeds every replica, `read`, `stream` and
`secondary` spread across them, and each run starts by listing what every replica reports for the collection.
`secondary` goes through the Aggregator with a single server too, like the .NET PerfTest.

Measured on Windows against local Release servers, 2M items of 100 or 500 bytes:

| | .NET PerfTest | Rust PerfTest |
|---|---|---|
| Feed, one server | 1.4–1.9 s | 2.1–2.3 s |
| Feed, two replicas | 2.4–4.5 s | 2.2–2.6 s |
| Stream, one server | 0.49–0.64 s | 0.68–0.82 s |
| Read: 100 queries of 100 keys | 8–11 ms | 5–8 ms |
| Secondary key, sequential (two replicas) | 748 queries/s | 834 queries/s |
| Secondary key, 8 concurrent (two replicas) | 2,700–6,000 queries/s (one run: 525) | 3,043 queries/s |

Queries are as fast or faster, and so are cluster feeds, where each batch is encoded once for all replicas.
Single-server bulk transfers are about 1.3 times slower: the client code isn't the bottleneck (encoding 2M items
takes about 0.5 s, overlapped with the writes), and a bare tokio socket writing pre-encoded batches is in the same
range, so the difference is likely in tokio's readiness-based socket I/O on Windows compared with .NET's
completion-based sends. Linux has not been measured.

## Tests

```bash
cargo test                                                          # unit tests; server tests are skipped
FROZENCACHE_TEST_SERVER=localhost:5123 cargo test                   # + Connector against a server
FROZENCACHE_TEST_SERVER=localhost:5124 FROZENCACHE_TEST_TLS=1 cargo test         # ... with UseSsl=true
FROZENCACHE_TEST_SERVERS=localhost:5123+localhost:5125 cargo test   # + Aggregator against two servers
```

- **Protocol tests** compare every message and the feed batch format byte for byte with encodings produced by the
  .NET `Messages` code. When the .NET protocol changes, regenerate those vectors from .NET and update
  `frozencache-client/src/protocol.rs` and its tests.
- **Connector tests** (`tests/dotnet_server.rs`) run against a real .NET server: the full lifecycle, server
  errors, an abandoned stream, and a TLS mismatch in both directions.
- **Aggregator tests** (`tests/aggregator.rs`) run against two real .NET servers: feeds and typed reads, a replica
  that is down, failover (with a fake replica that drops its connections), abandoned streams, server errors, the
  local cache and version notifications, and a feed failing on one replica being aborted on all.
- The server tests use their own collections and run one at a time, because of a server race between a feed and a
  drop on different collections (described in `tests/dotnet_server.rs`).
