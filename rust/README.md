# FrozenCache Rust client

A Rust client for the FrozenCache server, wire-compatible with the .NET server and the .NET `CacheClient`.
It works with the existing server as is: the same server, the same collections, the same protocol.

| Crate | What it is | .NET counterpart |
|---|---|---|
| [`frozencache-client`](frozencache-client) | Client library: `Connector` | `CacheClient.Connector` (+ `Messages`) |
| [`perftest`](perftest) | Load/perf test tool | `PerfTest` |

The port is in progress: `Connector` and PerfTest are done; `ConnectorPool` and `Aggregator` (connection pooling,
replicas, typed collections, local cache) come next.

## Usage

```toml
[dependencies]
frozencache-client = { path = "path/to/FrozenCache/rust/frozencache-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use frozencache_client::{ConnectOptions, Connector, Item};

#[tokio::main]
async fn main() -> frozencache_client::Result<()> {
    let mut connector = Connector::connect("localhost", 5123).await?;

    // primary key "id", secondary index "customerId"
    connector.create_collection("orders", "id", &["customerId"]).await?;

    // items: raw bytes + keys (primary key first, then the secondary keys in declaration order)
    let orders = (1..=1000).map(|id| Item::new(format!("order {id}").into_bytes(), [id, id % 50]));
    connector.feed_collection("orders", "20261014-120000", orders).await?;

    // by primary key
    let documents: Vec<Vec<u8>> = connector.query_by_primary_key("orders", &[1, 2, 3]).await?;

    // by secondary index, streamed in batches
    let mut stream = connector.stream_by_secondary_index("orders", "customerId", 42).await?;
    while let Some(document) = stream.next().await {
        let document = document?;
        println!("order {}: {} bytes", document.primary_key, document.data.len());
    }

    // TLS (the server must have ServerSettings:UseSsl enabled)
    let options = ConnectOptions { use_tls: true, ..Default::default() };
    let _secure = Connector::connect_with("cache-01", 5123, &options).await?;
    Ok(())
}
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

### Behavior worth knowing

- **One request at a time per connection, checked by the compiler.** Every operation takes `&mut self`, and a
  stream borrows the connector until it is dropped. Use one connector per concurrent operation.
- **A connection left in an unknown state is closed and refuses further requests** (`Error::Unusable`): an I/O
  error or timeout part way through a request, a ping timeout, or a stream dropped before its end. Closing it
  immediately also lets the server stop streaming and release the collection. `is_healthy()` tells whether a
  connector can still be used; errors reported by the server (unknown collection, version conflict...) leave it
  usable.
- **`Error::is_connection_failure()`** separates failures of the connection, where retrying on another replica can
  help, from errors reported by the server, which would occur on any replica.
- **TLS** uses rustls (pure Rust, no OpenSSL) and checks certificates against the operating system's trust store,
  like .NET's `SslStream`. `validate_server_certificate: false` disables all certificate checks, for self-signed
  test certificates only. The connect timeout (10 s by default) covers the TLS handshake: a TLS client on a
  server without TLS would otherwise wait forever.
- **Limits of the protocol:** a request body is limited to 1 MB by the server (about 100,000 keys in one
  `query_by_primary_key`), and so is a response by the .NET client and this one. Version names must sort after
  the collection's current one (ordinal string comparison).
- **Feeds are pipelined:** batches are built while the previous one is written, by a separate task.

## PerfTest

The same command line and output as the .NET PerfTest, so both can be run against the same server and compared
(except `--servers`, the .NET PerfTest's cluster mode, which needs the Aggregator):

```text
perftest [feed|read|stream|secondary] [--server host:port] [--collection name] [--size count] [--queries count] [--ssl]
```

```bash
cargo run --release -p perftest -- feed --server localhost:5123 --collection big --size 2000000
cargo run --release -p perftest -- secondary --server localhost:5123 --collection big --queries 1000
```

The .NET PerfTest's `secondary` goes through the Aggregator; until it is ported, the Rust one uses connectors
directly (one for the sequential run, eight for the concurrent one).

Measured on Windows against the same local Release server, 2M items of 100 or 500 bytes:

| | .NET PerfTest | Rust PerfTest |
|---|---|---|
| Feed 2M items | 1.4–1.9 s | 2.1–2.3 s |
| Stream 2M items | 0.49–0.64 s | 0.68–0.82 s |
| Read: 100 queries of 100 keys | 8–11 ms | 5–6 ms |
| Secondary key, sequential | 722 queries/s | 793 queries/s |
| Secondary key, 8 concurrent | 2,737 queries/s | 3,243 queries/s |

Queries are as fast or faster. Bulk transfers are about 1.3 times slower: the client code is not the bottleneck
(encoding 2M items takes about 0.5 s, overlapped with the writes), and a bare tokio socket writing pre-encoded
batches is in the same range, so the difference is likely in tokio's readiness-based socket I/O on Windows
compared with .NET's completion-based sends. Linux has not been measured.

## Tests

```bash
cargo test                                                     # protocol tests; server tests are skipped
FROZENCACHE_TEST_SERVER=localhost:5123 cargo test              # + end-to-end tests against a running server
FROZENCACHE_TEST_SERVER=localhost:5123 FROZENCACHE_TEST_TLS=1 cargo test   # server with UseSsl=true
```

- **Protocol tests** compare every message and the feed batch format byte for byte with encodings produced by the
  .NET `Messages` code. When the .NET protocol changes, regenerate those vectors from .NET and update
  `frozencache-client/src/protocol.rs` and its tests.
- **End-to-end tests** (`frozencache-client/tests/dotnet_server.rs`) run against a real .NET server: the full
  lifecycle, server errors, an abandoned stream, and a TLS mismatch in both directions. They use their own
  collections, and run one at a time because of a server race between a feed and a drop on different collections
  (described in that file).
