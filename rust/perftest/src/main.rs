//! Load/perf test for a running FrozenCache server: the Rust counterpart of the .NET `PerfTest`, with the same
//! command line, data and output, so that both clients can be compared against the same server.

use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use frozencache_client::{CollectionInformation, ConnectOptions, Connector, Item, Result};

const DEFAULT_PORT: u16 = 5123;

/// Secondary index of the collections fed by this tool. Historical name, shared with the .NET PerfTest so that
/// both tools work on the same collections: it holds the item's group (id % SECONDARY_KEY_GROUPS).
const SECONDARY_INDEX: &str = "name";
const SECONDARY_KEY_GROUPS: i64 = 1000;

/// Concurrent connections in the parallel secondary-key run.
const CONCURRENCY: usize = 8;

#[derive(Parser, Debug)]
#[command(
    name = "perftest",
    version,
    about = "Load/perf test for a FrozenCache server"
)]
struct Args {
    /// feed: create the collection (if needed) and feed a new version named after the current time
    /// (UTC, yyyyMMdd_HHmmss); read: query random primary keys in batches of 1, 5, 10 and 100; stream: stream the
    /// whole collection; secondary: query random secondary key values, sequentially then 8 at a time
    #[arg(value_enum, default_value_t = Action::Read)]
    action: Action,

    /// Server to connect to, host:port
    #[arg(long, default_value = "localhost:5123")]
    server: String,

    /// Collection name
    #[arg(long, default_value = "big")]
    collection: String,

    /// Number of items to feed (feed only)
    #[arg(long, default_value_t = 2_000_000, value_parser = clap::value_parser!(u32).range(1..))]
    size: u32,

    /// Number of queries per run (secondary only)
    #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u32).range(1..))]
    queries: u32,

    /// Connect over TLS (the server certificate is not validated)
    #[arg(long)]
    ssl: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Action {
    Feed,
    Read,
    Stream,
    Secondary,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    let Some((host, port)) = parse_server(&args.server) else {
        eprintln!("Invalid server '{}', expected host:port", args.server);
        return ExitCode::FAILURE;
    };

    println!(
        "Action:{:?}, Server:{host}:{port}, Collection:{}, Ssl:{}",
        args.action, args.collection, args.ssl
    );

    // certificate validation is skipped: this tool is meant to run against a local/dev server, typically using
    // a self-signed certificate
    let options = ConnectOptions {
        use_tls: args.ssl,
        validate_server_certificate: false,
        ..Default::default()
    };

    let mut connector = match Connector::connect_with(&host, port, &options).await {
        Ok(connector) => connector,
        Err(e) => {
            eprintln!("Failed to connect to server: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("Connected to server");

    let result = match args.action {
        Action::Feed => feed(&mut connector, &args.collection, args.size).await,
        Action::Read => read(&mut connector, &args.collection).await,
        Action::Stream => stream(&mut connector, &args.collection).await,
        Action::Secondary => {
            secondary(
                &mut connector,
                &host,
                port,
                &options,
                &args.collection,
                args.queries as usize,
            )
            .await
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

/// `host:port`, or `host` alone (default port); the last ':' separates the port, so that bracketed IPv6
/// literals like `[::1]:5123` work too.
fn parse_server(server: &str) -> Option<(String, u16)> {
    let (host, port) = match server.rfind(':') {
        Some(0) => return None, // no host
        Some(separator) if !server[separator..].contains(']') => {
            let port: u16 = server[separator + 1..].parse().ok().filter(|p| *p > 0)?;
            (&server[..separator], port)
        }
        _ => (server, DEFAULT_PORT),
    };
    let host = host.trim_matches(['[', ']']);
    (!host.is_empty()).then(|| (host.to_owned(), port))
}

/// Small, fast, deterministic pseudo-random generator (xorshift64*): enough for test data and random keys.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }

    fn from_time() -> Self {
        Rng::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
        )
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// In [0, bound).
    fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let value = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&value[..chunk.len()]);
        }
    }
}

/// The same items as the .NET PerfTest: alternately 100 and 500 random bytes, keyed by id and id % 1000.
fn items(count: u32) -> impl Iterator<Item = Item> {
    let mut rng = Rng::from_time();
    (0..count as i64).map(move |i| {
        let mut data = vec![0u8; if i % 2 == 0 { 100 } else { 500 }];
        rng.fill(&mut data);
        Item::new(data, [i, i % SECONDARY_KEY_GROUPS])
    })
}

async fn feed(connector: &mut Connector, collection: &str, count: u32) -> Result<()> {
    connector
        .create_collection(collection, "id", &[SECONDARY_INDEX])
        .await?;

    // a time-based version is unique per run, so repeated feeds never collide with an existing version
    // UTC and "_", like the .NET Aggregator: one collection can be fed by both, and the server orders versions by name
    let version = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    println!("Feeding {count} items into '{collection}' as version {version}");

    let watch = Instant::now();
    connector
        .feed_collection(collection, &version, items(count))
        .await?;
    println!(
        "Feeding {count} items items took {} ms",
        watch.elapsed().as_millis()
    );
    Ok(())
}

/// The collection's description, or `None` (with a message) if it doesn't exist or is empty.
async fn non_empty_collection(
    connector: &mut Connector,
    collection: &str,
) -> Result<Option<CollectionInformation>> {
    let description = connector.get_collections_description().await?;
    match description
        .collections
        .into_iter()
        .find(|c| c.name == collection)
    {
        Some(info) if info.count > 0 => {
            println!(
                "Collection '{collection}' version {} contains {} items",
                info.last_version, info.count
            );
            Ok(Some(info))
        }
        _ => {
            println!("Collection '{collection}' not found or empty");
            Ok(None)
        }
    }
}

async fn read(connector: &mut Connector, collection: &str) -> Result<()> {
    // items are fed with primary keys 0..count-1, so the collection size gives the key range to query
    let Some(info) = non_empty_collection(connector, collection).await? else {
        return Ok(());
    };
    let max_id = info.count as u64;

    for (batch_size, label) in [
        (1, "Reading 100 objects one by one"),
        (5, "Reading 100 times 5 objects"),
        (10, "Reading 100 times 10 objects"),
        (100, "Reading 100 times 100 objects"),
    ] {
        query_by_batch(connector, collection, batch_size, 100, max_id).await?; // warm-up
        let watch = Instant::now();
        query_by_batch(connector, collection, batch_size, 100, max_id).await?;
        println!("{label} took {} ms", watch.elapsed().as_millis());
    }
    Ok(())
}

/// The same random keys, queried `iterations` times.
async fn query_by_batch(
    connector: &mut Connector,
    collection: &str,
    batch_size: usize,
    iterations: usize,
    max_id: u64,
) -> Result<()> {
    let mut rng = Rng::from_time();
    let ids: Vec<i64> = (0..batch_size).map(|_| rng.below(max_id) as i64).collect();

    for _ in 0..iterations {
        let result = connector.query_by_primary_key(collection, &ids).await?;
        if result.len() != batch_size {
            return Err(frozencache_client::Error::Protocol(format!(
                "Expected {batch_size} items, but got {} items",
                result.len()
            )));
        }
    }
    Ok(())
}

async fn stream(connector: &mut Connector, collection: &str) -> Result<()> {
    let watch = Instant::now();
    let mut count: u64 = 0;

    let mut documents = connector.stream_all_data(collection).await?;
    while let Some(document) = documents.next().await {
        document?;
        count += 1;
        if count.is_multiple_of(100_000) {
            // stdout is line-buffered: without a flush, the dots would only appear with the final newline
            print!(".");
            let _ = std::io::stdout().flush();
        }
    }

    println!();
    println!(
        "Streamed {count} items in {} ms",
        watch.elapsed().as_millis()
    );
    Ok(())
}

async fn secondary(
    connector: &mut Connector,
    host: &str,
    port: u16,
    options: &ConnectOptions,
    collection: &str,
    queries: usize,
) -> Result<()> {
    let Some(info) = non_empty_collection(connector, collection).await? else {
        return Ok(());
    };

    // items are fed with secondary key id % SECONDARY_KEY_GROUPS, so these are the values that exist
    let groups = SECONDARY_KEY_GROUPS.min(info.count as i64);
    println!(
        "Each '{SECONDARY_INDEX}' value matches about {} items",
        info.count as i64 / groups
    );

    // the .NET PerfTest goes through the Aggregator; until it is ported, the runs use connectors directly: one
    // for the sequential run, CONCURRENCY for the parallel one
    let mut pool = vec![];
    for _ in 0..CONCURRENCY {
        pool.push(Connector::connect_with(host, port, options).await?);
    }

    let single = vec![pool.pop().expect("CONCURRENCY >= 1")];
    let (single, _) = run_secondary(single, collection, groups, (queries / 10).max(1)).await?; // warm-up

    let (single, sequential) = run_secondary(single, collection, groups, queries).await?;
    sequential.print("sequential");

    pool.extend(single);
    let (_, parallel) = run_secondary(pool, collection, groups, queries).await?;
    parallel.print(&format!("{CONCURRENCY} concurrent"));

    if sequential.items == 0 {
        println!(
            "No item matched: this collection was probably fed by an older PerfTest, which used a unique \
             '{SECONDARY_INDEX}' per item. Feed it again."
        );
    }
    Ok(())
}

/// 1234567 -> "1,234,567", like the .NET PerfTest's N0 format.
fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

struct SecondaryRun {
    queries: usize,
    items: u64,
    bytes: u64,
    elapsed: Duration,
    latencies_ms: Vec<f64>,
}

impl SecondaryRun {
    fn print(&self, label: &str) {
        let mut latencies = self.latencies_ms.clone();
        latencies.sort_by(f64::total_cmp);
        let percentile =
            |p: f64| latencies[((p * latencies.len() as f64).ceil() as usize).max(1) - 1];

        let seconds = self.elapsed.as_secs_f64();
        println!(
            "Secondary key, {label}: {} queries, {} items, {:.1} MB in {} ms",
            self.queries,
            grouped(self.items),
            self.bytes as f64 / 1e6,
            self.elapsed.as_millis()
        );
        println!(
            "  latency per query: p50 {:.2} ms, p95 {:.2} ms, max {:.2} ms",
            percentile(0.5),
            percentile(0.95),
            latencies.last().copied().unwrap_or_default()
        );
        println!(
            "  throughput: {} queries/s, {} items/s, {:.1} MB/s",
            grouped((self.queries as f64 / seconds) as u64),
            grouped((self.items as f64 / seconds) as u64),
            self.bytes as f64 / 1e6 / seconds
        );
    }
}

/// `queries` random secondary-key queries, shared between the connectors (one task each), which are handed back.
async fn run_secondary(
    connectors: Vec<Connector>,
    collection: &str,
    groups: i64,
    queries: usize,
) -> Result<(Vec<Connector>, SecondaryRun)> {
    let next_query = Arc::new(AtomicUsize::new(0));
    let watch = Instant::now();

    let tasks: Vec<_> = connectors
        .into_iter()
        .map(|mut connector| {
            let next_query = next_query.clone();
            let collection = collection.to_owned();
            tokio::spawn(async move {
                let mut rng = Rng::from_time();
                let (mut items, mut bytes, mut latencies) = (0u64, 0u64, vec![]);
                let mut outcome = Ok(());

                while next_query.fetch_add(1, Ordering::Relaxed) < queries {
                    let start = Instant::now();
                    let key = rng.below(groups as u64) as i64;
                    let result: Result<()> = async {
                        let mut documents = connector
                            .stream_by_secondary_index(&collection, SECONDARY_INDEX, key)
                            .await?;
                        while let Some(document) = documents.next().await {
                            items += 1;
                            bytes += document?.data.len() as u64;
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(e) = result {
                        outcome = Err(e);
                        break;
                    }
                    latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                }
                (connector, outcome, items, bytes, latencies)
            })
        })
        .collect();

    let mut run = SecondaryRun {
        queries,
        items: 0,
        bytes: 0,
        elapsed: Duration::ZERO,
        latencies_ms: vec![],
    };
    let mut connectors = Vec::with_capacity(tasks.len());
    let mut outcome = Ok(());
    for task in tasks {
        let (connector, result, items, bytes, latencies) = task.await.expect("query task panicked");
        connectors.push(connector);
        outcome = outcome.and(result);
        run.items += items;
        run.bytes += bytes;
        run.latencies_ms.extend(latencies);
    }
    run.elapsed = watch.elapsed();
    outcome.map(|()| (connectors, run))
}

#[cfg(test)]
mod tests {
    use super::parse_server;

    #[test]
    fn digit_grouping() {
        assert_eq!(super::grouped(0), "0");
        assert_eq!(super::grouped(999), "999");
        assert_eq!(super::grouped(1_000), "1,000");
        assert_eq!(super::grouped(1_234_567), "1,234,567");
    }

    #[test]
    fn server_addresses() {
        assert_eq!(
            parse_server("localhost:5123"),
            Some(("localhost".into(), 5123))
        );
        assert_eq!(parse_server("cache-01"), Some(("cache-01".into(), 5123)));
        assert_eq!(parse_server("[::1]:6000"), Some(("::1".into(), 6000)));
        assert_eq!(parse_server("[::1]"), Some(("::1".into(), 5123)));
        assert_eq!(
            parse_server("10.0.0.5:7000"),
            Some(("10.0.0.5".into(), 7000))
        );
        assert_eq!(parse_server("localhost:notaport"), None);
        assert_eq!(parse_server(":5123"), None);
    }
}
