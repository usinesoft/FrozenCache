//! Load/perf test for a running FrozenCache server or cluster: the Rust counterpart of the .NET `PerfTest`, with
//! the same command line, data and output, so that both clients can be compared against the same servers.

use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use frozencache_client::{
    Aggregator, AggregatorOptions, CollectionInformation, ConnectOptions, Connector, Document,
    Error, Item, Result,
};

const DEFAULT_SERVER: &str = "localhost:5123";
const DEFAULT_PORT: u16 = 5123;

/// Secondary index of the collections fed by this tool. Historical name, shared with the .NET PerfTest so that
/// both tools work on the same collections: it holds the item's group (id % SECONDARY_KEY_GROUPS).
const SECONDARY_INDEX: &str = "name";
const SECONDARY_KEY_GROUPS: i64 = 1000;

/// Concurrent queries in the parallel secondary-key run, and connections per server.
const CONCURRENCY: usize = 8;

#[derive(Parser, Debug)]
#[command(
    name = "perftest",
    version,
    about = "Load/perf test for a FrozenCache server or cluster"
)]
struct Args {
    /// feed: create the collection (if needed) and feed a new version named after the current time
    /// (UTC, yyyyMMdd_HHmmss); read: query random primary keys in batches of 1, 5, 10 and 100; stream: stream the
    /// whole collection; secondary: query random secondary key values through the Aggregator, sequentially then 8
    /// at a time
    #[arg(value_enum, default_value_t = Action::Read)]
    action: Action,

    /// Server to connect to, host:port [default: localhost:5123]
    #[arg(long, conflicts_with = "servers")]
    server: Option<String>,

    /// A cluster of replicas, host1:port1+host2:port2+..., used through the Aggregator: feeds go to every
    /// replica, queries and streams are spread across them
    #[arg(long)]
    servers: Option<String>,

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

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq)]
enum Action {
    Feed,
    Read,
    Stream,
    Secondary,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    // certificate validation is skipped: this tool is meant to run against local/dev servers, typically using a
    // self-signed certificate
    let options = ConnectOptions {
        use_tls: args.ssl,
        validate_server_certificate: false,
        ..Default::default()
    };

    let result = match &args.servers {
        Some(servers) => {
            let Some(cluster) = parse_servers(servers) else {
                eprintln!("Invalid servers '{servers}', expected host1:port1+host2:port2+...");
                return ExitCode::FAILURE;
            };
            println!(
                "Action:{:?}, Servers:{}, Collection:{}, Ssl:{}",
                args.action,
                cluster
                    .iter()
                    .map(|(h, p)| format!("{h}:{p}"))
                    .collect::<Vec<_>>()
                    .join("+"),
                args.collection,
                args.ssl
            );
            run_on_cluster(&args, &cluster, options).await
        }
        None => {
            let server = args.server.as_deref().unwrap_or(DEFAULT_SERVER);
            let Some((host, port)) = parse_server(server) else {
                eprintln!("Invalid server '{server}', expected host:port");
                return ExitCode::FAILURE;
            };
            println!(
                "Action:{:?}, Server:{host}:{port}, Collection:{}, Ssl:{}",
                args.action, args.collection, args.ssl
            );
            run_on_server(&args, &host, port, options).await
        }
    };

    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
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

/// `host1:port1+host2:port2+...`: at least one, no duplicates.
fn parse_servers(servers: &str) -> Option<Vec<(String, u16)>> {
    let cluster: Vec<(String, u16)> = servers
        .split('+')
        .map(|s| parse_server(s.trim()))
        .collect::<Option<_>>()?;
    let mut unique = cluster.clone();
    unique.sort();
    unique.dedup();
    (!cluster.is_empty() && unique.len() == cluster.len()).then_some(cluster)
}

// --- one server, through a Connector (secondary-key queries through an Aggregator, as in the .NET PerfTest) ---

async fn run_on_server(
    args: &Args,
    host: &str,
    port: u16,
    options: ConnectOptions,
) -> Result<bool> {
    let mut connector = match Connector::connect_with(host, port, &options).await {
        Ok(connector) => connector,
        Err(e) => {
            eprintln!("Failed to connect to server: {e}");
            return Ok(false);
        }
    };
    println!("Connected to server");

    match args.action {
        Action::Feed => {
            connector
                .create_collection(&args.collection, "id", &[SECONDARY_INDEX])
                .await?;

            // a time-based version is unique per run, so repeated feeds never collide with an existing version;
            // UTC and "_", like the Aggregators: one collection can be fed both ways, and the server orders
            // versions by name
            let version = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
            println!(
                "Feeding {} items into '{}' as version {version}",
                args.size, args.collection
            );

            let watch = Instant::now();
            connector
                .feed_collection(&args.collection, &version, items(args.size))
                .await?;
            println!(
                "Feeding {} items items took {} ms",
                args.size,
                watch.elapsed().as_millis()
            );
        }
        Action::Stream => {
            let watch = Instant::now();
            let mut progress = Progress::default();
            let mut documents = connector.stream_all_data(&args.collection).await?;
            while let Some(document) = documents.next().await {
                progress.add(document?);
            }
            progress.finish(watch);
        }
        Action::Read | Action::Secondary => {
            let Some(info) = non_empty_collection(&mut connector, &args.collection).await? else {
                return Ok(true);
            };
            if args.action == Action::Read {
                read(Target::Server(&mut connector), &args.collection, &info).await?;
            } else {
                let aggregator =
                    Aggregator::connect(&[(host, port)], aggregator_options(options)).await?;
                secondary(&aggregator, &args.collection, &info, args.queries as usize).await?;
            }
        }
    }
    Ok(true)
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

// --- a cluster, through the Aggregator ---

fn aggregator_options(connect: ConnectOptions) -> AggregatorOptions {
    AggregatorOptions {
        pool_capacity: CONCURRENCY,
        connect,
        ..Default::default()
    }
}

async fn run_on_cluster(
    args: &Args,
    cluster: &[(String, u16)],
    options: ConnectOptions,
) -> Result<bool> {
    let aggregator = Aggregator::connect(cluster, aggregator_options(options)).await?;

    if args.action == Action::Feed {
        let connected = aggregator.replicas().iter().filter(|r| r.connected).count();
        if connected == 0 {
            println!("Failed to connect to any server");
            return Ok(false);
        }

        aggregator
            .declare_collection(&args.collection, "id", &[SECONDARY_INDEX])
            .await?;
        println!(
            "Feeding {} items into '{}' on {connected} of {} servers",
            args.size,
            args.collection,
            cluster.len()
        );

        let watch = Instant::now();
        aggregator
            .feed_items(&args.collection, items(args.size))
            .await?;
        println!(
            "Feeding {} items items took {} ms",
            args.size,
            watch.elapsed().as_millis()
        );

        describe_cluster(&aggregator, &args.collection).await;
        return Ok(true);
    }

    let Some(info) = describe_cluster(&aggregator, &args.collection).await else {
        return Ok(false);
    };

    match args.action {
        Action::Read => read(Target::Cluster(&aggregator), &args.collection, &info).await?,
        Action::Secondary => {
            secondary(&aggregator, &args.collection, &info, args.queries as usize).await?
        }
        Action::Stream => {
            // from one replica, like any client
            let watch = Instant::now();
            let mut progress = Progress::default();
            let mut documents = aggregator.stream_all_data(&args.collection);
            while let Some(document) = documents.next().await {
                progress.add(document?);
            }
            progress.finish(watch);
        }
        Action::Feed => unreachable!(),
    }
    Ok(true)
}

/// Prints what every replica reports for the collection, and returns the description to test against: `None`
/// (with a message) if no replica is connected, or the collection is missing or empty.
async fn describe_cluster(
    aggregator: &Aggregator,
    collection: &str,
) -> Option<CollectionInformation> {
    let descriptions = aggregator.get_collections_description().await;
    let replicas = aggregator.replicas();

    let mut reference: Option<CollectionInformation> = None;
    let (mut connected, mut in_sync) = (0, true);

    for (replica, description) in replicas.iter().zip(descriptions) {
        let Some(description) = description else {
            println!("  {}: not connected", replica.address);
            continue;
        };
        connected += 1;

        match description
            .collections
            .into_iter()
            .find(|c| c.name == collection)
        {
            None => {
                println!("  {}: no collection '{collection}'", replica.address);
                in_sync = false;
            }
            Some(info) => {
                println!(
                    "  {}: version {}, {} items",
                    replica.address,
                    info.last_version,
                    grouped(info.count as u64)
                );
                match &reference {
                    None => reference = Some(info),
                    Some(first)
                        if first.last_version != info.last_version || first.count != info.count =>
                    {
                        in_sync = false
                    }
                    Some(_) => {}
                }
            }
        }
    }

    if connected == 0 {
        println!("Failed to connect to any server");
        return None;
    }
    if connected < replicas.len() {
        println!(
            "Warning: {} of {} servers are not connected",
            replicas.len() - connected,
            replicas.len()
        );
    }
    if !in_sync {
        println!("Warning: the replicas don't have the same version of the collection");
    }

    match reference {
        Some(info) if info.count > 0 => Some(info),
        _ => {
            println!("Collection '{collection}' not found or empty");
            None
        }
    }
}

// --- the benchmarks ---

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

/// Where primary-key queries go: one server, or a cluster.
enum Target<'a> {
    Server(&'a mut Connector),
    Cluster(&'a Aggregator),
}

impl Target<'_> {
    async fn query(&mut self, collection: &str, keys: &[i64]) -> Result<Vec<Vec<u8>>> {
        match self {
            Target::Server(connector) => connector.query_by_primary_key(collection, keys).await,
            Target::Cluster(aggregator) => {
                aggregator.query_raw_by_primary_key(collection, keys).await
            }
        }
    }
}

async fn read(
    mut target: Target<'_>,
    collection: &str,
    info: &CollectionInformation,
) -> Result<()> {
    // items are fed with primary keys 0..count-1, so the collection size gives the key range to query
    let max_id = info.count as u64;

    for (batch_size, label) in [
        (1, "Reading 100 objects one by one"),
        (5, "Reading 100 times 5 objects"),
        (10, "Reading 100 times 10 objects"),
        (100, "Reading 100 times 100 objects"),
    ] {
        query_by_batch(&mut target, collection, batch_size, 100, max_id).await?; // warm-up
        let watch = Instant::now();
        query_by_batch(&mut target, collection, batch_size, 100, max_id).await?;
        println!("{label} took {} ms", watch.elapsed().as_millis());
    }
    Ok(())
}

/// The same random keys, queried `iterations` times.
async fn query_by_batch(
    target: &mut Target<'_>,
    collection: &str,
    batch_size: usize,
    iterations: usize,
    max_id: u64,
) -> Result<()> {
    let mut rng = Rng::from_time();
    let ids: Vec<i64> = (0..batch_size).map(|_| rng.below(max_id) as i64).collect();

    for _ in 0..iterations {
        let result = target.query(collection, &ids).await?;
        if result.len() != batch_size {
            return Err(Error::Protocol(format!(
                "Expected {batch_size} items, but got {} items",
                result.len()
            )));
        }
    }
    Ok(())
}

/// Counts streamed documents, with a dot every 100,000.
#[derive(Default)]
struct Progress {
    count: u64,
}

impl Progress {
    fn add(&mut self, _document: Document) {
        self.count += 1;
        if self.count.is_multiple_of(100_000) {
            // stdout is line-buffered: without a flush, the dots would only appear with the final newline
            print!(".");
            let _ = std::io::stdout().flush();
        }
    }

    fn finish(&self, started: Instant) {
        println!();
        println!(
            "Streamed {} items in {} ms",
            self.count,
            started.elapsed().as_millis()
        );
    }
}

async fn secondary(
    aggregator: &Aggregator,
    collection: &str,
    info: &CollectionInformation,
    queries: usize,
) -> Result<()> {
    // items are fed with secondary key id % SECONDARY_KEY_GROUPS, so these are the values that exist
    let groups = SECONDARY_KEY_GROUPS.min(info.count as i64);
    println!(
        "Each '{SECONDARY_INDEX}' value matches about {} items",
        grouped((info.count as i64 / groups) as u64)
    );

    run_secondary(aggregator, collection, groups, (queries / 10).max(1), 1).await?; // warm-up

    let sequential = run_secondary(aggregator, collection, groups, queries, 1).await?;
    sequential.print("sequential");

    let parallel = run_secondary(aggregator, collection, groups, queries, CONCURRENCY).await?;
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

/// `queries` random secondary-key queries through the Aggregator, `parallelism` at a time.
async fn run_secondary(
    aggregator: &Aggregator,
    collection: &str,
    groups: i64,
    queries: usize,
    parallelism: usize,
) -> Result<SecondaryRun> {
    let next_query = Arc::new(AtomicUsize::new(0));
    let watch = Instant::now();

    let tasks: Vec<_> = (0..parallelism)
        .map(|_| {
            let (aggregator, next_query, collection) = (
                aggregator.clone(),
                next_query.clone(),
                collection.to_owned(),
            );
            tokio::spawn(async move {
                let mut rng = Rng::from_time();
                let (mut items, mut bytes, mut latencies) = (0u64, 0u64, vec![]);

                while next_query.fetch_add(1, Ordering::Relaxed) < queries {
                    let start = Instant::now();
                    let key = rng.below(groups as u64) as i64;
                    let mut documents =
                        aggregator.stream_by_secondary_key(&collection, SECONDARY_INDEX, key);
                    while let Some(document) = documents.next().await {
                        items += 1;
                        bytes += document?.data.len() as u64;
                    }
                    latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                }
                Ok::<_, Error>((items, bytes, latencies))
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
    let mut outcome = Ok(());
    for task in tasks {
        match task.await.expect("query task panicked") {
            Ok((items, bytes, latencies)) => {
                run.items += items;
                run.bytes += bytes;
                run.latencies_ms.extend(latencies);
            }
            Err(e) => outcome = outcome.and(Err(e)),
        }
    }
    run.elapsed = watch.elapsed();
    outcome.map(|()| run)
}

#[cfg(test)]
mod tests {
    use super::{grouped, parse_server, parse_servers};

    #[test]
    fn digit_grouping() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(1_234_567), "1,234,567");
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

    #[test]
    fn cluster_addresses() {
        assert_eq!(
            parse_servers("a:5123+b:5124"),
            Some(vec![("a".into(), 5123), ("b".into(), 5124)])
        );
        assert_eq!(parse_servers("a:5123"), Some(vec![("a".into(), 5123)]));
        assert_eq!(parse_servers("a:5123+"), None, "empty part");
        assert_eq!(parse_servers("a:5123+a:5123"), None, "duplicate");
        assert_eq!(parse_servers("a:abc"), None);
    }
}
