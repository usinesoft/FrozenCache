//! End-to-end tests of the Aggregator against two running .NET FrozenCache servers. Skipped unless
//! `FROZENCACHE_TEST_SERVERS` lists them:
//!
//! ```text
//! FROZENCACHE_TEST_SERVERS=localhost:5199+localhost:5197 cargo test
//! ```
//!
//! The tests run one at a time, for the reason given in `dotnet_server.rs`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use frozencache_client::protocol::{HEADER_SIZE, Message, MessageType};
use frozencache_client::{Aggregator, AggregatorOptions, CacheObject, Connector, Error, Item};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, MutexGuard};

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::const_new(());
    LOCK.lock().await
}

/// The two servers, or `None` (test skipped).
fn servers() -> Option<Vec<(String, u16)>> {
    let Ok(list) = std::env::var("FROZENCACHE_TEST_SERVERS") else {
        eprintln!("skipped: FROZENCACHE_TEST_SERVERS is not set");
        return None;
    };
    let servers: Vec<(String, u16)> = list
        .split('+')
        .map(|server| {
            let (host, port) = server.rsplit_once(':').expect("host:port");
            (host.to_owned(), port.parse().expect("port"))
        })
        .collect();
    assert_eq!(
        servers.len(),
        2,
        "FROZENCACHE_TEST_SERVERS must list two servers"
    );
    Some(servers)
}

fn options() -> AggregatorOptions {
    AggregatorOptions {
        pool_capacity: 2,
        // effectively no watchdog: only the code under test changes the replicas' state
        watchdog_interval: Duration::from_secs(600),
        ..Default::default()
    }
}

fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rust_aggregator_{name}_{nanos}")
}

/// A port nothing listens on.
async fn dead_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[derive(Debug, Clone, PartialEq)]
struct Product {
    id: i64,
    category: i64,
    payload: Vec<u8>,
}

impl Product {
    fn new(id: i64) -> Self {
        Product {
            id,
            category: id % 10,
            payload: vec![(id % 256) as u8; 300],
        }
    }
}

impl CacheObject for Product {
    fn keys(&self) -> Vec<i64> {
        vec![self.id, self.category]
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16 + self.payload.len());
        bytes.extend_from_slice(&self.id.to_le_bytes());
        bytes.extend_from_slice(&self.category.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Product {
            id: i64::from_le_bytes(bytes.get(0..8).ok_or("too short")?.try_into()?),
            category: i64::from_le_bytes(bytes.get(8..16).ok_or("too short")?.try_into()?),
            payload: bytes[16..].to_vec(),
        })
    }
}

/// A collection with keys (id, category) on every server.
async fn declared(aggregator: &Aggregator, name: &str) {
    aggregator
        .declare_collection(name, "id", &["category"])
        .await
        .unwrap();
}

#[tokio::test]
async fn feeds_every_replica_and_spreads_reads() {
    let _serial = serial().await;
    let Some(servers) = servers() else { return };
    let aggregator = Aggregator::connect(&servers, options()).await.unwrap();
    let name = unique("feed");
    declared(&aggregator, &name).await;

    let version = aggregator
        .feed_collection(&name, (0..12_000).map(Product::new))
        .await
        .unwrap();
    assert_eq!(version.len(), "yyyyMMdd_HHmmss".len());
    assert_eq!(
        aggregator.last_version(&name).as_deref(),
        Some(version.as_str())
    );

    // every replica has the same version
    for description in aggregator.get_collections_description().await {
        let info = description
            .unwrap()
            .collections
            .into_iter()
            .find(|c| c.name == name)
            .unwrap();
        assert_eq!(
            (info.last_version.as_str(), info.count),
            (version.as_str(), 12_000)
        );
    }

    // typed queries, spread across the replicas
    for _ in 0..4 {
        let products: Vec<Product> = aggregator
            .query_by_primary_key(&name, &[3, 11_999, 50_000])
            .await
            .unwrap();
        let mut ids: Vec<i64> = products.iter().map(|p| p.id).collect();
        ids.sort();
        assert_eq!(ids, vec![3, 11_999]);
        assert_eq!(products.iter().find(|p| p.id == 3), Some(&Product::new(3)));
    }

    let category: Vec<Product> = aggregator
        .query_by_secondary_key(&name, "category", 7)
        .await
        .unwrap();
    assert_eq!(category.len(), 1_200);
    assert!(category.iter().all(|p| p.category == 7));

    // the whole collection, once (from one replica)
    let all = aggregator
        .stream_all_data(&name)
        .collect_all()
        .await
        .unwrap();
    assert_eq!(all.len(), 12_000);

    let mut typed = aggregator.stream_by_secondary_key(&name, "category", 2);
    let mut count = 0;
    while let Some(product) = typed.next_as::<Product>().await {
        assert_eq!(product.unwrap().category, 2);
        count += 1;
    }
    assert_eq!(count, 1_200);

    aggregator.drop_collection(&name).await.unwrap();
}

#[tokio::test]
async fn a_replica_that_is_down_is_skipped() {
    let _serial = serial().await;
    let Some(mut servers) = servers() else { return };
    servers.push(("127.0.0.1".into(), dead_port().await));
    let aggregator = Aggregator::connect(&servers, options()).await.unwrap();
    let name = unique("down");

    let replicas = aggregator.replicas();
    assert_eq!(replicas.iter().filter(|r| r.connected).count(), 2);
    assert!(!replicas[2].connected);

    declared(&aggregator, &name).await;
    aggregator
        .feed_items(&name, (0..100).map(|i| Item::new(vec![1; 10], [i, i % 10])))
        .await
        .unwrap();
    assert_eq!(
        aggregator
            .query_raw_by_primary_key(&name, &[1, 2])
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(aggregator.get_collections_description().await[2].is_none());

    // dropping needs every replica: the missing one would bring the collection back
    assert!(matches!(
        aggregator.drop_collection(&name).await,
        Err(Error::NotConnected(_))
    ));

    let all_up = Aggregator::connect(&servers[..2], options()).await.unwrap();
    all_up.drop_collection(&name).await.unwrap();
}

/// A fake replica: it answers "describe" with the real server's collections, so that it is a candidate for
/// requests, then drops the connection on any other request.
async fn start_failing_replica(description: Message) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let frame = description.encode_frame().unwrap();

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let frame = frame.clone();
            tokio::spawn(async move {
                loop {
                    let mut header = [0u8; HEADER_SIZE];
                    if socket.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let Ok((message_type, size)) = Message::decode_header(header) else {
                        return;
                    };
                    let mut body = vec![0u8; size];
                    if socket.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    if message_type != MessageType::GetCollectionsDescriptionRequest {
                        return; // dropped: the connection closes without an answer
                    }
                    if socket.write_all(&frame).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn requests_fail_over_when_a_replica_connection_fails() {
    let _serial = serial().await;
    let Some(servers) = servers() else { return };
    let (host, port) = servers[0].clone();
    let name = unique("failover");

    // the collection, fed on the real server
    let mut connector = Connector::connect(&host, port).await.unwrap();
    connector
        .create_collection(&name, "id", &["category"])
        .await
        .unwrap();
    connector
        .feed_collection(
            &name,
            "v1",
            (0..12_000).map(|i| Item::new(Product::new(i).to_bytes(), [i, i % 10])),
        )
        .await
        .unwrap();
    let description =
        Message::CollectionsDescription(connector.get_collections_description().await.unwrap());

    for attempt in 0..4 {
        // fresh each time: the failing replica is marked down after its first failure
        let failing = start_failing_replica(description.clone()).await;
        let aggregator = Aggregator::connect(
            &[("127.0.0.1".to_string(), failing), (host.clone(), port)],
            options(),
        )
        .await
        .unwrap();
        assert!(
            aggregator.replicas().iter().all(|r| r.connected),
            "both look healthy at first"
        );

        // round-robin starts with the first replica: the failing one, then the request moves to the real one
        match attempt % 2 {
            0 => assert_eq!(
                aggregator
                    .query_raw_by_primary_key(&name, &[5])
                    .await
                    .unwrap()
                    .len(),
                1
            ),
            _ => assert_eq!(
                aggregator
                    .stream_all_data(&name)
                    .collect_all()
                    .await
                    .unwrap()
                    .len(),
                12_000
            ),
        }
        let products: Vec<Product> = aggregator
            .query_by_secondary_key(&name, "category", 1)
            .await
            .unwrap();
        assert_eq!(products.len(), 1_200);

        // the failing replica, once used, is marked down; the real one is not
        let replicas = aggregator.replicas();
        assert!(
            !replicas[0].connected && replicas[1].connected,
            "{replicas:?}"
        );
    }

    connector.drop_collection(&name, false).await.unwrap();
}

#[tokio::test]
async fn abandoned_streams_dont_exhaust_the_pool() {
    let _serial = serial().await;
    let Some(servers) = servers() else { return };
    let aggregator = Aggregator::connect(&servers, options()).await.unwrap();
    let name = unique("abandoned");
    declared(&aggregator, &name).await;
    aggregator
        .feed_collection(&name, (0..12_000).map(Product::new))
        .await
        .unwrap();

    // twice the connections of both pools: abandoned connections are replaced, not leaked
    for _ in 0..8 {
        let mut stream = aggregator.stream_all_data(&name);
        stream.next().await.unwrap().unwrap();
    }

    let query = aggregator.query_raw_by_primary_key(&name, &[42]);
    let documents = tokio::time::timeout(Duration::from_secs(10), query)
        .await
        .expect("the pool is exhausted")
        .unwrap();
    assert_eq!(documents.len(), 1);
    assert!(aggregator.replicas().iter().all(|r| r.connected));

    aggregator.drop_collection(&name).await.unwrap();
}

#[tokio::test]
async fn server_errors_are_reported_without_marking_replicas_down() {
    let _serial = serial().await;
    let Some(servers) = servers() else { return };
    let aggregator = Aggregator::connect(&servers, options()).await.unwrap();

    let missing = unique("missing");
    assert!(matches!(
        aggregator.query_raw_by_primary_key(&missing, &[1]).await,
        Err(Error::Server { .. })
    ));
    assert!(matches!(
        aggregator.stream_all_data(&missing).collect_all().await,
        Err(Error::Server { .. })
    ));
    assert!(aggregator.replicas().iter().all(|r| r.connected));
}

#[tokio::test]
async fn local_cache_answers_repeated_lookups_and_is_cleared_by_a_new_version() {
    let _serial = serial().await;
    let Some(servers) = servers() else { return };
    let aggregator = Aggregator::connect(&servers, options()).await.unwrap();
    let name = unique("cache");
    declared(&aggregator, &name).await;
    aggregator
        .feed_collection(&name, (0..100).map(Product::new))
        .await
        .unwrap();

    aggregator.configure_local_cache(&name, 1000);
    let mut events = aggregator.subscribe();

    for _ in 0..3 {
        let products: Vec<Product> = aggregator
            .query_by_primary_key(&name, &[1, 2, 999])
            .await
            .unwrap();
        assert_eq!(products.len(), 2);
    }
    let statistics = aggregator.local_cache_statistics()[&name];
    assert_eq!(statistics.calls, 9);
    assert_eq!(
        statistics.calls_to_server, 3,
        "each key fetched once, the missing one included"
    );
    assert_eq!(statistics.not_found_on_server, 1);
    assert_eq!(statistics.found_in_local_cache, 6);

    // a new version is reported (here by the feed itself) and clears the cache
    tokio::time::sleep(Duration::from_millis(1100)).await; // a different version name
    let version = aggregator
        .feed_collection(&name, (0..100).map(Product::new))
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (event.collection.as_str(), event.version.as_str()),
        (name.as_str(), version.as_str())
    );

    aggregator
        .query_raw_by_primary_key(&name, &[1])
        .await
        .unwrap();
    assert_eq!(
        aggregator.local_cache_statistics()[&name].calls_to_server,
        4,
        "fetched again"
    );

    aggregator.drop_collection(&name).await.unwrap();
}

#[tokio::test]
async fn a_feed_failing_on_one_replica_is_aborted_on_all() {
    let _serial = serial().await;
    let Some(servers) = servers() else { return };
    let name = unique("feedfailure");

    // replica 0 can store two keys per item, replica 1 only one: its feed fails
    let mut first = Connector::connect(&servers[0].0, servers[0].1)
        .await
        .unwrap();
    first
        .create_collection(&name, "id", &["category"])
        .await
        .unwrap();
    let mut second = Connector::connect(&servers[1].0, servers[1].1)
        .await
        .unwrap();
    second.create_collection(&name, "id", &[]).await.unwrap();

    let aggregator = Aggregator::connect(&servers, options()).await.unwrap();
    let feed = aggregator.feed_collection(&name, (0..500_000).map(Product::new));
    let result = tokio::time::timeout(Duration::from_secs(60), feed)
        .await
        .expect("the feed froze");

    match result {
        Err(Error::Replica { address, .. }) => {
            assert_eq!(address, format!("{}:{}", servers[1].0, servers[1].1))
        }
        other => panic!("expected the failure of replica 1, got {other:?}"),
    }

    // the healthy replica didn't commit a partial version (it notices the closed connection asynchronously)
    let mut committed = true;
    for _ in 0..50 {
        let description = first.get_collections_description().await.unwrap();
        committed = description.collections.iter().any(|c| c.name == name);
        if !committed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!committed, "replica 0 committed a partial version");

    // the aggregator still works on both replicas
    assert!(
        aggregator
            .get_collections_description()
            .await
            .iter()
            .all(Option::is_some)
    );

    first.drop_collection(&name, true).await.unwrap();
    second.drop_collection(&name, true).await.unwrap();
}
