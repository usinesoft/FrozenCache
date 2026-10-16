//! End-to-end tests against a running .NET FrozenCache server. Skipped unless `FROZENCACHE_TEST_SERVER` is set:
//!
//! ```text
//! FROZENCACHE_TEST_SERVER=localhost:5123 cargo test
//! FROZENCACHE_TEST_SERVER=localhost:5123 FROZENCACHE_TEST_TLS=1 cargo test   # server with UseSsl=true
//! ```
//!
//! Every test uses its own collections, so the tests can run against a server holding other data.
//!
//! The tests run one at a time (see [`serial`]): the server has a race between a feed and a drop running at the
//! same time on different collections. Finishing a feed re-reads the metadata of every collection, and fails on a
//! collection whose folder is being deleted (its `metadata.json` already gone), reporting an error although the
//! new version was stored and activated. The .NET client is affected the same way.

use std::time::{SystemTime, UNIX_EPOCH};

use frozencache_client::{ConnectOptions, Connector, Error, Item};
use tokio::sync::{Mutex, MutexGuard};

/// Held for the whole test, so that tests don't overlap on the server.
async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::const_new(());
    LOCK.lock().await
}

struct Server {
    host: String,
    port: u16,
    tls: bool,
}

fn server() -> Option<Server> {
    let address = std::env::var("FROZENCACHE_TEST_SERVER").ok()?;
    let (host, port) = address
        .rsplit_once(':')
        .expect("FROZENCACHE_TEST_SERVER must be host:port");
    let tls = std::env::var("FROZENCACHE_TEST_TLS")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    Some(Server {
        host: host.to_owned(),
        port: port.parse().expect("invalid port"),
        tls,
    })
}

/// The connector for this test's server, or `None` (test skipped) without a server.
async fn connect() -> Option<Connector> {
    let Some(server) = server() else {
        eprintln!("skipped: FROZENCACHE_TEST_SERVER is not set");
        return None;
    };
    Some(
        connect_to(&server, server.tls)
            .await
            .expect("cannot connect to the test server"),
    )
}

async fn connect_to(server: &Server, tls: bool) -> frozencache_client::Result<Connector> {
    let options = ConnectOptions {
        use_tls: tls,
        validate_server_certificate: false,
        connect_timeout: std::time::Duration::from_secs(3),
    };
    Connector::connect_with(&server.host, server.port, &options).await
}

/// A collection name no other test (or earlier run) uses.
fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rust_{name}_{nanos}")
}

/// Product `i`: 300 bytes starting with its id, in category `i % 10`.
fn product(i: i64) -> Item {
    let mut data = vec![0u8; 300];
    data[..8].copy_from_slice(&i.to_le_bytes());
    Item::new(data, [i, i % 10])
}

fn id_of(data: &[u8]) -> i64 {
    i64::from_le_bytes(data[..8].try_into().unwrap())
}

#[tokio::test]
async fn full_lifecycle() {
    let _serial = serial().await;
    let Some(mut c) = connect().await else { return };
    let name = unique("lifecycle");

    c.create_collection(&name, "id", &["category"])
        .await
        .unwrap();
    // idempotent with the same keys
    c.create_collection(&name, "id", &["category"])
        .await
        .unwrap();

    // 12,000 items: three batches of up to 5,000
    c.feed_collection(&name, "v001", (0..12_000).map(product))
        .await
        .unwrap();

    let description = c.get_collections_description().await.unwrap();
    let info = description
        .collections
        .iter()
        .find(|i| i.name == name)
        .expect("collection listed");
    assert_eq!(info.count, 12_000);
    assert_eq!(info.last_version, "v001");
    assert_eq!(info.keys, vec!["id", "category"]);
    assert_eq!(info.avg_object_size(), 300.0);

    // by primary key, including a missing key
    let mut ids: Vec<i64> = c
        .query_by_primary_key(&name, &[5, 11_999, 99_999])
        .await
        .unwrap()
        .iter()
        .map(|d| id_of(d))
        .collect();
    ids.sort();
    assert_eq!(ids, vec![5, 11_999]);

    // whole collection, across batches
    let all = c
        .stream_all_data(&name)
        .await
        .unwrap()
        .collect_all()
        .await
        .unwrap();
    assert_eq!(all.len(), 12_000);
    assert!(all.iter().all(|d| d.primary_key == id_of(&d.data)));

    // by secondary index
    let category = c
        .stream_by_secondary_index(&name, "category", 3)
        .await
        .unwrap()
        .collect_all()
        .await
        .unwrap();
    assert_eq!(category.len(), 1_200);
    assert!(category.iter().all(|d| d.primary_key % 10 == 3));

    // a new version replaces the old one
    c.feed_collection(&name, "v002", (0..10).map(product))
        .await
        .unwrap();
    assert!(
        c.query_by_primary_key(&name, &[5_000])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(c.query_by_primary_key(&name, &[5]).await.unwrap().len(), 1);

    c.drop_collection(&name, false).await.unwrap();
    assert!(
        c.get_collections_description()
            .await
            .unwrap()
            .collections
            .iter()
            .all(|i| i.name != name)
    );

    assert!(c.ping().await);
    assert!(c.is_healthy());
}

#[tokio::test]
async fn server_errors_leave_the_connection_usable() {
    let _serial = serial().await;
    let Some(mut c) = connect().await else { return };
    let name = unique("errors");

    let unknown = c.query_by_primary_key(&unique("missing"), &[1]).await;
    assert!(matches!(unknown, Err(Error::Server { .. })), "{unknown:?}");

    c.create_collection(&name, "id", &["category"])
        .await
        .unwrap();
    let schema = c.create_collection(&name, "id", &["other"]).await;
    assert!(
        matches!(schema, Err(Error::Server { .. })),
        "different keys: {schema:?}"
    );

    c.feed_collection(&name, "v002", (0..10).map(product))
        .await
        .unwrap();
    let older = c.feed_collection(&name, "v001", (0..10).map(product)).await;
    assert!(
        matches!(older, Err(Error::Server { .. })),
        "older version: {older:?}"
    );

    // (a stream borrows the connector, so the result is checked in place rather than kept)
    assert!(
        matches!(
            c.stream_by_secondary_index(&name, "nonexistent", 1).await,
            Err(Error::Server { .. })
        ),
        "unknown index"
    );

    let empty = c.query_by_primary_key(&name, &[]).await;
    assert!(matches!(empty, Err(Error::InvalidArgument(_))));

    // none of these broke the connection
    assert!(c.is_healthy());
    assert_eq!(c.query_by_primary_key(&name, &[3]).await.unwrap().len(), 1);

    assert!(matches!(
        c.drop_collection(&unique("missing"), false).await,
        Err(Error::Server { .. })
    ));
    c.drop_collection(&unique("missing"), true).await.unwrap();
    c.drop_collection(&name, false).await.unwrap();
}

#[tokio::test]
async fn an_abandoned_stream_makes_the_connection_unusable() {
    let _serial = serial().await;
    let Some(mut c) = connect().await else { return };
    let name = unique("abandoned");

    c.create_collection(&name, "id", &["category"])
        .await
        .unwrap();
    c.feed_collection(&name, "v001", (0..12_000).map(product))
        .await
        .unwrap();

    {
        let mut documents = c.stream_all_data(&name).await.unwrap();
        documents.next().await.unwrap().unwrap();
        // dropped with batches still unread
    }

    assert!(!c.is_healthy());
    assert!(!c.ping().await);
    assert!(matches!(
        c.query_by_primary_key(&name, &[1]).await,
        Err(Error::Unusable)
    ));

    // a new connection is fine, and so is the server
    let mut fresh = connect().await.unwrap();
    assert_eq!(
        fresh.query_by_primary_key(&name, &[1]).await.unwrap().len(),
        1
    );

    // The abandoned connection was closed, so the server stops streaming and releases the collection: dropping
    // it must succeed shortly (it is refused while a stream is in progress). `c` is deliberately still alive
    // here - keeping the socket open until the connector is dropped would block this for as long.
    let started = std::time::Instant::now();
    loop {
        match fresh.drop_collection(&name, false).await {
            Ok(()) => break,
            Err(Error::Server { message, .. }) if message.contains("being streamed") => {
                assert!(
                    started.elapsed().as_secs() < 5,
                    "the server is still streaming to the abandoned connection"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
    drop(c);
}

#[tokio::test]
async fn tls_mismatch_is_reported_explicitly() {
    let _serial = serial().await;
    let Some(server) = server() else {
        eprintln!("skipped: FROZENCACHE_TEST_SERVER is not set");
        return;
    };

    if server.tls {
        // a plain client on a TLS server: the server drops the connection
        let mut plain = connect_to(&server, false).await.unwrap();
        let result = plain.get_collections_description().await;
        assert!(
            matches!(result, Err(Error::ConnectionClosed) | Err(Error::Io(_))),
            "plain client on TLS server: {result:?}"
        );
        assert!(result.unwrap_err().is_connection_failure());
    } else {
        // a TLS client on a plain server: the handshake fails
        let result = connect_to(&server, true).await;
        assert!(
            matches!(result, Err(Error::Tls { .. })),
            "TLS client on plain server: {:?}",
            result.err()
        );
    }
}
