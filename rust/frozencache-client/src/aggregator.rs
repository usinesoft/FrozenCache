//! Replicas of a cluster, used as one: the counterpart of the .NET `Aggregator`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;

use crate::connector::{ConnectOptions, Document, Item};
use crate::error::{Error, Result};
use crate::local_cache::{CacheStatistics, LocalCache, Lookup};
use crate::pool::{ConnectorPool, PooledConnector, VersionListener};
use crate::protocol::{
    BatchWriter, CollectionsDescription, Message, StreamAllDataRequest,
    StreamBySecondaryIndexRequest,
};

/// A Rust type stored in a collection: how it is serialized, and its keys. The counterpart of the .NET
/// `Aggregator.RegisterTypedCollection<T>`, as a trait: there is nothing to register, and no "type not
/// registered" error at run time.
///
/// ```
/// use frozencache_client::CacheObject;
///
/// struct Product { id: i64, category: i64, name: String }
///
/// impl CacheObject for Product {
///     fn keys(&self) -> Vec<i64> {
///         vec![self.id, self.category] // primary key, then the secondary keys in declaration order
///     }
///     fn to_bytes(&self) -> Vec<u8> {
///         let mut bytes = Vec::new();
///         bytes.extend_from_slice(&self.id.to_le_bytes());
///         bytes.extend_from_slice(&self.category.to_le_bytes());
///         bytes.extend_from_slice(self.name.as_bytes());
///         bytes
///     }
///     fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
///         Ok(Product {
///             id: i64::from_le_bytes(bytes[0..8].try_into()?),
///             category: i64::from_le_bytes(bytes[8..16].try_into()?),
///             name: String::from_utf8(bytes[16..].to_vec())?,
///         })
///     }
/// }
/// ```
pub trait CacheObject: Sized {
    /// The primary key first, then the secondary keys in the order the indexes were declared.
    fn keys(&self) -> Vec<i64>;

    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(
        bytes: &[u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>>;
}

fn decode<T: CacheObject>(bytes: &[u8]) -> Result<T> {
    T::from_bytes(bytes).map_err(|e| Error::Deserialize(e.to_string()))
}

/// Options of [`Aggregator::connect`].
#[derive(Debug, Clone)]
pub struct AggregatorOptions {
    /// Connections to each server.
    pub pool_capacity: usize,
    /// How every connection is made (TLS, certificate validation, timeout).
    pub connect: ConnectOptions,
    /// How often each server is checked: reconnected if it was down, its collection versions refreshed.
    pub watchdog_interval: Duration,
}

impl Default for AggregatorOptions {
    fn default() -> Self {
        AggregatorOptions {
            pool_capacity: 4,
            connect: ConnectOptions::default(),
            watchdog_interval: Duration::from_secs(10),
        }
    }
}

/// A server reported a new version of a collection ([`Aggregator::subscribe`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewVersion {
    pub server: String,
    pub collection: String,
    pub version: String,
}

/// State of one replica ([`Aggregator::replicas`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaStatus {
    pub address: String,
    pub connected: bool,
}

/// The replicas of a FrozenCache cluster, used as one: the Rust counterpart of the .NET `CacheClient.Aggregator`.
///
/// * **Reads** (queries, streams) go to one replica at a time, round-robin among the connected replicas that
///   have the latest version of the collection. A replica whose connection fails is marked down and the request
///   moves to another, unless a stream has already returned documents. Errors reported by a server (unknown
///   collection or index...) are not retried: they would occur on any replica.
/// * **Feeds** go to every connected replica in parallel, with one version name; each batch is encoded once.
///   If one replica fails, the feed is aborted on all of them: no replica commits a partial version.
/// * Each server has a pool of connections and a watchdog that reconnects it when it comes back and refreshes the
///   versions it reports ([`Aggregator::subscribe`] to be notified).
/// * An optional **local cache** per collection answers repeated primary-key lookups, including missing keys.
///
/// Cheap to clone: clones share the same connections.
#[derive(Clone)]
pub struct Aggregator {
    inner: Arc<Inner>,
}

struct Inner {
    pools: Vec<Arc<ConnectorPool>>,
    next: AtomicUsize,
    shared: Arc<Shared>,
}

/// Reached from the pools' version notifications (which must not hold the pools themselves).
struct Shared {
    caches: RwLock<HashMap<String, Arc<LocalCache>>>,
    events: broadcast::Sender<NewVersion>,
}

impl Shared {
    fn on_new_version(&self, server: &str, collection: &str, version: &str) {
        // the cached documents belong to the previous version
        if let Some(cache) = self.caches.read().unwrap().get(collection) {
            cache.clear();
        }
        let _ = self.events.send(NewVersion {
            server: server.to_owned(),
            collection: collection.to_owned(),
            version: version.to_owned(),
        });
    }
}

/// Versions are ordered by name, case-insensitively, as the server does.
fn compare_versions(a: &str, b: &str) -> Ordering {
    a.to_lowercase().cmp(&b.to_lowercase())
}

fn on_replica(pool: &ConnectorPool, error: Error) -> Error {
    Error::Replica {
        address: pool.address().to_owned(),
        source: Box::new(error),
    }
}

impl Aggregator {
    /// Connects to every server (`host`, `port`). A server that can't be reached is not an error: it is marked
    /// disconnected and its watchdog keeps trying.
    pub async fn connect<S: AsRef<str>>(
        servers: &[(S, u16)],
        options: AggregatorOptions,
    ) -> Result<Aggregator> {
        if servers.is_empty() {
            return Err(Error::InvalidArgument(
                "at least one server is required".into(),
            ));
        }

        let (events, _) = broadcast::channel(256);
        let shared = Arc::new(Shared {
            caches: RwLock::new(HashMap::new()),
            events,
        });

        let listener: VersionListener = {
            let shared = shared.clone();
            Arc::new(move |server: &str, collection: &str, version: &str| {
                shared.on_new_version(server, collection, version)
            })
        };

        let pools: Vec<_> = servers
            .iter()
            .map(|(host, port)| {
                ConnectorPool::new(
                    host.as_ref(),
                    *port,
                    options.connect.clone(),
                    options.pool_capacity,
                    listener.clone(),
                )
            })
            .collect();

        let mut connecting = JoinSet::new();
        for pool in &pools {
            let pool = pool.clone();
            connecting.spawn(async move { pool.connect().await });
        }
        while connecting.join_next().await.is_some() {}

        for pool in &pools {
            pool.start_watchdog(options.watchdog_interval);
        }

        Ok(Aggregator {
            inner: Arc::new(Inner {
                pools,
                next: AtomicUsize::new(0),
                shared,
            }),
        })
    }

    /// The servers, in the order given to [`Aggregator::connect`], and whether each is connected.
    pub fn replicas(&self) -> Vec<ReplicaStatus> {
        self.inner
            .pools
            .iter()
            .map(|pool| ReplicaStatus {
                address: pool.address().to_owned(),
                connected: pool.is_connected(),
            })
            .collect()
    }

    /// The latest version of the collection reported by any replica, if any.
    pub fn last_version(&self, collection: &str) -> Option<String> {
        self.inner
            .pools
            .iter()
            .filter_map(|pool| pool.version_of(collection))
            .max_by(|a, b| compare_versions(a, b))
    }

    /// Notifications of new collection versions, as the watchdogs and feeds observe them (one per server). By the
    /// time one is received, the collection's local cache has been cleared.
    pub fn subscribe(&self) -> broadcast::Receiver<NewVersion> {
        self.inner.shared.events.subscribe()
    }

    /// A connection to a connected replica, round-robin; for a collection, among the replicas that have its
    /// latest version (any connected replica if none is known to).
    async fn get(&self, collection: Option<&str>) -> Result<PooledConnector> {
        loop {
            let connected: Vec<&Arc<ConnectorPool>> = self
                .inner
                .pools
                .iter()
                .filter(|p| p.is_connected())
                .collect();
            if connected.is_empty() {
                return Err(Error::NoServerAvailable);
            }

            let mut candidates = connected.clone();
            if let Some(collection) = collection
                && let Some(last) = self.last_version(collection)
            {
                let up_to_date: Vec<_> = connected
                    .iter()
                    .copied()
                    .filter(|p| p.version_of(collection).as_deref() == Some(last.as_str()))
                    .collect();
                if !up_to_date.is_empty() {
                    candidates = up_to_date;
                }
            }

            let index = self.inner.next.fetch_add(1, AtomicOrdering::Relaxed) % candidates.len();
            // fails only if the replica was disconnected meanwhile: then it's no longer a candidate
            if let Ok(connector) = candidates[index].get().await {
                return Ok(connector);
            }
        }
    }

    fn connected_pools(&self) -> Vec<Arc<ConnectorPool>> {
        self.inner
            .pools
            .iter()
            .filter(|p| p.is_connected())
            .cloned()
            .collect()
    }

    /// Creates the collection on every connected replica. Succeeds without change where it already exists with
    /// the same keys.
    pub async fn declare_collection(
        &self,
        collection: &str,
        primary_key: &str,
        other_indexes: &[&str],
    ) -> Result<()> {
        let mut requests = JoinSet::new();
        for pool in self.connected_pools() {
            let (collection, primary_key) = (collection.to_owned(), primary_key.to_owned());
            let other_indexes: Vec<String> =
                other_indexes.iter().map(|s| (*s).to_owned()).collect();
            requests.spawn(async move {
                let mut connector = pool.get().await.map_err(|e| on_replica(&pool, e))?;
                let indexes: Vec<&str> = other_indexes.iter().map(String::as_str).collect();
                connector
                    .create_collection(&collection, &primary_key, &indexes)
                    .await
                    .map_err(|e| failed_request(&pool, e))
            });
        }
        first_error(requests).await
    }

    /// Removes the collection, all its versions and metadata, from every replica. Every replica must be connected:
    /// otherwise the collection would come back with the missing one.
    pub async fn drop_collection(&self, collection: &str) -> Result<()> {
        if let Some(down) = self.inner.pools.iter().find(|p| !p.is_connected()) {
            return Err(Error::NotConnected(down.address().to_owned()));
        }

        let mut requests = JoinSet::new();
        for pool in self.connected_pools() {
            let collection = collection.to_owned();
            requests.spawn(async move {
                let mut connector = pool.get().await.map_err(|e| on_replica(&pool, e))?;
                connector
                    .drop_collection(&collection, true)
                    .await
                    .map_err(|e| failed_request(&pool, e))
            });
        }
        first_error(requests).await
    }

    /// Each replica's collections, in the order of the servers: `None` for a replica that isn't connected or
    /// doesn't answer.
    pub async fn get_collections_description(&self) -> Vec<Option<CollectionsDescription>> {
        let mut requests = JoinSet::new();
        for (index, pool) in self.inner.pools.iter().enumerate() {
            let pool = pool.clone();
            requests.spawn(async move {
                let mut connector = pool.get().await.ok()?;
                let description = connector
                    .get_collections_description()
                    .await
                    .map_err(|e| failed_request(&pool, e))
                    .ok();
                description.map(|d| (index, d))
            });
        }

        let mut descriptions = vec![None; self.inner.pools.len()];
        while let Some(result) = requests.join_next().await {
            if let Ok(Some((index, description))) = result {
                descriptions[index] = Some(description);
            }
        }
        descriptions
    }

    // --- queries by primary key ---

    /// The raw documents whose primary key is one of `keys`. Through the local cache if the collection has one.
    pub async fn query_raw_by_primary_key(
        &self,
        collection: &str,
        keys: &[i64],
    ) -> Result<Vec<Vec<u8>>> {
        let cache = self
            .inner
            .shared
            .caches
            .read()
            .unwrap()
            .get(collection)
            .cloned();
        match cache {
            Some(cache) => self.query_through_cache(&cache, collection, keys).await,
            None => self.query_server(collection, keys).await,
        }
    }

    /// The documents whose primary key is one of `keys`, deserialized as `T`.
    pub async fn query_by_primary_key<T: CacheObject>(
        &self,
        collection: &str,
        keys: &[i64],
    ) -> Result<Vec<T>> {
        self.query_raw_by_primary_key(collection, keys)
            .await?
            .iter()
            .map(|data| decode(data))
            .collect()
    }

    async fn query_server(&self, collection: &str, keys: &[i64]) -> Result<Vec<Vec<u8>>> {
        loop {
            let mut connector = self.get(Some(collection)).await?;
            match connector.query_by_primary_key(collection, keys).await {
                Ok(documents) => return Ok(documents),
                Err(e) if e.is_connection_failure() => {
                    // this replica is gone: the next attempt goes to another one
                    connector.pool().mark_disconnected();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Cached keys are answered locally; the others are fetched one by one (the server's answer doesn't say which
    /// key a document belongs to) and cached, missing keys included.
    async fn query_through_cache(
        &self,
        cache: &LocalCache,
        collection: &str,
        keys: &[i64],
    ) -> Result<Vec<Vec<u8>>> {
        let mut documents = Vec::with_capacity(keys.len());
        for &key in keys {
            match cache.lookup(key) {
                Lookup::Found(data) => documents.push(data),
                Lookup::Missing => {}
                Lookup::Unknown => {
                    let started = Instant::now();
                    let mut found = self.query_server(collection, &[key]).await?;
                    let document = if found.is_empty() {
                        None
                    } else {
                        Some(found.swap_remove(0))
                    };
                    cache.store(key, document.clone(), started.elapsed());
                    documents.extend(document);
                }
            }
        }
        Ok(documents)
    }

    /// Puts an LRU cache of `capacity` documents in front of the servers for this collection's primary-key
    /// queries. It holds one document per key, so it is for collections with a unique primary key; it is cleared
    /// when a new version of the collection is reported.
    pub fn configure_local_cache(&self, collection: &str, capacity: usize) {
        self.inner
            .shared
            .caches
            .write()
            .unwrap()
            .insert(collection.to_owned(), Arc::new(LocalCache::new(capacity)));
    }

    /// Statistics of every local cache, by collection.
    pub fn local_cache_statistics(&self) -> HashMap<String, CacheStatistics> {
        self.inner
            .shared
            .caches
            .read()
            .unwrap()
            .iter()
            .map(|(collection, cache)| (collection.clone(), cache.statistics()))
            .collect()
    }

    // --- streams ---

    /// Streams every document of the collection's active version, from one replica.
    pub fn stream_all_data(&self, collection: &str) -> ClusterStream {
        ClusterStream::new(
            self.clone(),
            collection,
            Message::StreamAllDataRequest(StreamAllDataRequest {
                collection_name: collection.to_owned(),
            }),
            "stream collection",
        )
    }

    /// Streams the documents whose value for the index `index` equals `key`, from one replica.
    pub fn stream_by_secondary_key(
        &self,
        collection: &str,
        index: &str,
        key: i64,
    ) -> ClusterStream {
        ClusterStream::new(
            self.clone(),
            collection,
            Message::StreamBySecondaryIndexRequest(StreamBySecondaryIndexRequest {
                collection_name: collection.to_owned(),
                index_name: index.to_owned(),
                key_value: key,
            }),
            "stream by secondary index",
        )
    }

    /// All the documents whose value for the index `index` equals `key`, deserialized as `T`. Unlike a stream,
    /// restarted on another replica if the one answering fails part way.
    pub async fn query_by_secondary_key<T: CacheObject>(
        &self,
        collection: &str,
        index: &str,
        key: i64,
    ) -> Result<Vec<T>> {
        loop {
            let mut stream = self.stream_by_secondary_key(collection, index, key);
            let mut objects = Vec::new();
            let mut replica_failed = false;

            while let Some(document) = stream.next().await {
                match document {
                    Ok(document) => objects.push(decode(&document.data)?),
                    Err(Error::NoServerAvailable) => return Err(Error::NoServerAvailable),
                    Err(e) if e.is_connection_failure() => {
                        // the replica is marked down: the next attempt goes to another one
                        replica_failed = true;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }

            if !replica_failed {
                return Ok(objects);
            }
        }
    }

    // --- feeds ---

    /// Feeds a new version of the collection to every connected replica, with raw items. Returns the version
    /// name: the current time, UTC, `yyyyMMdd_HHmmss` (as the .NET Aggregator names it).
    pub async fn feed_items<I: IntoIterator<Item = Item>>(
        &self,
        collection: &str,
        items: I,
    ) -> Result<String> {
        self.feed(
            collection,
            items.into_iter().map(|item| (item.keys, item.data)),
        )
        .await
    }

    /// Feeds a new version of the collection to every connected replica, with typed objects. Returns the version
    /// name, as [`Aggregator::feed_items`].
    pub async fn feed_collection<T: CacheObject, I: IntoIterator<Item = T>>(
        &self,
        collection: &str,
        objects: I,
    ) -> Result<String> {
        self.feed(
            collection,
            objects
                .into_iter()
                .map(|object| (object.keys(), object.to_bytes())),
        )
        .await
    }

    /// One feed session per replica, each with its own writer task; the batches are encoded once and shared. The
    /// first failure aborts every session without its end marker, so that no replica commits a partial version.
    async fn feed(
        &self,
        collection: &str,
        items: impl Iterator<Item = (Vec<i64>, Vec<u8>)>,
    ) -> Result<String> {
        let version = utc_version(SystemTime::now());

        let pools = self.connected_pools();
        if pools.is_empty() {
            return Err(Error::NoServerAvailable);
        }

        // every replica validates the feed before anything is sent
        let mut sessions: Vec<PooledConnector> = Vec::with_capacity(pools.len());
        for pool in &pools {
            let started = async {
                let mut connector = pool.get().await?;
                connector.begin_feed(collection, &version).await?;
                Ok::<_, Error>(connector)
            }
            .await;

            match started {
                Ok(connector) => sessions.push(connector),
                Err(e) => {
                    abort_sessions(sessions);
                    return Err(on_replica(pool, e));
                }
            }
        }

        let mut senders = Vec::with_capacity(sessions.len());
        let mut writers = Vec::with_capacity(sessions.len());
        for session in sessions {
            let (sender, receiver) = mpsc::channel(4);
            senders.push(sender);
            writers.push(tokio::spawn(write_feed(session, receiver)));
        }

        // produce the batches, unless a writer fails (its channel is then closed)
        let mut failure: Option<Error> = None;
        let mut aborted = false;
        let mut batch = BatchWriter::new();
        for (keys, data) in items {
            match batch.push(&keys, &data) {
                Err(e) => {
                    failure = Some(e);
                    break;
                }
                Ok(false) => {}
                Ok(true) => {
                    let encoded = Arc::new(batch.take(BatchWriter::buffer()));
                    if !send_to_all(&senders, FeedMessage::Batch(encoded)).await {
                        aborted = true;
                        break;
                    }
                }
            }
        }

        if failure.is_none() && !aborted {
            if !batch.is_empty() {
                let encoded = Arc::new(batch.take(Vec::new()));
                aborted = !send_to_all(&senders, FeedMessage::Batch(encoded)).await;
            }
            if !aborted {
                aborted = !send_to_all(&senders, FeedMessage::End).await;
            }
        }

        // closing the channels: the writers that didn't get the end marker abort their session
        drop(senders);

        for writer in writers {
            match writer.await {
                Ok(WriterOutcome::Done { pool }) => {
                    // known at once, rather than at the next watchdog check: reads go to the new version
                    pool.record_version(collection, &version);
                }
                Ok(WriterOutcome::Aborted) => {}
                Ok(WriterOutcome::Failed(error)) => {
                    failure.get_or_insert(error);
                }
                Err(e) => {
                    failure.get_or_insert(Error::Protocol(format!("feed writer task failed: {e}")));
                }
            }
        }

        match failure {
            Some(error) => Err(error),
            None if aborted => Err(Error::Protocol("the feed was aborted".into())),
            None => Ok(version),
        }
    }
}

enum FeedMessage {
    Batch(Arc<Vec<u8>>),
    End,
}

enum WriterOutcome {
    Done { pool: Arc<ConnectorPool> },
    Aborted,
    Failed(Error),
}

/// False if a writer has stopped (failed): the feed is then aborted.
async fn send_to_all(senders: &[mpsc::Sender<FeedMessage>], message: FeedMessage) -> bool {
    for sender in senders {
        let copy = match &message {
            FeedMessage::Batch(batch) => FeedMessage::Batch(batch.clone()),
            FeedMessage::End => FeedMessage::End,
        };
        if sender.send(copy).await.is_err() {
            return false;
        }
    }
    true
}

/// Writes one replica's feed session, until the end marker, or until the channel closes without it (aborted).
async fn write_feed(
    mut session: PooledConnector,
    mut batches: mpsc::Receiver<FeedMessage>,
) -> WriterOutcome {
    let result = async {
        while let Some(message) = batches.recv().await {
            match message {
                FeedMessage::Batch(batch) => session.write_batch(&batch).await?,
                FeedMessage::End => {
                    session.end_feed().await?;
                    return Ok(true);
                }
            }
        }
        Ok::<_, Error>(false)
    }
    .await;

    match result {
        Ok(true) => WriterOutcome::Done {
            pool: session.pool().clone(),
        },
        Ok(false) => {
            // in the middle of a feed session: closed, so that the server discards the partial version
            session.close();
            WriterOutcome::Aborted
        }
        Err(e) => {
            session.close();
            WriterOutcome::Failed(on_replica(session.pool(), e))
        }
    }
}

/// Feed sessions started but not continued: closed, so that each server discards the version.
fn abort_sessions(sessions: Vec<PooledConnector>) {
    for mut session in sessions {
        session.close();
    }
}

/// A request's error on a replica; a connection failure also marks the replica down.
fn failed_request(pool: &ConnectorPool, error: Error) -> Error {
    if error.is_connection_failure() {
        pool.mark_disconnected();
    }
    on_replica(pool, error)
}

async fn first_error(mut requests: JoinSet<Result<()>>) -> Result<()> {
    let mut first = None;
    while let Some(result) = requests.join_next().await {
        let result =
            result.unwrap_or_else(|e| Err(Error::Protocol(format!("request task failed: {e}"))));
        if let Err(e) = result {
            first.get_or_insert(e);
        }
    }
    first.map_or(Ok(()), Err)
}

/// Documents streamed from one replica of an [`Aggregator`] ([`Aggregator::stream_all_data`],
/// [`Aggregator::stream_by_secondary_key`]).
///
/// The replica is chosen like for a query. If its connection fails before the first document, the stream moves
/// to another replica; once documents were returned, the failure is reported instead, since a retry would return
/// them again. Dropping the stream before its end closes the connection (unread data remains), which the pool
/// replaces.
pub struct ClusterStream {
    aggregator: Aggregator,
    collection: String,
    request: Message,
    operation: &'static str,
    current: Option<PooledConnector>,
    any_document: bool,
    done: bool,
}

impl ClusterStream {
    fn new(
        aggregator: Aggregator,
        collection: &str,
        request: Message,
        operation: &'static str,
    ) -> Self {
        ClusterStream {
            aggregator,
            collection: collection.to_owned(),
            request,
            operation,
            current: None,
            any_document: false,
            done: false,
        }
    }

    /// The next document; `None` at the end, or after an error.
    pub async fn next(&mut self) -> Option<Result<Document>> {
        loop {
            if self.done {
                return None;
            }

            if self.current.is_none() {
                let mut connector = match self.aggregator.get(Some(&self.collection)).await {
                    Ok(connector) => connector,
                    Err(e) => return self.fail(e),
                };
                match connector.begin_stream(&self.request, self.operation).await {
                    Ok(()) => self.current = Some(connector),
                    Err(e) if e.is_connection_failure() => {
                        // nothing returned yet: try another replica
                        connector.pool().mark_disconnected();
                        continue;
                    }
                    Err(e) => return self.fail(e),
                }
            }

            let connector = self.current.as_mut()?;
            match connector.next_document().await {
                Some(Ok(document)) => {
                    self.any_document = true;
                    return Some(Ok(document));
                }
                None => {
                    // read to its end: the connection goes back to its pool
                    self.current = None;
                    self.done = true;
                    return None;
                }
                Some(Err(e)) => {
                    let connector = self.current.take()?;
                    if e.is_connection_failure() {
                        connector.pool().mark_disconnected();
                        if !self.any_document {
                            continue;
                        }
                    }
                    return self.fail(e);
                }
            }
        }
    }

    /// The next document, deserialized as `T`.
    pub async fn next_as<T: CacheObject>(&mut self) -> Option<Result<T>> {
        let document = self.next().await?;
        Some(document.and_then(|document| decode(&document.data)))
    }

    /// Reads the rest of the stream.
    pub async fn collect_all(mut self) -> Result<Vec<Document>> {
        let mut documents = Vec::new();
        while let Some(document) = self.next().await {
            documents.push(document?);
        }
        Ok(documents)
    }

    fn fail(&mut self, error: Error) -> Option<Result<Document>> {
        self.done = true;
        Some(Err(error))
    }
}

impl Drop for ClusterStream {
    fn drop(&mut self) {
        if let Some(mut connector) = self.current.take() {
            connector.abandon_stream();
        }
    }
}

/// The current time, UTC, as `yyyyMMdd_HHmmss`: the .NET Aggregator's version names, so that a collection can be
/// fed by both clients (the server only accepts a version whose name sorts after the current one).
fn utc_version(now: SystemTime) -> String {
    let seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (days, time) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}_{:02}{:02}{:02}",
        time / 3600,
        time % 3600 / 60,
        time % 60
    )
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian calendar (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_names_match_the_dotnet_aggregator() {
        let at = |seconds: u64| utc_version(UNIX_EPOCH + Duration::from_secs(seconds));
        assert_eq!(at(0), "19700101_000000");
        // 2026-10-16 13:05:09 UTC: a 24-hour clock
        assert_eq!(at(1_792_155_909), "20261016_130509");
        assert_eq!(at(951_782_400), "20000229_000000", "leap day");
        assert_eq!(
            at(4_107_542_399),
            "21000228_235959",
            "2100 is not a leap year"
        );
    }

    #[test]
    fn versions_compare_case_insensitively() {
        assert_eq!(
            compare_versions("20261016_130509", "20261016_010000"),
            Ordering::Greater
        );
        assert_eq!(compare_versions("V2", "v1"), Ordering::Greater);
        assert_eq!(compare_versions("abc", "ABC"), Ordering::Equal);
    }
}
