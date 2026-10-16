//! Connections to one server, with a watchdog: the counterpart of the .NET `ConnectorPool`.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::Notify;

use crate::connector::{ConnectOptions, Connector};
use crate::error::{Error, Result};

/// Called with (server, collection, version) when the server reports a new version of a collection.
pub(crate) type VersionListener = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;

/// `capacity` connections to one server, lent one at a time ([`ConnectorPool::get`]) and handed back by
/// [`PooledConnector`]'s drop.
///
/// While connected, a watchdog periodically checks the server with a pooled connection and records the versions
/// it reports; when the server stops answering, the pool is marked disconnected (its connections dropped) and
/// the watchdog reconnects it once the server is back.
pub(crate) struct ConnectorPool {
    host: String,
    port: u16,
    address: String,
    options: ConnectOptions,
    capacity: usize,
    state: Mutex<PoolState>,
    /// Signalled when a connection becomes available, and when the pool is disconnected (so that waiters fail
    /// instead of waiting for a server that is gone).
    changed: Notify,
    /// Last version of each collection reported by the server. Kept while disconnected.
    versions: Mutex<HashMap<String, String>>,
    on_new_version: VersionListener,
}

struct PoolState {
    idle: Vec<Connector>,
    connected: bool,
    /// Incremented by every (re)connection and disconnection: a connection lent before it is dropped on return
    /// rather than added to a pool that has since been refilled, which would exceed its capacity.
    generation: u64,
    /// True once the versions were read for the first time: from then on, changes raise events.
    has_baseline: bool,
}

impl ConnectorPool {
    pub fn new(
        host: &str,
        port: u16,
        options: ConnectOptions,
        capacity: usize,
        on_new_version: VersionListener,
    ) -> Arc<Self> {
        Arc::new(ConnectorPool {
            host: host.to_owned(),
            port,
            address: format!("{host}:{port}"),
            options,
            capacity: capacity.max(1),
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                connected: false,
                generation: 0,
                has_baseline: false,
            }),
            changed: Notify::new(),
            versions: Mutex::new(HashMap::new()),
            on_new_version,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn is_connected(&self) -> bool {
        self.state.lock().unwrap().connected
    }

    /// The last version of `collection` reported by the server.
    pub fn version_of(&self, collection: &str) -> Option<String> {
        self.versions.lock().unwrap().get(collection).cloned()
    }

    /// Opens the connections and reads the server's versions; false if the server can't be reached.
    pub async fn connect(self: &Arc<Self>) -> bool {
        let mut connections = Vec::with_capacity(self.capacity);
        for _ in 0..self.capacity {
            match Connector::connect_with(&self.host, self.port, &self.options).await {
                Ok(connector) => connections.push(connector),
                Err(_) => break,
            }
        }
        if connections.is_empty() {
            return false;
        }

        {
            let mut state = self.state.lock().unwrap();
            state.idle = connections;
            state.connected = true;
            state.generation += 1;
        }
        self.changed.notify_waiters();

        // the versions known at connection time, so that requests can be routed right away
        self.check_versions().await;
        true
    }

    /// Lends a connection, waiting for one if they are all in use; fails at once if the server is disconnected.
    pub async fn get(self: &Arc<Self>) -> Result<PooledConnector> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            {
                let mut state = self.state.lock().unwrap();
                if !state.connected {
                    return Err(Error::NotConnected(self.address.clone()));
                }
                if let Some(connector) = state.idle.pop() {
                    return Ok(PooledConnector {
                        connector: Some(connector),
                        pool: self.clone(),
                        generation: state.generation,
                    });
                }
            }

            changed.await;
        }
    }

    /// Drops every connection and refuses requests until the watchdog reconnects. Called when a request fails
    /// because of the connection.
    pub fn mark_disconnected(&self) {
        {
            let mut state = self.state.lock().unwrap();
            if !state.connected {
                return;
            }
            state.connected = false;
            state.idle.clear();
            state.generation += 1;
        }
        self.changed.notify_waiters();
    }

    /// A lent connection coming back: added to the idle ones if still usable; closed ones (an abandoned stream, a
    /// failed request) are replaced in the background, so that the pool keeps its capacity.
    fn give_back(self: &Arc<Self>, connector: Connector, generation: u64) {
        {
            let mut state = self.state.lock().unwrap();
            if !state.connected || state.generation != generation {
                return; // lent before a reconnection: the pool was refilled since
            }
            if connector.is_healthy() {
                state.idle.push(connector);
                drop(state);
                self.changed.notify_one();
                return;
            }
        }

        // replaced asynchronously; not possible when the runtime is shutting down
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let pool = self.clone();
            runtime.spawn(async move {
                match Connector::connect_with(&pool.host, pool.port, &pool.options).await {
                    Ok(replacement) => pool.give_back(replacement, generation),
                    Err(_) => pool.mark_disconnected(),
                }
            });
        }
    }

    /// Reads the server's collections, records their versions, and reports the changes.
    async fn check_versions(self: &Arc<Self>) -> bool {
        let mut connector = match self.get().await {
            Ok(connector) => connector,
            Err(_) => return false,
        };

        match connector.get_collections_description().await {
            Ok(description) => {
                drop(connector);
                self.record_versions(
                    description
                        .collections
                        .into_iter()
                        .map(|c| (c.name, c.last_version)),
                );
                true
            }
            Err(e) => {
                if e.is_connection_failure() {
                    self.mark_disconnected();
                }
                false
            }
        }
    }

    /// A version the server is known to have, e.g. just fed to it.
    pub fn record_version(&self, collection: &str, version: &str) {
        self.record_versions(std::iter::once((collection.to_owned(), version.to_owned())));
    }

    fn record_versions(&self, reported: impl Iterator<Item = (String, String)>) {
        let raise_events = std::mem::replace(&mut self.state.lock().unwrap().has_baseline, true);

        let mut changes = Vec::new();
        {
            let mut versions = self.versions.lock().unwrap();
            for (collection, version) in reported {
                if versions.get(&collection) != Some(&version) {
                    versions.insert(collection.clone(), version.clone());
                    changes.push((collection, version));
                }
            }
        }

        // the first reading is the baseline: what already exists is not news
        if raise_events {
            for (collection, version) in changes {
                (self.on_new_version)(&self.address, &collection, &version);
            }
        }
    }

    /// Checks the server every `interval`: reconnects a disconnected pool, refreshes a connected one's versions.
    /// Stops once the pool is dropped (it only holds a weak reference).
    pub fn start_watchdog(self: &Arc<Self>, interval: Duration) {
        let pool = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(pool) = Weak::upgrade(&pool) else {
                    break;
                };
                pool.watchdog_tick(interval).await;
            }
        });
    }

    async fn watchdog_tick(self: &Arc<Self>, interval: Duration) {
        if !self.is_connected() {
            self.connect().await;
            return;
        }

        // all connections busy for a whole interval: the server is evidently answering; check next time
        let _ = tokio::time::timeout(interval, self.check_versions()).await;
    }
}

/// A connection lent by a [`ConnectorPool`], given back when dropped.
pub(crate) struct PooledConnector {
    connector: Option<Connector>,
    pool: Arc<ConnectorPool>,
    generation: u64,
}

impl PooledConnector {
    pub fn pool(&self) -> &Arc<ConnectorPool> {
        &self.pool
    }
}

impl Deref for PooledConnector {
    type Target = Connector;

    fn deref(&self) -> &Connector {
        self.connector
            .as_ref()
            .expect("connector present until dropped")
    }
}

impl DerefMut for PooledConnector {
    fn deref_mut(&mut self) -> &mut Connector {
        self.connector
            .as_mut()
            .expect("connector present until dropped")
    }
}

impl Drop for PooledConnector {
    fn drop(&mut self) {
        if let Some(connector) = self.connector.take() {
            self.pool.give_back(connector, self.generation);
        }
    }
}
