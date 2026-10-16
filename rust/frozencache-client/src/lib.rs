//! Rust client for [FrozenCache](https://github.com/usinesoft/FrozenCache), a feed-and-read cache server.
//!
//! Wire-compatible with the .NET server and client: the encodings are checked byte for byte against the .NET
//! `Messages` project (see the tests of [`protocol`]).
//!
//! ```no_run
//! use frozencache_client::{Connector, Item};
//!
//! # async fn example() -> frozencache_client::Result<()> {
//! let mut connector = Connector::connect("localhost", 5123).await?;
//!
//! // primary key "id", secondary index "customerId"
//! connector.create_collection("orders", "id", &["customerId"]).await?;
//!
//! let orders = vec![Item::new(b"order 1".to_vec(), [1, 42]), Item::new(b"order 2".to_vec(), [2, 42])];
//! connector.feed_collection("orders", "20261014-120000", orders).await?;
//!
//! let documents = connector.query_by_primary_key("orders", &[1, 2]).await?;
//! assert_eq!(documents.len(), 2);
//! # Ok(())
//! # }
//! ```

mod aggregator;
mod connector;
mod error;
mod local_cache;
mod pool;
pub mod protocol;
mod transport;

pub use aggregator::{
    Aggregator, AggregatorOptions, CacheObject, ClusterStream, NewVersion, ReplicaStatus,
};
pub use connector::{
    ConnectOptions, Connector, DEFAULT_PING_TIMEOUT, Document, DocumentStream, Item,
};
pub use error::{Error, Result};
pub use local_cache::CacheStatistics;
pub use protocol::{CollectionInformation, CollectionsDescription};
