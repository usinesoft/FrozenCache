use std::io;
use std::time::Duration;

use crate::protocol::MessageType;

pub type Result<T> = std::result::Result<T, Error>;

/// Errors of the FrozenCache client.
///
/// [`Error::is_connection_failure`] separates failures of the connection itself, after which the operation may
/// succeed on another replica, from errors reported by the server, which would occur on any replica.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not connect to {address}: {source}")]
    Connect {
        address: String,
        #[source]
        source: io::Error,
    },

    #[error(
        "SSL handshake with {address} failed: {message}. This client has TLS enabled - check that the server \
         has ServerSettings:UseSsl enabled with a valid certificate."
    )]
    Tls { address: String, message: String },

    /// The server closed the connection instead of answering: it stopped or crashed, or the client and server
    /// disagree on TLS. Same meaning as the .NET client's `ConnectionClosedException`.
    #[error(
        "No response received from the server (the connection was closed). This often means an SSL mismatch \
         between client and server - check that this connector's TLS setting matches ServerSettings:UseSsl on \
         the server."
    )]
    ConnectionClosed,

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("timed out after {0:?}")]
    Timeout(Duration),

    /// An error reported by the server (unknown collection or index, schema mismatch, version conflict...).
    #[error("{operation} failed: {message}")]
    Server {
        operation: &'static str,
        message: String,
    },

    #[error("unexpected response from the server: {0:?}")]
    UnexpectedResponse(MessageType),

    /// Malformed data on the wire, or a message exceeding the protocol's size limits.
    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("{0}")]
    InvalidArgument(String),

    /// The connection is in an unknown state and refuses further requests: a previous request failed part way,
    /// or a document stream was dropped before its end and unread data remains. Open a new connection.
    #[error(
        "this connection can no longer be used (a previous request failed part way, or a stream was not read \
         to its end); open a new one"
    )]
    Unusable,

    /// A replica of an [`crate::Aggregator`] is not connected (its watchdog reconnects it when it's back).
    #[error("server {0} is not connected")]
    NotConnected(String),

    /// No replica of an [`crate::Aggregator`] is connected.
    #[error("no server is connected")]
    NoServerAvailable,

    /// An error on one replica of an [`crate::Aggregator`] operation that involves several.
    #[error("{address}: {source}")]
    Replica {
        address: String,
        #[source]
        source: Box<Error>,
    },

    /// A document couldn't be converted from its stored bytes ([`crate::CacheObject::from_bytes`]).
    #[error("cannot deserialize a document: {0}")]
    Deserialize(String),
}

impl Error {
    /// True for failures of the connection itself, as opposed to errors reported by the server.
    pub fn is_connection_failure(&self) -> bool {
        match self {
            Error::Connect { .. }
            | Error::Tls { .. }
            | Error::ConnectionClosed
            | Error::Io(_)
            | Error::Timeout(_)
            | Error::NotConnected(_)
            | Error::NoServerAvailable => true,
            Error::Replica { source, .. } => source.is_connection_failure(),
            _ => false,
        }
    }
}
