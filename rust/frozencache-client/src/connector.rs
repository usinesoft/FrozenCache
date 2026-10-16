use std::collections::VecDeque;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::protocol::{
    BatchWriter, BeginFeedRequest, CollectionsDescription, CreateCollectionRequest,
    DropCollectionRequest, END_OF_BATCHES, HEADER_SIZE, Message, QueryByPrimaryKey, StatusResponse,
    StreamAllDataRequest, StreamBySecondaryIndexRequest, decode_batch_header, decode_batch_payload,
};
use crate::transport::{self, Transport};

/// How long [`Connector::ping`] waits for the answer.
pub const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(1);

/// Options of [`Connector::connect_with`].
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Wrap the connection in TLS. The server must have `ServerSettings:UseSsl` enabled too.
    pub use_tls: bool,
    /// When true (the default), the server certificate must be trusted by the operating system and match the
    /// host. Set to false only for testing against a self-signed certificate: it disables all certificate checks.
    pub validate_server_certificate: bool,
    pub connect_timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        ConnectOptions {
            use_tls: false,
            validate_server_certificate: true,
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// A document fed to a collection: raw data and its keys, the primary key first, then the secondary keys in the
/// order the indexes were declared by [`Connector::create_collection`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub keys: Vec<i64>,
    pub data: Vec<u8>,
}

impl Item {
    pub fn new(data: impl Into<Vec<u8>>, keys: impl Into<Vec<i64>>) -> Self {
        Item {
            keys: keys.into(),
            data: data.into(),
        }
    }
}

/// A document returned by a stream, with its primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub primary_key: i64,
    pub data: Vec<u8>,
}

/// A single connection to a FrozenCache server: the Rust counterpart of the .NET `CacheClient.Connector`, and
/// wire-compatible with the same server.
///
/// Every operation takes `&mut self`, so one connection carries one request at a time: use one connector per
/// concurrent operation. A connection left in an unknown state - an I/O error or timeout part way through a
/// request, a stream dropped before its end - is closed at once and refuses further requests with
/// [`Error::Unusable`], instead of misreading leftover data; [`Connector::is_healthy`] tells whether it can still
/// be used. Closing it immediately also matters to the server: a stream abandoned on an open connection would
/// keep it writing, and holding the collection's version, until the connector is dropped.
pub struct Connector {
    address: String,
    /// `None` once closed.
    stream: Option<BufStream<Box<dyn Transport>>>,
    /// The document stream in progress, if any: kept here rather than in [`DocumentStream`] so that a stream can
    /// also be owned together with its connector (by the Aggregator).
    streaming: Option<StreamState>,
}

struct StreamState {
    pending: VecDeque<Document>,
    finished: bool,
}

impl Connector {
    /// Connects without TLS.
    pub async fn connect(host: &str, port: u16) -> Result<Connector> {
        Self::connect_with(host, port, &ConnectOptions::default()).await
    }

    pub async fn connect_with(
        host: &str,
        port: u16,
        options: &ConnectOptions,
    ) -> Result<Connector> {
        let transport = transport::open(
            host,
            port,
            options.use_tls,
            options.validate_server_certificate,
            options.connect_timeout,
        )
        .await?;

        Ok(Connector {
            address: format!("{host}:{port}"),
            stream: Some(BufStream::new(transport)),
            streaming: None,
        })
    }

    /// `host:port`, as given to connect.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// False once the connection has been closed because it was left in an unknown state; it then refuses every
    /// request.
    pub fn is_healthy(&self) -> bool {
        self.stream.is_some()
    }

    /// Closes the connection: dropping the transport closes the socket.
    pub(crate) fn close(&mut self) {
        self.stream = None;
        self.streaming = None;
    }

    fn io(&mut self) -> Result<&mut BufStream<Box<dyn Transport>>> {
        self.stream.as_mut().ok_or(Error::Unusable)
    }

    /// Creates a collection, with a primary key and optional secondary indexes. Succeeds without change if the
    /// collection already exists with the same keys; fails if it exists with different ones.
    pub async fn create_collection(
        &mut self,
        collection: &str,
        primary_key: &str,
        other_indexes: &[&str],
    ) -> Result<()> {
        let request = Message::CreateCollectionRequest(CreateCollectionRequest {
            collection_name: collection.to_owned(),
            primary_key_name: Some(primary_key.to_owned()),
            other_indexes: other_indexes.iter().map(|s| (*s).to_owned()).collect(),
        });

        let response = self.request(&request).await?;
        expect_success(response, "create collection")
    }

    pub async fn get_collections_description(&mut self) -> Result<CollectionsDescription> {
        match self
            .request(&Message::GetCollectionsDescriptionRequest)
            .await?
        {
            Message::CollectionsDescription(description) => Ok(description),
            other => Err(self.unexpected(other)),
        }
    }

    /// Removes a collection, every version and its metadata. With `ignore_if_not_found`, an error reported by the
    /// server (typically: no such collection) is ignored, as in the .NET client.
    pub async fn drop_collection(
        &mut self,
        collection: &str,
        ignore_if_not_found: bool,
    ) -> Result<()> {
        let request = Message::DropCollectionRequest(DropCollectionRequest {
            collection_name: collection.to_owned(),
        });

        match self.request(&request).await? {
            Message::StatusResponse(StatusResponse { success: false, .. })
                if ignore_if_not_found =>
            {
                Ok(())
            }
            response => expect_success(response, "drop collection"),
        }
    }

    /// Feeds a new version of a collection. The items are streamed in batches, so they don't need to be in
    /// memory at once. The new version replaces the active one once the server has stored and indexed all of it;
    /// version names must sort after the current one (as an ordinal string comparison).
    ///
    /// An error reported by the server before the upload (unknown collection, version conflict) leaves the
    /// connection usable; a failure during the upload doesn't.
    pub async fn feed_collection<I>(
        &mut self,
        collection: &str,
        version: &str,
        items: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Item>,
    {
        let begin = Message::BeginFeedRequest(BeginFeedRequest {
            collection_name: collection.to_owned(),
            collection_version: Some(version.to_owned()),
        });

        // the server validates the request before receiving data: this also catches a TLS mismatch before the
        // whole upload
        let response = self.request(&begin).await?;
        expect_success(response, "feed collection")?;

        let upload = self.upload(items).await;
        self.track(upload)?;

        let response = self.receive().await?;
        expect_success(response, "feed collection")
    }

    /// Builds the batches while a writer task sends the previous ones, recycling two buffers between them.
    /// Building and writing one after the other made a 2M-item feed about twice as slow as the .NET client: the
    /// socket sat idle while each batch was built, whereas .NET's overlapped sends keep it busy.
    async fn upload<I: IntoIterator<Item = Item>>(&mut self, items: I) -> Result<()> {
        let mut stream = self.stream.take().ok_or(Error::Unusable)?;

        let (batches, mut to_write) = mpsc::channel::<Vec<u8>>(1);
        let (recycle, mut recycled) = mpsc::channel::<Vec<u8>>(2);

        let writer = tokio::spawn(async move {
            while let Some(batch) = to_write.recv().await {
                stream.write_all(&batch).await?;
                let _ = recycle.try_send(batch);
            }
            // only reached when every batch was handed over: an aborted upload never sends the end marker
            stream.write_all(&END_OF_BATCHES).await?;
            stream.flush().await?;
            Ok::<_, std::io::Error>(stream)
        });

        let encoded: Result<()> = async {
            let mut batch = BatchWriter::new();
            for item in items {
                if batch.push(&item.keys, &item.data)? {
                    let next = recycled
                        .try_recv()
                        .unwrap_or_else(|_| BatchWriter::buffer());
                    if batches.send(batch.take(next)).await.is_err() {
                        return Ok(()); // the writer failed: its error is reported below
                    }
                }
            }
            if !batch.is_empty() {
                let _ = batches.send(batch.take(Vec::new())).await;
            }
            Ok(())
        }
        .await;

        if let Err(e) = encoded {
            // Without the end marker the server discards the partial version; the connection, in the middle of
            // a feed, can't be used any more (it's closed with the writer).
            writer.abort();
            return Err(e);
        }
        drop(batches);

        match writer.await {
            Ok(Ok(stream)) => {
                self.stream = Some(stream);
                Ok(())
            }
            Ok(Err(e)) => Err(e.into()),
            Err(e) => Err(Error::Protocol(format!("feed writer task failed: {e}"))),
        }
    }

    /// First step of a feed session driven batch by batch (by the Aggregator, which encodes the batches once for
    /// every replica): the server validates the request and acknowledges.
    pub(crate) async fn begin_feed(&mut self, collection: &str, version: &str) -> Result<()> {
        let begin = Message::BeginFeedRequest(BeginFeedRequest {
            collection_name: collection.to_owned(),
            collection_version: Some(version.to_owned()),
        });
        let response = self.request(&begin).await?;
        expect_success(response, "feed collection")
    }

    /// Writes one encoded batch of the feed session started by [`Connector::begin_feed`].
    pub(crate) async fn write_batch(&mut self, batch: &[u8]) -> Result<()> {
        let result = async {
            self.io()?.write_all(batch).await?;
            Ok(())
        }
        .await;
        self.track(result)
    }

    /// Ends the feed session: the end marker, then the server's answer once the version is stored and indexed.
    pub(crate) async fn end_feed(&mut self) -> Result<()> {
        let result = async {
            let stream = self.io()?;
            stream.write_all(&END_OF_BATCHES).await?;
            stream.flush().await?;
            Ok(())
        }
        .await;
        self.track(result)?;

        let response = self.receive().await?;
        expect_success(response, "feed collection")
    }

    /// Documents whose primary key is one of `keys`, in no particular order; a key matching no document adds
    /// nothing. The request must stay under the server's 1 MB message limit, about 100,000 keys.
    pub async fn query_by_primary_key(
        &mut self,
        collection: &str,
        keys: &[i64],
    ) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "at least one key is required".into(),
            ));
        }

        let request = Message::QueryByPrimaryKey(QueryByPrimaryKey {
            primary_key_values: keys.to_vec(),
            collection_name: Some(collection.to_owned()),
        });
        self.send(&request).await?;

        // the server sends a single answer; the protocol also allows a sequence ended by an empty one
        let mut documents = Vec::new();
        loop {
            match self.receive().await? {
                Message::QueryResponse(result) => {
                    let end = result.is_end_marker() || result.single_answer;
                    documents.extend(result.objects_data.into_iter().map(|d| d.into_vec()));
                    if end {
                        return Ok(documents);
                    }
                }
                Message::StatusResponse(status) if !status.success => {
                    return Err(server_error("query", status));
                }
                Message::StatusResponse(_) => return Ok(documents),
                other => return Err(self.unexpected(other)),
            }
        }
    }

    /// Streams every document of the collection's active version, in batches.
    pub async fn stream_all_data(&mut self, collection: &str) -> Result<DocumentStream<'_>> {
        let request = Message::StreamAllDataRequest(StreamAllDataRequest {
            collection_name: collection.to_owned(),
        });
        self.start_stream(&request, "stream collection").await
    }

    /// Streams every document of the collection's active version whose value for the index `index` equals `key`.
    /// Works for the primary key too.
    pub async fn stream_by_secondary_index(
        &mut self,
        collection: &str,
        index: &str,
        key: i64,
    ) -> Result<DocumentStream<'_>> {
        let request = Message::StreamBySecondaryIndexRequest(StreamBySecondaryIndexRequest {
            collection_name: collection.to_owned(),
            index_name: index.to_owned(),
            key_value: key,
        });
        self.start_stream(&request, "stream by secondary index")
            .await
    }

    async fn start_stream(
        &mut self,
        request: &Message,
        operation: &'static str,
    ) -> Result<DocumentStream<'_>> {
        self.begin_stream(request, operation).await?;
        Ok(DocumentStream { connector: self })
    }

    /// Sends a stream request; once acknowledged, the documents are read with [`Connector::next_document`].
    pub(crate) async fn begin_stream(
        &mut self,
        request: &Message,
        operation: &'static str,
    ) -> Result<()> {
        // the server acknowledges (or reports an error) before switching to batches
        let response = self.request(request).await?;
        expect_success(response, operation)?;
        self.streaming = Some(StreamState {
            pending: VecDeque::new(),
            finished: false,
        });
        Ok(())
    }

    /// The next document of the stream in progress; `None` at its end, or after an error.
    pub(crate) async fn next_document(&mut self) -> Option<Result<Document>> {
        loop {
            let state = self.streaming.as_mut()?;
            if let Some(document) = state.pending.pop_front() {
                return Some(Ok(document));
            }
            if state.finished {
                self.streaming = None;
                return None;
            }
            if let Err(e) = self.read_batch().await {
                self.close();
                return Some(Err(e));
            }
        }
    }

    /// Ends the stream in progress. If it wasn't read to its end, unread data remains: the connection is closed,
    /// which also stops the server's streaming.
    pub(crate) fn abandon_stream(&mut self) {
        if let Some(state) = &self.streaming
            && !(state.finished && state.pending.is_empty())
        {
            self.close();
        }
        self.streaming = None;
    }

    async fn read_batch(&mut self) -> Result<()> {
        let result = async {
            let mut header = [0; HEADER_SIZE];
            self.read_exact(&mut header).await?;

            let Some((size, count)) = decode_batch_header(header)? else {
                if let Some(state) = self.streaming.as_mut() {
                    state.finished = true;
                }
                return Ok(());
            };

            let mut payload = vec![0; size];
            self.read_exact(&mut payload).await?;

            let documents = decode_batch_payload(&payload, count)?
                .into_iter()
                .map(|(keys, data)| {
                    let primary_key = *keys
                        .first()
                        .ok_or_else(|| Error::Protocol("streamed document without a key".into()))?;
                    Ok(Document { primary_key, data })
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(state) = self.streaming.as_mut() {
                state.pending.extend(documents);
            }
            Ok(())
        }
        .await;
        self.track(result)
    }

    /// True if the server answered within [`DEFAULT_PING_TIMEOUT`].
    pub async fn ping(&mut self) -> bool {
        self.ping_with_timeout(DEFAULT_PING_TIMEOUT).await
    }

    /// True if the server answered within `timeout`. A late answer would arrive in place of the next response,
    /// so after a timeout the connection is no longer usable.
    pub async fn ping_with_timeout(&mut self, timeout: Duration) -> bool {
        match tokio::time::timeout(timeout, self.request(&Message::Ping)).await {
            Ok(Ok(Message::Ping)) => true,
            Ok(_) => false,
            Err(_) => {
                self.close();
                false
            }
        }
    }

    // --- framing ---

    async fn request(&mut self, message: &Message) -> Result<Message> {
        self.send(message).await?;
        self.receive().await
    }

    async fn send(&mut self, message: &Message) -> Result<()> {
        self.io()?;
        if self.streaming.is_some() {
            // only reachable from inside the crate: a DocumentStream borrows the connector
            return Err(Error::Protocol(
                "a stream is in progress on this connection".into(),
            ));
        }
        // encoded before anything is written: an invalid request leaves the connection usable
        let frame = message.encode_frame()?;

        let result = async {
            let stream = self.io()?;
            stream.write_all(&frame).await?;
            stream.flush().await?;
            Ok(())
        }
        .await;
        self.track(result)
    }

    async fn receive(&mut self) -> Result<Message> {
        let result = async {
            let mut header = [0; HEADER_SIZE];
            self.read_exact(&mut header).await?;
            let (message_type, size) = Message::decode_header(header)?;

            let mut body = vec![0; size];
            self.read_exact(&mut body).await?;
            Message::decode_body(message_type, &body)
        }
        .await;
        self.track(result)
    }

    /// Like `read_exact`, but the server closing the connection is [`Error::ConnectionClosed`].
    async fn read_exact(&mut self, buffer: &mut [u8]) -> Result<()> {
        match self.io()?.read_exact(buffer).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(Error::ConnectionClosed),
            Err(e) => Err(e.into()),
        }
    }

    /// A request failing part way leaves unread or unwritten data: the connection is closed. Errors raised before
    /// anything is sent, or reported by the server, don't count.
    fn track<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(e) = &result
            && !matches!(
                e,
                Error::InvalidArgument(_) | Error::Server { .. } | Error::Unusable
            )
        {
            self.close();
        }
        result
    }

    /// An answer of the wrong type means the two sides are out of step.
    fn unexpected(&mut self, response: Message) -> Error {
        self.close();
        Error::UnexpectedResponse(response.message_type())
    }
}

fn expect_success(response: Message, operation: &'static str) -> Result<()> {
    match response {
        Message::StatusResponse(status) if status.success => Ok(()),
        Message::StatusResponse(status) => Err(server_error(operation, status)),
        other => Err(Error::UnexpectedResponse(other.message_type())),
    }
}

fn server_error(operation: &'static str, status: StatusResponse) -> Error {
    Error::Server {
        operation,
        message: status.error_message.unwrap_or_default(),
    }
}

/// Documents streamed by [`Connector::stream_all_data`] or [`Connector::stream_by_secondary_index`]:
///
/// ```no_run
/// # async fn example(connector: &mut frozencache_client::Connector) -> frozencache_client::Result<()> {
/// let mut documents = connector.stream_by_secondary_index("orders", "customerId", 42).await?;
/// while let Some(document) = documents.next().await {
///     let document = document?;
///     println!("{}: {} bytes", document.primary_key, document.data.len());
/// }
/// # Ok(())
/// # }
/// ```
///
/// It borrows the connector until dropped. Dropping it before the end leaves unread data on the connection, so
/// the connection is closed (which also stops the server's streaming) and refuses further requests
/// ([`Error::Unusable`]).
pub struct DocumentStream<'a> {
    connector: &'a mut Connector,
}

impl DocumentStream<'_> {
    /// The next document; `None` at the end of the stream, or after an error.
    pub async fn next(&mut self) -> Option<Result<Document>> {
        self.connector.next_document().await
    }

    /// Reads the rest of the stream.
    pub async fn collect_all(mut self) -> Result<Vec<Document>> {
        let mut documents = Vec::new();
        while let Some(document) = self.next().await {
            documents.push(document?);
        }
        Ok(documents)
    }
}

impl Drop for DocumentStream<'_> {
    fn drop(&mut self) {
        self.connector.abandon_stream();
    }
}
