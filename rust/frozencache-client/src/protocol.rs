//! Wire protocol of the FrozenCache server, byte-compatible with the .NET `Messages` project.
//!
//! Two encodings share the TCP connection:
//!
//! * **Messages**: an 8-byte little-endian header (`i32` message type, `i32` body length) followed by a
//!   MessagePack body. Every DTO uses integer keys, which MessagePack encodes as a positional array.
//!   `Ping` and `GetCollectionsDescriptionRequest` have no body. Bodies are limited to [`MAX_MESSAGE_BODY`] in
//!   both directions: the server drops the connection on a larger request.
//! * **Batches** of documents, used by feeds (client to server) and streams (server to client): an 8-byte
//!   header (`i32` payload length, `i32` item count), then each item as `i32` size, `i32` key count, the keys as
//!   `i64`, and the raw document bytes. An empty batch (`0, 0`) ends the sequence.

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::error::{Error, Result};

/// Largest message body accepted by the server and by the .NET client.
pub const MAX_MESSAGE_BODY: usize = 1024 * 1024;

/// Items per feed batch, as written by the .NET client.
pub const MAX_ITEMS_PER_BATCH: usize = 5_000;

/// A feed batch is closed as soon as its payload exceeds this size (so it can exceed it by one item).
pub const MAX_BATCH_PAYLOAD: usize = 1_000_000;

/// Sanity limit when reading a batch, so that corrupted data can't trigger a huge allocation. Batches written by
/// the server are around [`MAX_BATCH_PAYLOAD`] plus one document.
const MAX_INCOMING_BATCH_PAYLOAD: usize = 256 * 1024 * 1024;

/// Size of a message header: message type, then body length (both `i32`, little-endian).
pub const HEADER_SIZE: usize = 8;

/// Values of the .NET `MessageType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum MessageType {
    Ping = 1,
    BeginFeedRequest = 2,
    FeedItem = 3,
    CreateCollectionRequest = 5,
    StatusResponse = 6,
    QueryByPrimaryKeyRequest = 7,
    QueryResponse = 8,
    DropCollectionRequest = 9,
    GetCollectionsDescriptionRequest = 10,
    CollectionsDescription = 11,
    StreamAllDataRequest = 12,
    StreamBySecondaryIndexRequest = 13,
}

impl MessageType {
    pub fn from_i32(value: i32) -> Option<Self> {
        use MessageType::*;
        Some(match value {
            1 => Ping,
            2 => BeginFeedRequest,
            3 => FeedItem,
            5 => CreateCollectionRequest,
            6 => StatusResponse,
            7 => QueryByPrimaryKeyRequest,
            8 => QueryResponse,
            9 => DropCollectionRequest,
            10 => GetCollectionsDescriptionRequest,
            11 => CollectionsDescription,
            12 => StreamAllDataRequest,
            13 => StreamBySecondaryIndexRequest,
            _ => return None,
        })
    }
}

// --- message bodies; field order is the MessagePack key order of the .NET DTOs ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateCollectionRequest {
    pub collection_name: String,
    pub primary_key_name: Option<String>,
    pub other_indexes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeginFeedRequest {
    pub collection_name: String,
    pub collection_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryByPrimaryKey {
    pub primary_key_values: Vec<i64>,
    pub collection_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropCollectionRequest {
    pub collection_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamAllDataRequest {
    pub collection_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamBySecondaryIndexRequest {
    pub collection_name: String,
    pub index_name: String,
    pub key_value: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub success: bool,
    pub error_message: Option<String>,
}

/// Answer to a query by primary key. The server sends a single one (`single_answer`); the protocol also allows
/// a sequence of them ended by one without data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultWithData {
    pub single_answer: bool,
    pub objects_data: Vec<ByteBuf>,
    pub collection_name: Option<String>,
}

impl ResultWithData {
    pub fn is_end_marker(&self) -> bool {
        self.objects_data.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionsDescription {
    pub collections: Vec<CollectionInformation>,
}

/// Description of one collection, as returned by [`crate::Connector::get_collections_description`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionInformation {
    pub count: i32,
    pub size_in_bytes: i64,
    pub last_version: String,
    /// Index names; the first one is the primary key.
    pub keys: Vec<String>,
    pub segment_file_size: i32,
    pub max_objects_per_segment: i32,
    pub name: String,
}

impl CollectionInformation {
    pub fn avg_object_size(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.size_in_bytes as f64 / self.count as f64
        }
    }
}

/// A framed message, in either direction.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Ping,
    GetCollectionsDescriptionRequest,
    CreateCollectionRequest(CreateCollectionRequest),
    BeginFeedRequest(BeginFeedRequest),
    QueryByPrimaryKey(QueryByPrimaryKey),
    DropCollectionRequest(DropCollectionRequest),
    StreamAllDataRequest(StreamAllDataRequest),
    StreamBySecondaryIndexRequest(StreamBySecondaryIndexRequest),
    StatusResponse(StatusResponse),
    QueryResponse(ResultWithData),
    CollectionsDescription(CollectionsDescription),
}

impl Message {
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::Ping => MessageType::Ping,
            Message::GetCollectionsDescriptionRequest => {
                MessageType::GetCollectionsDescriptionRequest
            }
            Message::CreateCollectionRequest(_) => MessageType::CreateCollectionRequest,
            Message::BeginFeedRequest(_) => MessageType::BeginFeedRequest,
            Message::QueryByPrimaryKey(_) => MessageType::QueryByPrimaryKeyRequest,
            Message::DropCollectionRequest(_) => MessageType::DropCollectionRequest,
            Message::StreamAllDataRequest(_) => MessageType::StreamAllDataRequest,
            Message::StreamBySecondaryIndexRequest(_) => MessageType::StreamBySecondaryIndexRequest,
            Message::StatusResponse(_) => MessageType::StatusResponse,
            Message::QueryResponse(_) => MessageType::QueryResponse,
            Message::CollectionsDescription(_) => MessageType::CollectionsDescription,
        }
    }

    fn encode_body(&self) -> Result<Vec<u8>> {
        let body = match self {
            Message::Ping | Message::GetCollectionsDescriptionRequest => Ok(Vec::new()),
            Message::CreateCollectionRequest(m) => rmp_serde::to_vec(m),
            Message::BeginFeedRequest(m) => rmp_serde::to_vec(m),
            Message::QueryByPrimaryKey(m) => rmp_serde::to_vec(m),
            Message::DropCollectionRequest(m) => rmp_serde::to_vec(m),
            Message::StreamAllDataRequest(m) => rmp_serde::to_vec(m),
            Message::StreamBySecondaryIndexRequest(m) => rmp_serde::to_vec(m),
            Message::StatusResponse(m) => rmp_serde::to_vec(m),
            Message::QueryResponse(m) => rmp_serde::to_vec(m),
            Message::CollectionsDescription(m) => rmp_serde::to_vec(m),
        };
        body.map_err(|e| Error::Protocol(format!("cannot encode {:?}: {e}", self.message_type())))
    }

    /// Header and body, ready to write. Fails, without side effect, for a body the server would refuse.
    pub fn encode_frame(&self) -> Result<Vec<u8>> {
        let body = self.encode_body()?;
        if body.len() > MAX_MESSAGE_BODY {
            return Err(Error::InvalidArgument(format!(
                "{:?} too large: {} bytes, the server accepts at most {MAX_MESSAGE_BODY}",
                self.message_type(),
                body.len()
            )));
        }

        let mut frame = Vec::with_capacity(HEADER_SIZE + body.len());
        frame.extend_from_slice(&(self.message_type() as i32).to_le_bytes());
        frame.extend_from_slice(&(body.len() as i32).to_le_bytes());
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    /// Parses a message header: the message type and the body length.
    pub fn decode_header(header: [u8; HEADER_SIZE]) -> Result<(MessageType, usize)> {
        let raw_type = i32::from_le_bytes(header[0..4].try_into().unwrap());
        let size = i32::from_le_bytes(header[4..8].try_into().unwrap());

        let message_type = MessageType::from_i32(raw_type)
            .ok_or_else(|| Error::Protocol(format!("unknown message type {raw_type}")))?;
        if size < 0 || size as usize > MAX_MESSAGE_BODY {
            return Err(Error::Protocol(format!("invalid message size {size}")));
        }
        Ok((message_type, size as usize))
    }

    pub fn decode_body(message_type: MessageType, body: &[u8]) -> Result<Message> {
        fn parse<'a, T: Deserialize<'a>>(message_type: MessageType, body: &'a [u8]) -> Result<T> {
            rmp_serde::from_slice(body)
                .map_err(|e| Error::Protocol(format!("cannot decode {message_type:?}: {e}")))
        }

        Ok(match message_type {
            MessageType::Ping => Message::Ping,
            MessageType::GetCollectionsDescriptionRequest => {
                Message::GetCollectionsDescriptionRequest
            }
            MessageType::CreateCollectionRequest => {
                Message::CreateCollectionRequest(parse(message_type, body)?)
            }
            MessageType::BeginFeedRequest => Message::BeginFeedRequest(parse(message_type, body)?),
            MessageType::QueryByPrimaryKeyRequest => {
                Message::QueryByPrimaryKey(parse(message_type, body)?)
            }
            MessageType::DropCollectionRequest => {
                Message::DropCollectionRequest(parse(message_type, body)?)
            }
            MessageType::StreamAllDataRequest => {
                Message::StreamAllDataRequest(parse(message_type, body)?)
            }
            MessageType::StreamBySecondaryIndexRequest => {
                Message::StreamBySecondaryIndexRequest(parse(message_type, body)?)
            }
            MessageType::StatusResponse => Message::StatusResponse(parse(message_type, body)?),
            MessageType::QueryResponse => Message::QueryResponse(parse(message_type, body)?),
            MessageType::CollectionsDescription => {
                Message::CollectionsDescription(parse(message_type, body)?)
            }
            MessageType::FeedItem => {
                return Err(Error::Protocol(
                    "FeedItem is only sent inside batches".into(),
                ));
            }
        })
    }
}

// --- batches ---

/// Builds feed batches: items are appended until [`MAX_ITEMS_PER_BATCH`] items or more than
/// [`MAX_BATCH_PAYLOAD`] bytes, like the .NET client. Each batch is one buffer, so it goes out in a single write
/// (one TLS record instead of one per item).
pub(crate) struct BatchWriter {
    buffer: Vec<u8>,
    count: usize,
}

impl BatchWriter {
    pub fn new() -> Self {
        let mut writer = BatchWriter {
            buffer: Self::buffer(),
            count: 0,
        };
        writer.reset();
        writer
    }

    /// A buffer large enough for a full batch without reallocation.
    pub fn buffer() -> Vec<u8> {
        Vec::with_capacity(HEADER_SIZE + 2 * MAX_BATCH_PAYLOAD)
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.buffer.extend_from_slice(&[0; HEADER_SIZE]);
        self.count = 0;
    }

    /// Appends an item; true when the batch is full and must be written.
    pub fn push(&mut self, keys: &[i64], data: &[u8]) -> Result<bool> {
        let size = 4 + keys.len() * 8 + data.len();
        let size = i32::try_from(size).map_err(|_| {
            Error::InvalidArgument(format!("document too large: {} bytes", data.len()))
        })?;

        self.buffer.extend_from_slice(&size.to_le_bytes());
        self.buffer
            .extend_from_slice(&(keys.len() as i32).to_le_bytes());
        for key in keys {
            self.buffer.extend_from_slice(&key.to_le_bytes());
        }
        self.buffer.extend_from_slice(data);
        self.count += 1;

        Ok(self.count >= MAX_ITEMS_PER_BATCH || self.payload_len() > MAX_BATCH_PAYLOAD)
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn payload_len(&self) -> usize {
        self.buffer.len() - HEADER_SIZE
    }

    /// Hands over the complete batch (header included) and continues in `next`, a recycled buffer, so that a batch
    /// can be written while the next one is built.
    pub fn take(&mut self, next: Vec<u8>) -> Vec<u8> {
        self.finish();
        let full = std::mem::replace(&mut self.buffer, next);
        self.reset();
        full
    }

    /// The complete batch (header included), ready to write.
    pub fn finish(&mut self) -> &[u8] {
        let payload = self.payload_len() as i32;
        self.buffer[0..4].copy_from_slice(&payload.to_le_bytes());
        self.buffer[4..8].copy_from_slice(&(self.count as i32).to_le_bytes());
        &self.buffer
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.reset();
    }
}

/// The empty batch ending a feed or a stream.
pub(crate) const END_OF_BATCHES: [u8; HEADER_SIZE] = [0; HEADER_SIZE];

/// Parses a batch header: payload length and item count; `None` for the end marker.
pub(crate) fn decode_batch_header(header: [u8; HEADER_SIZE]) -> Result<Option<(usize, usize)>> {
    let size = i32::from_le_bytes(header[0..4].try_into().unwrap());
    let count = i32::from_le_bytes(header[4..8].try_into().unwrap());

    if size == 0 && count == 0 {
        return Ok(None);
    }
    if size <= 0 || count <= 0 || size as usize > MAX_INCOMING_BATCH_PAYLOAD {
        return Err(Error::Protocol(format!(
            "invalid batch header: {size} bytes, {count} items"
        )));
    }
    Ok(Some((size as usize, count as usize)))
}

/// One decoded batch item: its keys and its data.
pub(crate) type BatchItem = (Vec<i64>, Vec<u8>);

pub(crate) fn decode_batch_payload(payload: &[u8], count: usize) -> Result<Vec<BatchItem>> {
    fn malformed() -> Error {
        Error::Protocol("malformed batch".into())
    }
    fn read_i32(bytes: &[u8], offset: usize) -> Result<i32> {
        bytes
            .get(offset..offset + 4)
            .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
            .ok_or_else(malformed)
    }

    let mut items = Vec::with_capacity(count);
    let mut offset = 0;

    for _ in 0..count {
        let size = usize::try_from(read_i32(payload, offset)?).map_err(|_| malformed())?;
        let item = payload
            .get(offset + 4..offset + 4 + size)
            .ok_or_else(malformed)?;

        let key_count = usize::try_from(read_i32(item, 0)?).map_err(|_| malformed())?;
        let keys_end = 4 + key_count.checked_mul(8).ok_or_else(malformed)?;
        let key_bytes = item.get(4..keys_end).ok_or_else(malformed)?;
        let keys = key_bytes
            .chunks_exact(8)
            .map(|k| i64::from_le_bytes(k.try_into().unwrap()))
            .collect();

        items.push((keys, item[keys_end..].to_vec()));
        offset += 4 + size;
    }

    Ok(items)
}

#[cfg(test)]
mod tests {
    //! Reference encodings produced by the .NET `Messages` code (`StreamingHelper.WriteMessageAsync` and
    //! `FeedItemBatchSerializer`), so that any difference with the .NET wire format fails here.

    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Encoding matches the .NET bytes, and decoding them gives back the message.
    fn assert_wire_compatible(message: Message, dotnet_frame: &str) {
        let expected = hex(dotnet_frame);
        assert_eq!(
            message.encode_frame().unwrap(),
            expected,
            "{:?}",
            message.message_type()
        );

        let (message_type, size) =
            Message::decode_header(expected[..HEADER_SIZE].try_into().unwrap()).unwrap();
        assert_eq!(size, expected.len() - HEADER_SIZE);
        assert_eq!(
            Message::decode_body(message_type, &expected[HEADER_SIZE..]).unwrap(),
            message
        );
    }

    #[test]
    fn messages_without_body() {
        assert_wire_compatible(Message::Ping, "0100000000000000");
        assert_wire_compatible(
            Message::GetCollectionsDescriptionRequest,
            "0a00000000000000",
        );
    }

    #[test]
    fn requests() {
        assert_wire_compatible(
            Message::CreateCollectionRequest(CreateCollectionRequest {
                collection_name: "orders".into(),
                primary_key_name: Some("id".into()),
                other_indexes: vec!["customerId".into(), "status".into()],
            }),
            "050000001e00000093a66f7264657273a2696492aa637573746f6d65724964a6737461747573",
        );
        assert_wire_compatible(
            Message::BeginFeedRequest(BeginFeedRequest {
                collection_name: "orders".into(),
                collection_version: Some("20261014-120000".into()),
            }),
            "020000001800000092a66f7264657273af32303236313031342d313230303030",
        );
        assert_wire_compatible(
            Message::QueryByPrimaryKey(QueryByPrimaryKey {
                primary_key_values: vec![1, 300, -5, 5_000_000_000, i64::MIN],
                collection_name: Some("orders".into()),
            }),
            "0700000020000000929501cd012cfbcf000000012a05f200d38000000000000000a66f7264657273",
        );
        assert_wire_compatible(
            Message::DropCollectionRequest(DropCollectionRequest {
                collection_name: "orders".into(),
            }),
            "090000000800000091a66f7264657273",
        );
        assert_wire_compatible(
            Message::StreamAllDataRequest(StreamAllDataRequest {
                collection_name: "orders".into(),
            }),
            "0c0000000800000091a66f7264657273",
        );
        assert_wire_compatible(
            Message::StreamBySecondaryIndexRequest(StreamBySecondaryIndexRequest {
                collection_name: "orders".into(),
                index_name: "customerId".into(),
                key_value: 42,
            }),
            "0d0000001400000093a66f7264657273aa637573746f6d657249642a",
        );
    }

    #[test]
    fn responses() {
        assert_wire_compatible(
            Message::StatusResponse(StatusResponse {
                success: true,
                error_message: None,
            }),
            "060000000300000092c3c0",
        );
        assert_wire_compatible(
            Message::StatusResponse(StatusResponse {
                success: false,
                error_message: Some("Collection orders does not exist".into()),
            }),
            "060000002400000092c2d920436f6c6c656374696f6e206f726465727320646f6573206e6f74206578697374",
        );

        let mut result = String::from("080000004001000093c393c403010203c400c5012c");
        result.push_str(&"00".repeat(300));
        result.push_str("a66f7264657273");
        assert_wire_compatible(
            Message::QueryResponse(ResultWithData {
                single_answer: true,
                objects_data: vec![
                    ByteBuf::from(vec![1, 2, 3]),
                    ByteBuf::new(),
                    ByteBuf::from(vec![0; 300]),
                ],
                collection_name: Some("orders".into()),
            }),
            &result,
        );

        assert_wire_compatible(
            Message::CollectionsDescription(CollectionsDescription {
                collections: vec![
                    CollectionInformation {
                        count: 120_000,
                        size_in_bytes: 36_000_000,
                        last_version: "20261014-120000".into(),
                        keys: vec!["id".into(), "name".into()],
                        segment_file_size: 1_000_000_000,
                        max_objects_per_segment: 2_000_000,
                        name: "orders".into(),
                    },
                    CollectionInformation {
                        count: 0,
                        size_in_bytes: 0,
                        last_version: String::new(),
                        keys: vec!["id".into()],
                        segment_file_size: 1024,
                        max_objects_per_segment: 10,
                        name: "empty".into(),
                    },
                ],
            }),
            "0b00000049000000919297ce0001d4c0ce02255100af32303236313031342d31323030303092a26964a46e616d65\
             ce3b9aca00ce001e8480a66f7264657273970000a091a26964cd04000aa5656d707479",
        );
    }

    #[test]
    fn feed_batch() {
        // one batch of two items, then the end marker
        let expected = hex(
            "2a00000002000000160000000200000001000000000000002a00000000000000aabb0c00000001000000\
             f9ffffffffffffff0000000000000000",
        );

        let mut writer = BatchWriter::new();
        assert!(!writer.push(&[1, 42], &[0xAA, 0xBB]).unwrap());
        assert!(!writer.push(&[-7], &[]).unwrap());
        let mut encoded = writer.finish().to_vec();
        encoded.extend_from_slice(&END_OF_BATCHES);
        assert_eq!(encoded, expected);

        let (size, count) = decode_batch_header(expected[..8].try_into().unwrap())
            .unwrap()
            .unwrap();
        let items = decode_batch_payload(&expected[8..8 + size], count).unwrap();
        assert_eq!(
            items,
            vec![(vec![1, 42], vec![0xAA, 0xBB]), (vec![-7], vec![])]
        );
        assert_eq!(
            decode_batch_header(expected[8 + size..].try_into().unwrap()).unwrap(),
            None
        );
    }

    #[test]
    fn batches_are_split_like_the_dotnet_client() {
        let mut writer = BatchWriter::new();
        for i in 0..MAX_ITEMS_PER_BATCH - 1 {
            assert!(!writer.push(&[i as i64], &[0; 10]).unwrap());
        }
        assert!(writer.push(&[0], &[0; 10]).unwrap(), "full at 5,000 items");

        writer.clear();
        assert!(
            !writer
                .push(&[0], &vec![0; MAX_BATCH_PAYLOAD - 100])
                .unwrap()
        );
        assert!(
            writer.push(&[1], &[0; 200]).unwrap(),
            "full once the payload exceeds 1 MB"
        );
    }

    #[test]
    fn oversized_or_malformed_data_is_rejected() {
        let too_many_keys = Message::QueryByPrimaryKey(QueryByPrimaryKey {
            primary_key_values: vec![i64::MAX; 200_000],
            collection_name: Some("orders".into()),
        });
        assert!(matches!(
            too_many_keys.encode_frame(),
            Err(Error::InvalidArgument(_))
        ));

        let mut oversized = [0u8; 8];
        oversized[0] = 6;
        oversized[4..8].copy_from_slice(&(MAX_MESSAGE_BODY as i32 + 1).to_le_bytes());
        assert!(Message::decode_header(oversized).is_err());

        assert!(
            decode_batch_payload(&[5, 0, 0, 0, 1], 1).is_err(),
            "truncated item"
        );
        assert!(
            decode_batch_payload(&[12, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 1).is_err(),
            "too many keys"
        );
    }
}
