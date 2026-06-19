//! Broker connection abstraction.
//!
//! [`BrokerConnection`] is the trait every broker implements; the per-connection
//! actor drives it generically so the rest of the app is broker-agnostic. The
//! shared request/result types here are shaped so AMQP can slot in later (Phase
//! 4) — some result types currently carry Redis-flavoured data and will grow
//! enum variants when a second broker arrives.

pub mod actor;
pub mod redis;

use std::collections::BTreeMap;
use std::pin::Pin;

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::Stream;
use time::OffsetDateTime;

/// Stable identifier for an open connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub u32);

/// What a connection can do — drives which views/actions the UI offers.
/// (A broker-kind tag returns in Phase 4 when a second broker exists.)
#[derive(Debug, Clone)]
pub struct Capabilities {
    /// Number of selectable databases (Redis); 1 when not applicable.
    pub databases: u32,
}

/// The Redis value type of a key (and the "missing" / "unknown" cases).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String,
    List,
    Set,
    Hash,
    ZSet,
    Stream,
    None,
    Unknown,
}

impl ValueType {
    /// Map a Redis `TYPE` reply to a [`ValueType`].
    pub fn from_redis(s: &str) -> Self {
        match s {
            "string" => ValueType::String,
            "list" => ValueType::List,
            "set" => ValueType::Set,
            "hash" => ValueType::Hash,
            "zset" => ValueType::ZSet,
            "stream" => ValueType::Stream,
            "none" => ValueType::None,
            _ => ValueType::Unknown,
        }
    }

    /// Short label for the type column.
    pub fn label(self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::List => "list",
            ValueType::Set => "set",
            ValueType::Hash => "hash",
            ValueType::ZSet => "zset",
            ValueType::Stream => "stream",
            ValueType::None => "none",
            ValueType::Unknown => "?",
        }
    }
}

/// Time-to-live of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// No expiry set.
    NoExpire,
    /// Expires in this many seconds.
    Seconds(i64),
    /// Could not be determined.
    Unknown,
}

impl Ttl {
    /// Build from a Redis `TTL` reply (seconds; `-1` no expire, `-2` missing).
    pub fn from_redis(ttl: i64) -> Self {
        match ttl {
            -1 => Ttl::NoExpire,
            -2 => Ttl::Unknown,
            secs if secs >= 0 => Ttl::Seconds(secs),
            _ => Ttl::Unknown,
        }
    }
}

/// A request to list a page of entries (Redis: keys in a database).
#[derive(Debug, Clone)]
pub struct BrowseReq {
    pub db: u32,
    /// Glob pattern for SCAN `MATCH` (default `*`).
    pub pattern: String,
    /// SCAN cursor; `0` starts a new scan.
    pub cursor: u64,
    /// SCAN `COUNT` hint.
    pub page_size: usize,
}

/// One listed entry with its type and TTL.
#[derive(Debug, Clone)]
pub struct EntryMeta {
    pub key: String,
    pub vtype: ValueType,
    pub ttl: Ttl,
}

/// A page of browse results.
#[derive(Debug, Clone)]
pub struct BrowsePage {
    pub db: u32,
    pub entries: Vec<EntryMeta>,
    /// Cursor for the next page; `0` means the scan is complete.
    pub next_cursor: u64,
}

/// A request to inspect a single key's value (with paging for collections).
#[derive(Debug, Clone)]
pub struct InspectReq {
    pub db: u32,
    pub key: String,
    /// Offset into a collection (lists/hashes/zsets).
    pub offset: usize,
    /// Maximum elements/bytes to return.
    pub limit: usize,
}

/// How a payload's bytes are represented for display/recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadEncoding {
    Utf8,
    Base64,
    Json,
}

impl PayloadEncoding {
    /// Lowercase tag written to the recording envelope (`encoding` field).
    pub fn tag(self) -> &'static str {
        match self {
            PayloadEncoding::Utf8 => "utf8",
            PayloadEncoding::Base64 => "base64",
            PayloadEncoding::Json => "json",
        }
    }
}

/// A realtime payload, kept binary-safe. The bytes are classified once on the
/// way in so the UI and recorder agree on encoding without re-deciding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Valid UTF-8 that is not JSON.
    Utf8(String),
    /// Valid UTF-8 that parses as JSON (stored as the original text).
    Json(String),
    /// Bytes that are not valid UTF-8.
    Binary(Vec<u8>),
}

impl Payload {
    /// Classify raw bytes the same way the value viewer does: UTF-8 that parses
    /// as JSON is `Json`, other UTF-8 is `Utf8`, everything else is `Binary`.
    pub fn classify(bytes: Vec<u8>) -> Self {
        match String::from_utf8(bytes) {
            Ok(text) => {
                if serde_json::from_str::<serde_json::Value>(&text).is_ok() {
                    Payload::Json(text)
                } else {
                    Payload::Utf8(text)
                }
            }
            // `from_utf8` hands the bytes back on failure, so nothing is copied.
            Err(e) => Payload::Binary(e.into_bytes()),
        }
    }

    /// The encoding tag for this payload.
    pub fn encoding(&self) -> PayloadEncoding {
        match self {
            Payload::Utf8(_) => PayloadEncoding::Utf8,
            Payload::Json(_) => PayloadEncoding::Json,
            Payload::Binary(_) => PayloadEncoding::Base64,
        }
    }

    /// The payload as a single display/record string (binary → base64).
    pub fn as_text(&self) -> String {
        match self {
            Payload::Utf8(s) | Payload::Json(s) => s.clone(),
            Payload::Binary(b) => base64::engine::general_purpose::STANDARD.encode(b),
        }
    }
}

/// What to subscribe/tail. Built from a `kind:target` spec string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubSpec {
    /// A pub/sub channel (`SUBSCRIBE`).
    Channel(String),
    /// A pub/sub channel pattern (`PSUBSCRIBE`).
    Pattern(String),
    /// A stream key, tailed from `$` (new entries only) within `db`.
    Stream { key: String, db: u32 },
}

impl SubSpec {
    /// Parse a `kind:target` spec — `pubsub:ch`, `psub:ch.*`, `stream:key` —
    /// using `default_db` for stream targets (which are database-scoped).
    pub fn parse(spec: &str, default_db: u32) -> anyhow::Result<Self> {
        let spec = spec.trim();
        let (kind, target) = spec
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("expected `kind:target`, e.g. `pubsub:news`"))?;
        let target = target.trim();
        if target.is_empty() {
            anyhow::bail!("missing target after `{kind}:`");
        }
        match kind.trim() {
            "pubsub" | "sub" | "channel" => Ok(SubSpec::Channel(target.to_string())),
            "psub" | "psubscribe" | "pattern" => Ok(SubSpec::Pattern(target.to_string())),
            "stream" | "xread" => Ok(SubSpec::Stream {
                key: target.to_string(),
                db: default_db,
            }),
            other => anyhow::bail!("unknown source kind `{other}` (use pubsub/psub/stream)"),
        }
    }

    /// Short source-type tag for the recording envelope.
    pub fn source_type(&self) -> &'static str {
        match self {
            SubSpec::Channel(_) => "pubsub",
            SubSpec::Pattern(_) => "psubscribe",
            SubSpec::Stream { .. } => "stream",
        }
    }

    /// The target name (channel, pattern, or stream key).
    pub fn target(&self) -> &str {
        match self {
            SubSpec::Channel(s) | SubSpec::Pattern(s) => s,
            SubSpec::Stream { key, .. } => key,
        }
    }

    /// A stable `kind:target` label for tabs, filenames, and round-tripping.
    pub fn label(&self) -> String {
        let kind = match self {
            SubSpec::Channel(_) => "pubsub",
            SubSpec::Pattern(_) => "psub",
            SubSpec::Stream { .. } => "stream",
        };
        format!("{kind}:{}", self.target())
    }
}

/// One realtime event from a subscription/tail. The recorder and the UI consume
/// the same events, so AMQP can reuse this stream unchanged later.
#[derive(Debug, Clone)]
pub struct BrokerEvent {
    /// When the event was observed (UTC).
    pub ts: OffsetDateTime,
    /// Where it came from: channel, or stream key.
    pub source: String,
    /// The message body.
    pub payload: Payload,
    /// Extra context: stream entry `id`, matched `pattern`, etc.
    pub meta: Vec<(String, String)>,
}

impl BrokerEvent {
    /// Look up a metadata value by key.
    pub fn meta(&self, key: &str) -> Option<&str> {
        self.meta
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// A `'static`, `Send` stream of [`BrokerEvent`]s — what [`BrokerConnection::subscribe`]
/// hands back. It owns its own dedicated socket so the actor's main connection
/// stays free for browse/inspect work.
pub type BrokerEventStream = Pin<Box<dyn Stream<Item = BrokerEvent> + Send>>;

/// A single stream entry (id + field/value pairs).
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

/// A rendered, size-capped view of a key's value.
#[derive(Debug, Clone)]
pub enum ValueView {
    Str {
        total_bytes: usize,
        shown_bytes: usize,
        text: String,
        encoding: PayloadEncoding,
    },
    List {
        len: usize,
        offset: usize,
        items: Vec<String>,
    },
    Set {
        len: usize,
        members: Vec<String>,
    },
    Hash {
        len: usize,
        fields: Vec<(String, String)>,
    },
    ZSet {
        len: usize,
        items: Vec<(String, f64)>,
    },
    Stream {
        len: usize,
        last_id: String,
        entries: Vec<StreamEntry>,
    },
    /// The key did not exist (or expired between listing and inspecting).
    Missing,
}

/// Server statistics, parsed from Redis `INFO` (raw plus extracted metrics).
#[derive(Debug, Clone, Default)]
pub struct ServerStats {
    /// All sections, preserving order, for the raw view.
    pub sections: Vec<(String, Vec<(String, String)>)>,
    /// Flattened key→value for convenient lookups.
    pub raw: BTreeMap<String, String>,
    pub redis_version: Option<String>,
    pub uptime_seconds: Option<u64>,
    pub connected_clients: Option<u64>,
    pub used_memory: Option<u64>,
    pub used_memory_peak: Option<u64>,
    pub maxmemory: Option<u64>,
    pub instantaneous_ops_per_sec: Option<u64>,
    pub keyspace_hits: Option<u64>,
    pub keyspace_misses: Option<u64>,
    /// `(db index, key count)` pairs from the keyspace section.
    pub db_keys: Vec<(u32, u64)>,
}

impl ServerStats {
    /// Cache hit ratio in `[0, 1]`, if hit/miss counters are present.
    pub fn hit_ratio(&self) -> Option<f64> {
        match (self.keyspace_hits, self.keyspace_misses) {
            (Some(h), Some(m)) if h + m > 0 => Some(h as f64 / (h + m) as f64),
            _ => None,
        }
    }
}

/// The interface every broker implements. The connection actor owns one of
/// these as a `Box<dyn BrokerConnection>` and calls it in response to commands.
#[async_trait]
pub trait BrokerConnection: Send {
    /// Establish the connection and report capabilities.
    async fn connect(&mut self) -> anyhow::Result<Capabilities>;

    /// Cheap liveness check.
    async fn ping(&mut self) -> anyhow::Result<()>;

    /// List a page of entries.
    async fn browse(&mut self, req: BrowseReq) -> anyhow::Result<BrowsePage>;

    /// Inspect a single entry's value.
    async fn inspect(&mut self, req: InspectReq) -> anyhow::Result<ValueView>;

    /// Fetch server statistics for the dashboard.
    async fn stats(&mut self) -> anyhow::Result<ServerStats>;

    /// Open a live tail for `spec` on a *dedicated* socket and return its event
    /// stream. The returned stream owns that socket, so it is `'static` and the
    /// actor's main connection is untouched. Takes `&mut self` (not `&self`) so
    /// the actor can hold it across the await without requiring `Sync`.
    async fn subscribe(&mut self, spec: SubSpec) -> anyhow::Result<BrokerEventStream>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_payloads() {
        assert_eq!(
            Payload::classify(b"hello".to_vec()),
            Payload::Utf8("hello".into())
        );
        assert_eq!(
            Payload::classify(br#"{"a":1}"#.to_vec()),
            Payload::Json(r#"{"a":1}"#.into())
        );
        match Payload::classify(vec![0x00, 0xff, 0xfe]) {
            Payload::Binary(b) => assert_eq!(b, vec![0x00, 0xff, 0xfe]),
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn payload_encoding_and_text() {
        assert_eq!(Payload::Utf8("x".into()).encoding(), PayloadEncoding::Utf8);
        assert_eq!(Payload::Json("{}".into()).encoding(), PayloadEncoding::Json);
        let bin = Payload::Binary(vec![0x00, 0x01, 0xff, 0xfe]);
        assert_eq!(bin.encoding(), PayloadEncoding::Base64);
        assert_eq!(bin.as_text(), "AAH//g==");
        assert_eq!(PayloadEncoding::Base64.tag(), "base64");
    }

    #[test]
    fn parses_sub_specs() {
        assert_eq!(
            SubSpec::parse("pubsub:news", 0).unwrap(),
            SubSpec::Channel("news".into())
        );
        assert_eq!(
            SubSpec::parse("psub:news.*", 0).unwrap(),
            SubSpec::Pattern("news.*".into())
        );
        assert_eq!(
            SubSpec::parse("stream:orders", 3).unwrap(),
            SubSpec::Stream {
                key: "orders".into(),
                db: 3
            }
        );
        // Whitespace tolerated.
        assert_eq!(
            SubSpec::parse("  pubsub : a ", 0).unwrap(),
            SubSpec::Channel("a".into())
        );
    }

    #[test]
    fn rejects_bad_specs() {
        assert!(SubSpec::parse("news", 0).is_err()); // no kind
        assert!(SubSpec::parse("pubsub:", 0).is_err()); // empty target
        assert!(SubSpec::parse("bogus:x", 0).is_err()); // unknown kind
    }

    #[test]
    fn sub_spec_label_and_source_type() {
        let s = SubSpec::Stream {
            key: "k".into(),
            db: 1,
        };
        assert_eq!(s.label(), "stream:k");
        assert_eq!(s.source_type(), "stream");
        assert_eq!(SubSpec::Pattern("p*".into()).label(), "psub:p*");
        assert_eq!(SubSpec::Channel("c".into()).source_type(), "pubsub");
    }
}
