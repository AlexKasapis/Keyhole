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

use async_trait::async_trait;

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
}
