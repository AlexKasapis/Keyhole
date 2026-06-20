//! Broker connection abstraction.
//!
//! [`BrokerConnection`] is the trait every broker implements; the per-connection
//! actor drives it generically so the rest of the app is broker-agnostic.
//!
//! Browse / inspect / dashboard / console are **optional, capability-gated**
//! operations. Their request and result types ([`BrowseReq`], [`ValueView`],
//! [`ServerStats`], …) are Redis-shaped; a broker that doesn't offer them (e.g.
//! AMQP) keeps the trait's default `bail!` implementations and reports `false`
//! in its [`Capabilities`], so the UI never surfaces those screens for it.
//! Realtime tailing ([`subscribe`](BrokerConnection::subscribe), yielding
//! [`BrokerEvent`]s) is the one capability every broker shares, so the live view
//! and the recorder work against any broker unchanged.

pub mod actor;
#[cfg(feature = "amqp")]
pub mod amqp;
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

/// Which broker a connection talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerKind {
    Redis,
    Amqp,
}

impl BrokerKind {
    /// Lowercase tag for display and the recording envelope.
    pub fn label(self) -> &'static str {
        match self {
            BrokerKind::Redis => "redis",
            BrokerKind::Amqp => "amqp",
        }
    }
}

/// What a connection can do — drives which views/actions the UI offers, so a
/// broker that lacks a key browser or dashboard simply doesn't surface them.
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub kind: BrokerKind,
    /// Number of selectable databases (Redis); 1 when not applicable.
    pub databases: u32,
    /// Key/destination browser (Browser screen).
    pub can_browse: bool,
    /// Server statistics (Dashboard screen).
    pub can_dashboard: bool,
    /// Read-only command console (Console screen).
    pub can_console: bool,
}

impl Capabilities {
    /// Redis: full browse + dashboard + console over `databases` databases.
    pub fn redis(databases: u32) -> Self {
        Self {
            kind: BrokerKind::Redis,
            databases,
            can_browse: true,
            can_dashboard: true,
            can_console: true,
        }
    }

    /// AMQP (v1): realtime tail + record only — no key browser, dashboard, or
    /// command console (the broker model and the read-only mandate don't fit
    /// them yet). Only constructed by the AMQP impl / tests, so it is dead code
    /// in a build without the `amqp` feature.
    #[cfg_attr(not(feature = "amqp"), allow(dead_code))]
    pub fn amqp() -> Self {
        Self {
            kind: BrokerKind::Amqp,
            databases: 1,
            can_browse: false,
            can_dashboard: false,
            can_console: false,
        }
    }
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
    /// Keyspace-notification events for a database (`PSUBSCRIBE __keyevent@db__:*`).
    /// Requires the server's `notify-keyspace-events` to be enabled.
    Keyspace { db: u32 },
    /// Every command the server processes (`MONITOR`). Server-wide (all dbs).
    Monitor,
    /// An AMQP 1.0 topic — a non-destructive live subscription (each subscriber
    /// gets its own copy, so observing never steals messages). The primary AMQP tail.
    Topic(String),
    /// An AMQP 1.0 queue address.
    Queue(String),
}

impl SubSpec {
    /// Parse a source spec. Pub/sub-style specs are `kind:target` —
    /// `pubsub:ch`, `psub:ch.*`, `stream:key`; `default_db` supplies the database
    /// for `stream`/`keyspace` targets. `monitor` and `keyspace` may be given
    /// bare (the latter defaults to `default_db`) or as `keyspace:N`.
    pub fn parse(spec: &str, default_db: u32) -> anyhow::Result<Self> {
        let spec = spec.trim();
        // Targetless / database-defaulted forms.
        if spec.eq_ignore_ascii_case("monitor") {
            return Ok(SubSpec::Monitor);
        }
        if spec.eq_ignore_ascii_case("keyspace") {
            return Ok(SubSpec::Keyspace { db: default_db });
        }
        let (kind, target) = spec
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("expected `kind:target`, e.g. `pubsub:news`"))?;
        let target = target.trim();
        if target.is_empty() {
            anyhow::bail!("missing target after `{kind}:`");
        }
        match kind.trim().to_ascii_lowercase().as_str() {
            "pubsub" | "sub" | "channel" => Ok(SubSpec::Channel(target.to_string())),
            "psub" | "psubscribe" | "pattern" => Ok(SubSpec::Pattern(target.to_string())),
            "stream" | "xread" => Ok(SubSpec::Stream {
                key: target.to_string(),
                db: default_db,
            }),
            "keyspace" => {
                let db = target.parse::<u32>().map_err(|_| {
                    anyhow::anyhow!("keyspace db must be a number, e.g. `keyspace:0`")
                })?;
                Ok(SubSpec::Keyspace { db })
            }
            "topic" => Ok(SubSpec::Topic(target.to_string())),
            "queue" => Ok(SubSpec::Queue(target.to_string())),
            other => anyhow::bail!(
                "unknown source kind `{other}` \
                 (redis: pubsub/psub/stream/keyspace/monitor · amqp: topic/queue)"
            ),
        }
    }

    /// Short source-type tag for the recording envelope.
    pub fn source_type(&self) -> &'static str {
        match self {
            SubSpec::Channel(_) => "pubsub",
            SubSpec::Pattern(_) => "psubscribe",
            SubSpec::Stream { .. } => "stream",
            SubSpec::Keyspace { .. } => "keyspace",
            SubSpec::Monitor => "monitor",
            SubSpec::Topic(_) => "amqp-topic",
            SubSpec::Queue(_) => "amqp-queue",
        }
    }

    /// The target name (channel, pattern, stream key, `dbN`, `all`, or AMQP address).
    pub fn target(&self) -> String {
        match self {
            SubSpec::Channel(s) | SubSpec::Pattern(s) | SubSpec::Topic(s) | SubSpec::Queue(s) => {
                s.clone()
            }
            SubSpec::Stream { key, .. } => key.clone(),
            SubSpec::Keyspace { db } => format!("db{db}"),
            SubSpec::Monitor => "all".to_string(),
        }
    }

    /// A stable label for tabs, filenames, and round-tripping.
    pub fn label(&self) -> String {
        match self {
            SubSpec::Monitor => "monitor".to_string(),
            SubSpec::Keyspace { db } => format!("keyspace:db{db}"),
            SubSpec::Channel(_) => format!("pubsub:{}", self.target()),
            SubSpec::Pattern(_) => format!("psub:{}", self.target()),
            SubSpec::Stream { .. } => format!("stream:{}", self.target()),
            SubSpec::Topic(_) => format!("topic:{}", self.target()),
            SubSpec::Queue(_) => format!("queue:{}", self.target()),
        }
    }
}

/// One realtime event from a subscription/tail. The recorder and the UI consume
/// the same events, so AMQP can reuse this stream unchanged later.
#[derive(Debug, Clone)]
pub struct BrokerEvent {
    /// When the event was observed (UTC).
    pub ts: OffsetDateTime,
    /// Where it came from: a Redis channel / stream key, or an AMQP destination.
    pub source: String,
    /// The message body.
    pub payload: Payload,
    /// Broker-defined extras: Redis stream entry `id`, matched `pattern`,
    /// MONITOR `db`/`client`, etc.
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

    /// List a page of entries. Default: unsupported (gated off via
    /// [`Capabilities::can_browse`], so a broker without a browser skips it).
    async fn browse(&mut self, _req: BrowseReq) -> anyhow::Result<BrowsePage> {
        anyhow::bail!("this broker does not support browsing")
    }

    /// Inspect a single entry's value. Default: unsupported.
    async fn inspect(&mut self, _req: InspectReq) -> anyhow::Result<ValueView> {
        anyhow::bail!("this broker does not support inspecting values")
    }

    /// Fetch server statistics for the dashboard. Default: unsupported.
    async fn stats(&mut self) -> anyhow::Result<ServerStats> {
        anyhow::bail!("this broker does not support a stats dashboard")
    }

    /// Open a live tail for `spec` on a *dedicated* socket and return its event
    /// stream. The returned stream owns that socket, so it is `'static` and the
    /// actor's main connection is untouched. Takes `&mut self` (not `&self`) so
    /// the actor can hold it across the await without requiring `Sync`.
    async fn subscribe(&mut self, spec: SubSpec) -> anyhow::Result<BrokerEventStream>;

    /// An optional, non-fatal advisory shown when a tail for `spec` is opened —
    /// e.g. that keyspace notifications are disabled, so the tail will stay
    /// silent. Returns `None` when there is nothing to flag. Default: no notice.
    async fn tail_notice(&mut self, _spec: &SubSpec) -> Option<String> {
        None
    }

    /// Execute a single, already-validated **read-only** command and render its
    /// reply for display. Implementations must still defensively reject writes
    /// (see the read-only command console). Default: unsupported.
    async fn exec_readonly(&mut self, _command: &str) -> anyhow::Result<String> {
        anyhow::bail!("this broker does not support a command console")
    }
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
    fn parses_monitor_and_keyspace_specs() {
        // MONITOR is targetless and case-insensitive.
        assert_eq!(SubSpec::parse("monitor", 0).unwrap(), SubSpec::Monitor);
        assert_eq!(SubSpec::parse("MONITOR", 7).unwrap(), SubSpec::Monitor);
        // Bare `keyspace` defaults to the active db; `keyspace:N` is explicit.
        assert_eq!(
            SubSpec::parse("keyspace", 3).unwrap(),
            SubSpec::Keyspace { db: 3 }
        );
        assert_eq!(
            SubSpec::parse("keyspace:5", 0).unwrap(),
            SubSpec::Keyspace { db: 5 }
        );
        // A non-numeric keyspace db is rejected.
        assert!(SubSpec::parse("keyspace:abc", 0).is_err());
    }

    #[test]
    fn parses_amqp_specs() {
        assert_eq!(
            SubSpec::parse("topic:events", 0).unwrap(),
            SubSpec::Topic("events".into())
        );
        assert_eq!(
            SubSpec::parse("queue:orders", 0).unwrap(),
            SubSpec::Queue("orders".into())
        );
        // Kind is case-insensitive; address keeps its case.
        assert_eq!(
            SubSpec::parse("TOPIC:MyTopic", 0).unwrap(),
            SubSpec::Topic("MyTopic".into())
        );
    }

    #[test]
    fn rejects_bad_specs() {
        assert!(SubSpec::parse("news", 0).is_err()); // no kind
        assert!(SubSpec::parse("pubsub:", 0).is_err()); // empty target
        assert!(SubSpec::parse("bogus:x", 0).is_err()); // unknown kind
    }

    #[test]
    fn capabilities_constructors() {
        let r = Capabilities::redis(16);
        assert_eq!(r.kind, BrokerKind::Redis);
        assert_eq!(r.databases, 16);
        assert!(r.can_browse && r.can_dashboard && r.can_console);

        let a = Capabilities::amqp();
        assert_eq!(a.kind, BrokerKind::Amqp);
        assert_eq!(a.databases, 1);
        assert!(!a.can_browse && !a.can_dashboard && !a.can_console);
        assert_eq!(BrokerKind::Amqp.label(), "amqp");
        assert_eq!(BrokerKind::Redis.label(), "redis");
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
        assert_eq!(SubSpec::Monitor.label(), "monitor");
        assert_eq!(SubSpec::Monitor.source_type(), "monitor");
        assert_eq!(SubSpec::Keyspace { db: 2 }.label(), "keyspace:db2");
        assert_eq!(SubSpec::Keyspace { db: 2 }.source_type(), "keyspace");
        assert_eq!(SubSpec::Topic("e".into()).label(), "topic:e");
        assert_eq!(SubSpec::Topic("e".into()).source_type(), "amqp-topic");
        assert_eq!(SubSpec::Queue("q".into()).label(), "queue:q");
        assert_eq!(SubSpec::Queue("q".into()).source_type(), "amqp-queue");
    }

    #[test]
    fn sub_spec_target_accessor() {
        assert_eq!(SubSpec::Channel("c".into()).target(), "c");
        assert_eq!(SubSpec::Pattern("p.*".into()).target(), "p.*");
        assert_eq!(
            SubSpec::Stream {
                key: "k".into(),
                db: 0
            }
            .target(),
            "k"
        );
        assert_eq!(SubSpec::Keyspace { db: 4 }.target(), "db4");
        assert_eq!(SubSpec::Monitor.target(), "all");
        assert_eq!(SubSpec::Topic("e".into()).target(), "e");
        assert_eq!(SubSpec::Queue("q".into()).target(), "q");
    }

    #[test]
    fn value_type_from_redis_and_label() {
        for (reply, vtype, label) in [
            ("string", ValueType::String, "string"),
            ("list", ValueType::List, "list"),
            ("set", ValueType::Set, "set"),
            ("hash", ValueType::Hash, "hash"),
            ("zset", ValueType::ZSet, "zset"),
            ("stream", ValueType::Stream, "stream"),
            ("none", ValueType::None, "none"),
        ] {
            assert_eq!(ValueType::from_redis(reply), vtype);
            assert_eq!(vtype.label(), label);
        }
        assert_eq!(ValueType::from_redis("mystery"), ValueType::Unknown);
        assert_eq!(ValueType::Unknown.label(), "?");
    }

    #[test]
    fn ttl_from_redis_classifies() {
        assert_eq!(Ttl::from_redis(-1), Ttl::NoExpire);
        assert_eq!(Ttl::from_redis(-2), Ttl::Unknown);
        assert_eq!(Ttl::from_redis(0), Ttl::Seconds(0));
        assert_eq!(Ttl::from_redis(42), Ttl::Seconds(42));
        assert_eq!(
            Ttl::from_redis(-99),
            Ttl::Unknown,
            "other negatives are unknown"
        );
    }

    #[test]
    fn payload_encoding_tags() {
        assert_eq!(PayloadEncoding::Utf8.tag(), "utf8");
        assert_eq!(PayloadEncoding::Json.tag(), "json");
        assert_eq!(PayloadEncoding::Base64.tag(), "base64");
    }

    #[test]
    fn broker_event_meta_lookup() {
        let ev = BrokerEvent {
            ts: OffsetDateTime::UNIX_EPOCH,
            source: "s".into(),
            payload: Payload::Utf8("x".into()),
            meta: vec![
                ("id".into(), "1-0".into()),
                ("pattern".into(), "p.*".into()),
            ],
        };
        assert_eq!(ev.meta("id"), Some("1-0"));
        assert_eq!(ev.meta("pattern"), Some("p.*"));
        assert_eq!(ev.meta("missing"), None);
    }

    #[test]
    fn server_stats_hit_ratio_edge_cases() {
        let mut s = ServerStats::default();
        assert_eq!(s.hit_ratio(), None, "no counters yields no ratio");
        s.keyspace_hits = Some(0);
        s.keyspace_misses = Some(0);
        assert_eq!(s.hit_ratio(), None, "zero traffic yields no ratio");
        s.keyspace_hits = Some(3);
        s.keyspace_misses = Some(1);
        assert_eq!(s.hit_ratio(), Some(0.75));
    }
}
