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
pub mod amqp;
pub mod factory;
pub mod jolokia;
pub mod rabbitmq;
pub mod redis;

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::Stream;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use time::OffsetDateTime;

/// The AMQP short-string length cap (one length byte), shared by the AMQP 1.0
/// and RabbitMQ brokers. Names longer than this make the AMQP client panic on
/// conversion, so source specs are validated against it up front. Referenced
/// only by the test-exercised spec parser now that headless `record` is gone.
#[allow(dead_code)]
pub(crate) const AMQP_SHORTSTR_MAX: usize = 255;

/// A process-wide, monotonically increasing sequence for minting unique
/// connection identifiers (AMQP container-ids, RabbitMQ connection names), so
/// every broker connection is distinct even against a single broker.
pub(crate) fn next_conn_seq() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Build `amqp[s]://[user[:pass]@]host:port` with percent-encoded credentials
/// and an IPv6-bracketed host. Shared by the AMQP 1.0 and RabbitMQ brokers
/// (RabbitMQ appends a `/vhost` segment); `tls` selects the `amqps://` scheme.
pub(crate) fn amqp_base_url(
    tls: bool,
    host: &str,
    port: u16,
    user: Option<&str>,
    pass: Option<&str>,
) -> String {
    let enc = |s: &str| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string();
    let scheme = if tls { "amqps" } else { "amqp" };
    let mut url = format!("{scheme}://");
    if user.is_some() || pass.is_some() {
        if let Some(u) = user {
            url.push_str(&enc(u));
        }
        if let Some(p) = pass {
            url.push(':');
            url.push_str(&enc(p));
        }
        url.push('@');
    }
    // Bracket an IPv6 literal (which contains `:`) so the `host:port` boundary
    // parses unambiguously; leave an already-bracketed or non-IPv6 host as-is.
    if host.contains(':') && !host.starts_with('[') {
        url.push('[');
        url.push_str(host);
        url.push(']');
    } else {
        url.push_str(host);
    }
    url.push(':');
    url.push_str(&port.to_string());
    url
}

/// Stable identifier for an open connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnId(pub u32);

/// Which broker a connection talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerType {
    Redis,
    /// AMQP 1.0 (ActiveMQ / Amazon MQ / RabbitMQ 4.x).
    Amqp,
    /// RabbitMQ over AMQP 0.9.1 (all RabbitMQ versions).
    Rabbitmq,
}

impl BrokerType {
    /// A compact one-line hint of the source specs this broker accepts, shown in
    /// the subscribe prompt so the user knows what to type. The brokers tail
    /// different kinds of destination, so the hint is broker-specific.
    pub fn sub_spec_hint(self) -> &'static str {
        match self {
            BrokerType::Redis => "pubsub:ch · psub:ch.* · stream:key · keyspace · monitor",
            BrokerType::Amqp => "topic:name · queue:name",
            BrokerType::Rabbitmq => "exchange:name · exchange:name/binding-key",
        }
    }
}

/// What a connection can do — drives which views/actions the UI offers, so a
/// broker that lacks a key browser or dashboard simply doesn't surface them.
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub r#type: BrokerType,
    /// Number of databases the server reports (Redis `CONFIG GET databases`); 1
    /// when not applicable. Discovered on connect and surfaced for reference; no
    /// in-app database switcher consumes it (the `[`/`]` switcher was removed).
    #[allow(dead_code)]
    pub databases: u32,
    /// Key/destination browser (Browser screen).
    pub can_browse: bool,
    /// Server statistics (shown as a stats band atop the Browser screen).
    pub can_dashboard: bool,
    /// Read-only command console (Console screen).
    pub can_console: bool,
    /// Whether the broker can publish a message to a destination. This is the
    /// one *write* keyhole performs; most brokers keep it `false` (the
    /// observe-only default), so the UI offers the publish action only where it
    /// is `true`.
    pub can_publish: bool,
}

impl Capabilities {
    /// Redis: full browse + dashboard + console over `databases` databases.
    pub fn redis(databases: u32) -> Self {
        Self {
            r#type: BrokerType::Redis,
            databases,
            can_browse: true,
            can_dashboard: true,
            can_console: true,
            can_publish: false,
        }
    }

    /// AMQP (v1): a destination *browser* (a user-curated list of topics/queues,
    /// since AMQP 1.0 cannot enumerate them), with a queue message peek and live
    /// tails — but no stats dashboard or command console (those need a management
    /// plane AMQP 1.0 lacks; deferred to the RabbitMQ phase). `can_browse` is true
    /// so the broker lands on the Browser screen, but [`Self::uses_key_scan`] is
    /// false: the browse list is curated, not scanned, so the Redis SCAN cadence
    /// (auto-refresh) never runs for it.
    pub fn amqp() -> Self {
        Self {
            r#type: BrokerType::Amqp,
            databases: 1,
            can_browse: true,
            can_dashboard: false,
            can_console: false,
            // AMQP 1.0 has a symmetric sender link, so the browser can publish a
            // message to a topic/queue — the one write it offers, behind a
            // deliberate keystroke (see [`crate::broker::amqp`]).
            can_publish: true,
        }
    }

    /// RabbitMQ (AMQP 0.9.1): tail + record only for now (a non-destructive
    /// exchange tap, see [`crate::broker::rabbitmq`]). The browser experience is
    /// deferred to the RabbitMQ phase, so it stays on the Connections list.
    pub fn rabbitmq() -> Self {
        Self {
            r#type: BrokerType::Rabbitmq,
            databases: 1,
            can_browse: false,
            can_dashboard: false,
            can_console: false,
            can_publish: false,
        }
    }

    /// Whether the browse list is driven by a Redis-style `SCAN` (cursor paging,
    /// periodic auto-refresh, per-database scoping). True only for Redis; AMQP's
    /// browse list is a curated set of destinations, so the SCAN cadence must not
    /// run for it. Gates the scan-specific code paths (initial scan on connect,
    /// auto-refresh, key navigation).
    pub fn uses_key_scan(&self) -> bool {
        matches!(self.r#type, BrokerType::Redis)
    }

    /// Whether the broker offers a live-tail bottom panel. Redis surfaces it
    /// alongside its console; AMQP has tails without a console, so it qualifies
    /// on broker type rather than [`Self::can_console`].
    pub fn can_tail(&self) -> bool {
        self.can_console || matches!(self.r#type, BrokerType::Amqp)
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
    /// Generation of the scan this request belongs to. Echoed back on the
    /// resulting [`BrowsePage`] so the UI can discard pages from a scan that has
    /// since been superseded (a DB switch, a new filter, a fresh refresh).
    pub epoch: u64,
}

/// One listed entry with its type, TTL, and (when available) memory footprint.
#[derive(Debug, Clone)]
pub struct EntryMeta {
    pub key: String,
    pub vtype: ValueType,
    pub ttl: Ttl,
    /// Approximate memory used by the key in bytes (`MEMORY USAGE`); `None` when
    /// the server did not report it (missing key, command unavailable, etc.).
    pub size: Option<u64>,
}

/// A page of browse results.
#[derive(Debug, Clone)]
pub struct BrowsePage {
    pub db: u32,
    pub entries: Vec<EntryMeta>,
    /// Cursor for the next page; `0` means the scan is complete.
    pub next_cursor: u64,
    /// Generation copied from the originating [`BrowseReq`] (stamped by the
    /// connection actor). The UI compares it against the scan it is currently
    /// driving and ignores pages that no longer match.
    pub epoch: u64,
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

/// A request to peek the current messages sitting in a destination (AMQP). The
/// [`mode`](crate::config::PeekMode) decides whether the read is non-destructive
/// (browse), skipped, or consumes the messages (destructive).
#[derive(Debug, Clone)]
pub struct PeekReq {
    /// The destination to peek. Only a queue is peekable; a topic does not retain
    /// messages, so the broker rejects a topic peek.
    pub spec: SubSpec,
    /// How to read: browse (copy, non-destructive), skip (no read), or destructive.
    pub mode: crate::config::PeekMode,
    /// Maximum number of messages to read before returning.
    pub limit: usize,
}

/// A request to publish a single message to a destination (AMQP topic/queue).
/// This is the only *write* the browser performs; it is capability-gated
/// ([`Capabilities::can_publish`]) and driven by a deliberate keystroke, so the
/// browser's default observe-only posture is unchanged for brokers that don't
/// offer it.
#[derive(Debug, Clone)]
pub struct PublishReq {
    /// The destination to publish to (an AMQP topic or queue).
    pub spec: SubSpec,
    /// The raw message body bytes, sent verbatim as an AMQP data section.
    pub body: Vec<u8>,
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
    ///
    /// `Topic`/`Queue`/`Exchange` are constructed only in tests now that the
    /// headless `record` source-spec parser is gone; the AMQP/RabbitMQ subscribe
    /// paths still match on them, so they are retained for the pending TUI
    /// realtime rework that will re-expose AMQP/RabbitMQ tailing.
    #[allow(dead_code)]
    Topic(String),
    /// An AMQP 1.0 queue address.
    #[allow(dead_code)]
    Queue(String),
    /// A RabbitMQ (AMQP 0.9.1) exchange tap: bind a temporary, exclusive,
    /// auto-delete queue to `exchange` with `binding_key` and consume the
    /// copies routed to it. Non-destructive — real queues and their consumers
    /// never lose a message. `binding_key` defaults to `#` (matches every
    /// routing key on a topic exchange; ignored by a fanout exchange).
    #[allow(dead_code)]
    Exchange {
        exchange: String,
        binding_key: String,
    },
}

impl SubSpec {
    /// Parse a source spec. Pub/sub-style specs are `kind:target` —
    /// `pubsub:ch`, `psub:ch.*`, `stream:key`; `default_db` supplies the database
    /// for `stream`/`keyspace` targets. `monitor` and `keyspace` may be given
    /// bare (the latter defaults to `default_db`) or as `keyspace:N`.
    ///
    /// Exercised only by tests now that the headless `record` command (its sole
    /// caller) is gone; retained as the canonical spec parser for the pending
    /// TUI realtime rework.
    #[allow(dead_code)]
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
            "exchange" => {
                // Optional `/binding-key` suffix. Exchange names don't contain
                // `/`, so splitting on the first `/` cleanly separates the two
                // (a routing key may itself contain `/`, which stays in the key).
                // An absent or empty key defaults to `#` — every routing key on a
                // topic exchange, ignored by a fanout exchange.
                let (exchange, binding_key) = match target.split_once('/') {
                    Some((ex, key)) => (ex.trim(), key.trim()),
                    None => (target, ""),
                };
                if exchange.is_empty() {
                    anyhow::bail!("missing exchange name before `/`");
                }
                let binding_key = if binding_key.is_empty() {
                    "#"
                } else {
                    binding_key
                };
                // AMQP short-strings cap at 255 bytes; reject longer names/keys
                // here so the AMQP client never panics converting them (its
                // `ShortString::from` calls `.expect()` on the length).
                if exchange.len() > AMQP_SHORTSTR_MAX {
                    anyhow::bail!("exchange name exceeds {AMQP_SHORTSTR_MAX} bytes");
                }
                if binding_key.len() > AMQP_SHORTSTR_MAX {
                    anyhow::bail!("binding key exceeds {AMQP_SHORTSTR_MAX} bytes");
                }
                Ok(SubSpec::Exchange {
                    exchange: exchange.to_string(),
                    binding_key: binding_key.to_string(),
                })
            }
            other => anyhow::bail!(
                "unknown source kind `{other}` (redis: pubsub/psub/stream/keyspace/monitor \
                 · amqp: topic/queue · rabbitmq: exchange)"
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
            SubSpec::Exchange { .. } => "rabbitmq-exchange",
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
            SubSpec::Exchange { exchange, .. } => exchange.clone(),
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
            // The default `#` binding key is implicit in the short form.
            SubSpec::Exchange {
                exchange,
                binding_key,
            } => {
                if binding_key == "#" {
                    format!("exchange:{exchange}")
                } else {
                    format!("exchange:{exchange}/{binding_key}")
                }
            }
        }
    }

    /// The broker type this source spec targets. Each spec belongs to exactly
    /// one broker, so a spec typed for the wrong broker can be rejected up front
    /// (with a clear message) instead of failing later at subscribe time.
    ///
    /// Exercised only by tests now that the headless `record` command (its sole
    /// caller) is gone; retained for the pending TUI realtime rework.
    #[allow(dead_code)]
    pub fn supported_type(&self) -> BrokerType {
        match self {
            SubSpec::Channel(_)
            | SubSpec::Pattern(_)
            | SubSpec::Stream { .. }
            | SubSpec::Keyspace { .. }
            | SubSpec::Monitor => BrokerType::Redis,
            SubSpec::Topic(_) | SubSpec::Queue(_) => BrokerType::Amqp,
            SubSpec::Exchange { .. } => BrokerType::Rabbitmq,
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

/// One connected client, parsed from a line of the Redis `CLIENT LIST` reply.
/// Surfaced in the Browser's Server Details tab; fields keyhole doesn't display
/// are dropped during parsing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientInfo {
    /// Connection id (`id=`).
    pub id: u64,
    /// Client-set name (`name=`), empty unless the client ran `CLIENT SETNAME`.
    pub name: String,
    /// Peer address `host:port` (`addr=`).
    pub addr: String,
    /// Seconds since the connection opened (`age=`).
    pub age: u64,
    /// Seconds the connection has been idle (`idle=`).
    pub idle: u64,
    /// The last command the client ran (`cmd=`), e.g. `get` or `client|list`.
    pub last_cmd: String,
}

/// Server statistics, parsed from Redis `INFO` (raw plus extracted metrics),
/// plus the connected-client roster from `CLIENT LIST`.
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
    /// Network read rate in KB/s (`instantaneous_input_kbps`). Feeds the Server
    /// Details network graph alongside the write rate below.
    pub instantaneous_input_kbps: Option<f64>,
    /// Network write rate in KB/s (`instantaneous_output_kbps`).
    pub instantaneous_output_kbps: Option<f64>,
    pub keyspace_hits: Option<u64>,
    pub keyspace_misses: Option<u64>,
    /// `(db index, key count)` pairs from the keyspace section.
    pub db_keys: Vec<(u32, u64)>,
    /// Connected clients from `CLIENT LIST`. Empty when the command was not run
    /// or the server declined it (ACL / managed Redis).
    pub clients: Vec<ClientInfo>,
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

    /// Peek a bounded batch of the messages currently sitting in a destination
    /// (AMQP queue), returning them as [`BrokerEvent`]s so they render like a
    /// tail. The read is non-destructive, skipped, or destructive per
    /// [`PeekReq::mode`]. Default: unsupported (gated off via
    /// [`Capabilities::can_browse`] for brokers without a destination browser).
    async fn peek(&mut self, _req: PeekReq) -> anyhow::Result<Vec<BrokerEvent>> {
        anyhow::bail!("this broker does not support peeking messages")
    }

    /// Publish a single message to `req.spec`. The one *write* in the trait;
    /// default unsupported, so observe-only brokers keep this bail. Gated by
    /// [`Capabilities::can_publish`].
    async fn publish(&mut self, _req: PublishReq) -> anyhow::Result<()> {
        anyhow::bail!("this broker does not support publishing")
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
    fn parses_rabbitmq_exchange_specs() {
        // Bare exchange → default `#` binding key.
        assert_eq!(
            SubSpec::parse("exchange:events", 0).unwrap(),
            SubSpec::Exchange {
                exchange: "events".into(),
                binding_key: "#".into()
            }
        );
        // Explicit binding key after `/` (may contain dots, as topic keys do).
        assert_eq!(
            SubSpec::parse("exchange:amq.topic/orders.*", 0).unwrap(),
            SubSpec::Exchange {
                exchange: "amq.topic".into(),
                binding_key: "orders.*".into()
            }
        );
        // Only the first `/` splits, so a routing key may itself contain `/`.
        assert_eq!(
            SubSpec::parse("exchange:ex/a/b", 0).unwrap(),
            SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "a/b".into()
            }
        );
        // A trailing slash / empty key falls back to the `#` default.
        assert_eq!(
            SubSpec::parse("exchange:ex/", 0).unwrap(),
            SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "#".into()
            }
        );
        // Whitespace tolerated; kind is case-insensitive; names keep their case.
        assert_eq!(
            SubSpec::parse("  EXCHANGE : my.ex / key ", 0).unwrap(),
            SubSpec::Exchange {
                exchange: "my.ex".into(),
                binding_key: "key".into()
            }
        );
        // An empty exchange name (key given but no name) is rejected.
        assert!(SubSpec::parse("exchange:/key", 0).is_err());
    }

    #[test]
    fn rejects_overlong_exchange_name_or_binding_key() {
        // Over the 255-byte AMQP short-string cap → rejected up front (so the
        // AMQP client never panics converting it).
        let long = "x".repeat(AMQP_SHORTSTR_MAX + 1);
        assert!(SubSpec::parse(&format!("exchange:{long}"), 0).is_err());
        assert!(SubSpec::parse(&format!("exchange:ex/{long}"), 0).is_err());
        // Exactly the cap is allowed.
        let max = "x".repeat(AMQP_SHORTSTR_MAX);
        assert!(SubSpec::parse(&format!("exchange:{max}"), 0).is_ok());
    }

    #[test]
    fn sub_spec_supported_kind_maps_each_spec_to_its_broker() {
        assert_eq!(
            SubSpec::Channel("c".into()).supported_type(),
            BrokerType::Redis
        );
        assert_eq!(SubSpec::Monitor.supported_type(), BrokerType::Redis);
        assert_eq!(
            SubSpec::Keyspace { db: 0 }.supported_type(),
            BrokerType::Redis
        );
        assert_eq!(
            SubSpec::Topic("t".into()).supported_type(),
            BrokerType::Amqp
        );
        assert_eq!(
            SubSpec::Queue("q".into()).supported_type(),
            BrokerType::Amqp
        );
        assert_eq!(
            SubSpec::Exchange {
                exchange: "e".into(),
                binding_key: "#".into()
            }
            .supported_type(),
            BrokerType::Rabbitmq
        );
    }

    #[test]
    fn amqp_base_url_encodes_creds_and_brackets_ipv6() {
        // Percent-encodes userinfo (shared by both AMQP brokers).
        assert_eq!(
            amqp_base_url(false, "h.example.com", 5672, Some("u"), Some("p@ss/word")),
            "amqp://u:p%40ss%2Fword@h.example.com:5672"
        );
        // TLS selects the amqps scheme.
        assert!(amqp_base_url(true, "h", 5671, None, None).starts_with("amqps://"));
        // No credentials → no userinfo.
        assert_eq!(amqp_base_url(false, "h", 5672, None, None), "amqp://h:5672");
        // An IPv6 literal host is bracketed so host:port parses unambiguously.
        assert_eq!(
            amqp_base_url(false, "::1", 5672, None, None),
            "amqp://[::1]:5672"
        );
        assert_eq!(
            amqp_base_url(false, "fe80::1", 5672, Some("u"), None),
            "amqp://u@[fe80::1]:5672"
        );
        // An already-bracketed host is left as-is.
        assert_eq!(
            amqp_base_url(false, "[::1]", 5672, None, None),
            "amqp://[::1]:5672"
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
        assert_eq!(r.r#type, BrokerType::Redis);
        assert_eq!(r.databases, 16);
        assert!(r.can_browse && r.can_dashboard && r.can_console);
        // Redis browses via SCAN and tails alongside its console.
        assert!(r.uses_key_scan() && r.can_tail());

        let a = Capabilities::amqp();
        assert_eq!(a.r#type, BrokerType::Amqp);
        assert_eq!(a.databases, 1);
        // AMQP has a (curated) destination browser and tails, but no dashboard
        // or console — and it never runs the Redis SCAN cadence.
        assert!(a.can_browse && a.can_tail());
        assert!(!a.can_dashboard && !a.can_console);
        assert!(!a.uses_key_scan());

        // RabbitMQ is tail + record only for now — its browser is deferred to the
        // RabbitMQ phase, so it stays off the Browser screen.
        let rmq = Capabilities::rabbitmq();
        assert_eq!(rmq.r#type, BrokerType::Rabbitmq);
        assert_eq!(rmq.databases, 1);
        assert!(!rmq.can_browse && !rmq.can_dashboard && !rmq.can_console);
        assert!(!rmq.uses_key_scan());
    }

    #[test]
    fn sub_spec_hint_is_broker_specific() {
        assert!(BrokerType::Redis.sub_spec_hint().contains("pubsub:"));
        assert!(BrokerType::Amqp.sub_spec_hint().contains("topic:"));
        let rmq = BrokerType::Rabbitmq.sub_spec_hint();
        assert!(rmq.contains("exchange:"));
        assert!(rmq.contains("binding-key"));
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
        // The default `#` binding key is implicit in the label; a custom key shows.
        let ex_default = SubSpec::Exchange {
            exchange: "ex".into(),
            binding_key: "#".into(),
        };
        assert_eq!(ex_default.label(), "exchange:ex");
        assert_eq!(ex_default.source_type(), "rabbitmq-exchange");
        let ex_keyed = SubSpec::Exchange {
            exchange: "amq.topic".into(),
            binding_key: "orders.*".into(),
        };
        assert_eq!(ex_keyed.label(), "exchange:amq.topic/orders.*");
        // The label round-trips back to the same spec through the parser.
        assert_eq!(SubSpec::parse(&ex_keyed.label(), 0).unwrap(), ex_keyed);
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
        // An exchange tap's target is the exchange name (the binding key is meta).
        assert_eq!(
            SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "k".into()
            }
            .target(),
            "ex"
        );
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
