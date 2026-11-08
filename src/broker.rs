//! Broker connection abstraction.
//!
//! [`BrokerConnection`] is the trait every broker implements; the per-connection
//! actor drives it generically so the rest of the app is broker-agnostic.
//!
//! Browse / inspect / dashboard / console are **optional, capability-gated**
//! operations. Their request and result types ([`BrowseReq`], [`ValueView`],
//! …) are mostly Redis-shaped; [`BrokerStats`] has a variant per broker that
//! reports stats (Redis, RabbitMQ). A broker that doesn't offer an operation
//! (e.g. AMQP) keeps the trait's default `bail!` implementation and reports
//! `false` in its [`Capabilities`], so the UI never surfaces it.
//! Realtime tailing ([`subscribe`](BrokerConnection::subscribe), yielding
//! [`BrokerEvent`]s) is the one capability every broker shares, so the live view
//! and the recorder work against any broker unchanged.

pub mod actor;
pub mod amqp;
pub mod factory;
mod http;
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

/// Make aws-lc-rs the process's rustls crypto provider, once. Both rustls
/// backends are built in (`ring` through ureq, `aws-lc-rs` through lapin and
/// fe2o3-amqp), so rustls can't pick one itself and panics building a TLS
/// config. `main` calls this first; every path that may open a TLS connection
/// calls it again, since tests don't run `main`.
pub(crate) fn install_tls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // An error means a provider is already installed, which serves too.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// A process-wide, monotonically increasing sequence for minting unique
/// connection identifiers (AMQP container-ids, RabbitMQ connection names), so
/// every broker connection is distinct even against a single broker.
pub(crate) fn next_conn_seq() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Build `amqp[s]://[user[:pass]@]host:port` with percent-encoded credentials
/// and an IPv6-bracketed host, for the AMQP 1.0 broker; `tls` selects the
/// `amqps://` scheme. RabbitMQ hands lapin its URI struct instead (see
/// [`rabbitmq::amqp_uri`]).
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
    /// Key/destination browser (Browser screen).
    pub can_browse: bool,
    /// Server statistics: the Server band atop the Browser screen and the
    /// Server Details tab, refreshed on the stats clock. RabbitMQ sets it only
    /// when the management API answered the connect-time probe.
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
    /// Redis: full browse + dashboard + console.
    pub fn redis() -> Self {
        Self {
            r#type: BrokerType::Redis,
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
            can_browse: true,
            can_dashboard: false,
            can_console: false,
            // AMQP 1.0 has a symmetric sender link, so the browser can publish a
            // message to a topic/queue — the one write it offers, behind a
            // deliberate keystroke (see [`crate::broker::amqp`]).
            can_publish: true,
        }
    }

    /// RabbitMQ (AMQP 0.9.1): tail + record (a non-destructive exchange tap, see
    /// [`crate::broker::rabbitmq`]). `can_browse` is true so it lands on the
    /// Browser screen, whose bottom panel hosts the Tail anchor. AMQP 0.9.1 can't
    /// list queues or exchanges, so the destination list comes from the
    /// management API ([`BrokerConnection::list_destinations`]). So do its stats:
    /// `connect` turns `can_dashboard` on when the API answered its probe.
    /// Publishing goes over AMQP, so it's on whether or not the API answered.
    pub fn rabbitmq() -> Self {
        Self {
            r#type: BrokerType::Rabbitmq,
            can_browse: true,
            can_dashboard: false,
            can_console: false,
            can_publish: true,
        }
    }

    /// Whether the browse list is driven by a Redis-style `SCAN` (cursor paging,
    /// per-database scoping). True only for Redis; the other brokers browse a
    /// list of destinations. Gates the scan-specific code paths (initial scan on
    /// connect, the scan behind an auto-refresh, key navigation).
    pub fn uses_key_scan(&self) -> bool {
        matches!(self.r#type, BrokerType::Redis)
    }

    /// Whether the Browser's list refreshes itself on the `browse_refresh_ms`
    /// clock: Redis re-scans its keys and RabbitMQ lists its queues and
    /// exchanges again. AMQP 1.0's curated list only changes when the user edits
    /// it.
    pub fn auto_refresh(&self) -> bool {
        matches!(self.r#type, BrokerType::Redis | BrokerType::Rabbitmq)
    }

    /// Whether opening a queue in the destination browser peeks its messages:
    /// AMQP 1.0 browses them over a `copy` link, RabbitMQ reads them through its
    /// management API (see [`BrokerConnection::peek`]).
    pub fn peeks_queues(&self) -> bool {
        matches!(self.r#type, BrokerType::Amqp | BrokerType::Rabbitmq)
    }

    /// Whether a publish to the queue in view peeks it again, so the new
    /// message shows up. AMQP 1.0 does. A RabbitMQ peek takes the messages off
    /// the queue (browse mode puts them back flagged redelivered), so it only
    /// runs when the user presses Enter.
    pub fn repeeks_after_publish(&self) -> bool {
        matches!(self.r#type, BrokerType::Amqp)
    }

    /// Whether the Browser body is a destination list (AMQP 1.0, RabbitMQ) rather
    /// than a Redis key table. Routes the keys pane's keys and the Tail anchor's
    /// spec parsing.
    pub fn has_destinations(&self) -> bool {
        !self.uses_key_scan() && self.can_browse
    }

    /// Whether the destination list is user-curated: AMQP 1.0 can't enumerate
    /// destinations, so the user adds (`a`) and removes (`x`) them, and `r` runs
    /// Jolokia discovery. False for RabbitMQ, whose list comes from the broker.
    pub fn curates_destinations(&self) -> bool {
        matches!(self.r#type, BrokerType::Amqp)
    }

    /// Whether the broker offers a live-tail bottom panel. Redis surfaces it
    /// alongside its console; AMQP and RabbitMQ have tails without a console, so
    /// they qualify on broker type rather than [`Self::can_console`].
    pub fn can_tail(&self) -> bool {
        self.can_console || matches!(self.r#type, BrokerType::Amqp | BrokerType::Rabbitmq)
    }

    /// Whether the broker lists its connections and consumers
    /// ([`BrokerConnection::roster`]): RabbitMQ, from the same management API
    /// as its stats, so only with the dashboard.
    pub fn lists_clients(&self) -> bool {
        self.can_dashboard && matches!(self.r#type, BrokerType::Rabbitmq)
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

/// A request to peek the current messages sitting in a queue (AMQP 1.0 or
/// RabbitMQ). The [`mode`](crate::config::PeekMode) decides whether the read
/// leaves the messages in the queue (browse), is skipped, or consumes them
/// (destructive).
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

/// A request to publish a single message to a destination (an AMQP 1.0 topic
/// or queue, a RabbitMQ queue or exchange). This is the only *write* the
/// browser performs; it is capability-gated ([`Capabilities::can_publish`])
/// and driven by a deliberate keystroke, so the browser's default observe-only
/// posture is unchanged for brokers that don't offer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReq {
    /// The destination to publish to. A RabbitMQ exchange's binding key is
    /// unused: the message goes out with `routing_key`.
    pub spec: SubSpec,
    /// The RabbitMQ routing key: the queue's name for a queue (published
    /// through the default exchange), the key typed for an exchange (empty for
    /// a fanout or headers exchange, which ignore it). AMQP 1.0 ignores it.
    pub routing_key: String,
    /// The raw message body bytes, sent verbatim.
    pub body: Vec<u8>,
}

/// Whether a destination is a topic, a queue or an exchange. AMQP 1.0's curated
/// list holds topics and queues; RabbitMQ lists queues and exchanges. Decides
/// the source spec built when peeking or tailing it (and the tag shown in the
/// list).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestKind {
    Topic,
    Queue,
    Exchange,
}

impl DestKind {
    /// The lower-case tag shown beside the name in the destination list.
    pub fn tag(self) -> &'static str {
        match self {
            DestKind::Topic => "topic",
            DestKind::Queue => "queue",
            DestKind::Exchange => "exchange",
        }
    }
}

/// A RabbitMQ queue's type, from the management API's `type` field. (4.x also
/// puts `x-queue-type` in every queue's `arguments`, so the arguments can't
/// tell.)
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(from = "String")]
pub enum QueueType {
    /// Also what a broker too old to report `type` has.
    #[default]
    Classic,
    Quorum,
    Stream,
    Other(String),
}

impl From<String> for QueueType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "classic" => QueueType::Classic,
            "quorum" => QueueType::Quorum,
            "stream" => QueueType::Stream,
            _ => QueueType::Other(s),
        }
    }
}

/// Why a RabbitMQ stream queue isn't peeked, and what to do instead. A peek
/// reads through `basic.get`, which streams don't support (the broker answers
/// `540 basic.get not supported by stream queues`).
pub const STREAM_PEEK_HINT: &str = "Stream queues can't be peeked: RabbitMQ doesn't \
    support basic.get on a stream. To watch its messages, tap an exchange that routes to it.";

impl QueueType {
    /// The destination list's Kind label. A classic queue is plain `queue`.
    pub fn label(&self) -> &str {
        match self {
            QueueType::Classic => "queue",
            QueueType::Quorum => "quorum",
            QueueType::Stream => "stream",
            QueueType::Other(s) => s,
        }
    }
}

/// A RabbitMQ exchange's type. It decides how a tap binds: the catch-all `#`
/// matches every routing key on a topic exchange and is ignored by a fanout
/// one, a direct exchange matches only the exact routing key, and a headers
/// exchange routes on message headers instead of the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
    Headers,
    /// A plugin type, e.g. `x-delayed-message` or `x-consistent-hash`.
    Other(String),
}

impl From<String> for ExchangeType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "direct" => ExchangeType::Direct,
            "fanout" => ExchangeType::Fanout,
            "topic" => ExchangeType::Topic,
            "headers" => ExchangeType::Headers,
            _ => ExchangeType::Other(s),
        }
    }
}

impl ExchangeType {
    /// The type's name, as RabbitMQ reports it.
    pub fn label(&self) -> &str {
        match self {
            ExchangeType::Direct => "direct",
            ExchangeType::Fanout => "fanout",
            ExchangeType::Topic => "topic",
            ExchangeType::Headers => "headers",
            ExchangeType::Other(s) => s,
        }
    }
}

/// The most rows of each kind (queues, exchanges) a destination listing
/// returns. A broker with more reports the listing as
/// [`truncated`](DestinationList::truncated).
pub const DESTINATION_LIST_CAP: usize = 5_000;

/// What a destination listing knows about one queue or exchange (RabbitMQ,
/// from the management API). A count or rate the broker didn't report is
/// `None`: a queue that never saw a publish has no publish rate, and with
/// management metrics disabled every count and rate is missing.
#[derive(Debug, Clone, PartialEq)]
pub struct DestinationInfo {
    pub name: String,
    /// [`DestKind::Queue`] or [`DestKind::Exchange`].
    pub kind: DestKind,
    /// A queue's messages, ready plus unacknowledged.
    pub messages: Option<u64>,
    pub ready: Option<u64>,
    pub unacked: Option<u64>,
    pub consumers: Option<u64>,
    /// A queue's type; `None` for an exchange.
    pub queue_type: Option<QueueType>,
    /// An exchange's type; `None` for a queue.
    pub exchange_type: Option<ExchangeType>,
    /// Messages published per second, into a queue or into an exchange.
    pub publish_rate: Option<f64>,
    /// The `delivery-limit` that a browse peek of this queue counts toward:
    /// set only for a RabbitMQ 3.x quorum queue with a limit (by argument or
    /// policy), where each requeue raises `x-delivery-count` and a message
    /// past the limit is dropped or dead-lettered — on its first requeue when
    /// the limit is below 1 (3.x accepts a negative argument). A 4.x requeue
    /// raises only `x-acquired-count`, so it never reaches the limit and this
    /// is `None`.
    pub peek_delivery_limit: Option<i64>,
    /// An internal exchange (e.g. `amq.rabbitmq.trace`). Clients can't publish
    /// to it, but a tap can still bind to it.
    pub internal: bool,
}

/// The result of [`BrokerConnection::list_destinations`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DestinationList {
    /// Queues first, then exchanges, each sorted by name.
    pub items: Vec<DestinationInfo>,
    /// Whether the broker has more than [`DESTINATION_LIST_CAP`] queues or
    /// exchanges, so the list stops short.
    pub truncated: bool,
    /// Whether the broker has its management metrics disabled, which is why
    /// every count and rate is missing.
    pub stats_disabled: bool,
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
    /// `Topic`/`Queue`/`Exchange` are built by [`SubSpec::parse`] from the
    /// destination add/seed/tail paths (and directly by the `dev` publisher,
    /// Jolokia discovery and the destination browser); the AMQP/RabbitMQ
    /// subscribe paths match on them to open the corresponding tail.
    Topic(String),
    /// A queue: an AMQP 1.0 queue address (peeked, or tailed as a
    /// non-destructive browse), or a queue in RabbitMQ's destination list.
    Queue(String),
    /// A RabbitMQ (AMQP 0.9.1) exchange tap: bind a temporary, exclusive,
    /// auto-delete queue to `exchange` with `binding_key` and consume the
    /// copies routed to it. Non-destructive — real queues and their consumers
    /// never lose a message. `binding_key` defaults to `#` (matches every
    /// routing key on a topic exchange; ignored by a fanout exchange).
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
    /// The canonical source-spec parser, driving the destination add/seed/tail
    /// paths (`app::input`, `app::connection`, `app::realtime::submit_subscribe`).
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
                // AMQP short strings cap at 255 bytes. Refuse a longer name or
                // key here, with the error the tap itself would give.
                rabbitmq::short_string("exchange name", exchange)?;
                rabbitmq::short_string("binding key", binding_key)?;
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

/// Server statistics in the shape of the broker that reported them: Redis's
/// from `INFO`, RabbitMQ's from its management API. The Server band and the
/// Server Details tab draw whichever variant arrives.
#[derive(Debug, Clone)]
pub enum BrokerStats {
    Redis(ServerStats),
    Rabbitmq(RabbitmqStats),
}

impl BrokerStats {
    /// The Redis statistics, if these are Redis's.
    pub fn redis(&self) -> Option<&ServerStats> {
        match self {
            BrokerStats::Redis(s) => Some(s),
            BrokerStats::Rabbitmq(_) => None,
        }
    }

    /// The RabbitMQ statistics, if these are RabbitMQ's.
    pub fn rabbitmq(&self) -> Option<&RabbitmqStats> {
        match self {
            BrokerStats::Rabbitmq(s) => Some(s),
            BrokerStats::Redis(_) => None,
        }
    }
}

/// RabbitMQ's overview (versions, rates and totals) plus its nodes' health,
/// from the management API. Any figure can be missing: the broker omits
/// rates, message totals and node figures when its metrics collector is off,
/// and `/api/nodes` needs the `monitoring` tag.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RabbitmqStats {
    pub rabbitmq_version: Option<String>,
    pub erlang_version: Option<String>,
    pub cluster_name: Option<String>,
    /// The broker's metrics collector is off (`disable_stats`), which is why
    /// the rates and message totals are missing.
    pub stats_disabled: bool,
    /// Whether the totals cover the whole cluster. Without the `monitoring`
    /// tag the overview only counts the vhosts the user can access, and only
    /// its own connections, so those aren't reported.
    pub cluster_wide: bool,
    /// Messages published per second. The broker's smoothed rates are floats
    /// and can dip below zero.
    pub publish_rate: Option<f64>,
    /// Messages delivered to consumers or fetched with `get`, per second.
    pub deliver_rate: Option<f64>,
    /// Messages acknowledged per second.
    pub ack_rate: Option<f64>,
    /// Messages in all queues: ready plus unacknowledged.
    pub messages: Option<u64>,
    pub messages_ready: Option<u64>,
    pub messages_unacked: Option<u64>,
    pub queues: Option<u64>,
    pub connections: Option<u64>,
    pub consumers: Option<u64>,
    /// One entry per cluster node. Empty without the `monitoring` tag.
    pub nodes: Vec<NodeHealth>,
}

impl RabbitmqStats {
    /// Whether any node raised an alarm or is down or partitioned.
    pub fn has_alarm(&self) -> bool {
        self.nodes.iter().any(|n| !n.alarms().is_empty())
    }
}

/// One RabbitMQ node's resource use and alarms. The figures are missing when
/// the metrics collector is off, and on a node that just started.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeHealth {
    pub name: String,
    pub running: Option<bool>,
    /// Seconds since the node started.
    pub uptime: Option<u64>,
    pub mem_used: Option<u64>,
    /// The memory high watermark: publishers block above it.
    pub mem_limit: Option<u64>,
    pub mem_alarm: Option<bool>,
    pub fd_used: Option<u64>,
    pub fd_total: Option<u64>,
    pub disk_free: Option<u64>,
    /// The free-disk low watermark: publishers block below it.
    pub disk_free_limit: Option<u64>,
    pub disk_free_alarm: Option<bool>,
    /// Nodes this one is partitioned from.
    pub partitions: Vec<String>,
}

impl NodeHealth {
    /// What's wrong with the node, if anything: down, partitioned, or a
    /// memory or disk alarm (which blocks publishers).
    pub fn alarms(&self) -> Vec<&'static str> {
        let mut alarms = Vec::new();
        if self.running == Some(false) {
            alarms.push("down");
        }
        if !self.partitions.is_empty() {
            alarms.push("partitioned");
        }
        if self.mem_alarm == Some(true) {
            alarms.push("memory alarm");
        }
        if self.disk_free_alarm == Some(true) {
            alarms.push("disk alarm");
        }
        alarms
    }
}

/// Which client roster to fetch: the vhost's connections or its consumers.
/// Each has its own bottom-panel tab (RabbitMQ, from its management API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RosterKind {
    Connections,
    Consumers,
}

impl RosterKind {
    /// The lowercase noun for messages: `connections`, `consumers`.
    pub fn noun(self) -> &'static str {
        match self {
            RosterKind::Connections => "connections",
            RosterKind::Consumers => "consumers",
        }
    }
}

/// A client roster's rows.
#[derive(Debug, Clone, PartialEq)]
pub enum RosterRows {
    Connections(Vec<ClientConnection>),
    Consumers(Vec<QueueConsumer>),
}

/// The vhost's connections or consumers, as the broker lists them.
#[derive(Debug, Clone, PartialEq)]
pub struct Roster {
    pub rows: RosterRows,
    /// Only the user's own connections, or the consumers on its own
    /// channels: without the `monitoring` tag the API lists nothing else.
    pub own_only: bool,
    /// The broker's metrics collector is off, so it lists none at all.
    pub stats_disabled: bool,
}

impl Roster {
    /// A roster of `kind` with no rows.
    pub fn empty(kind: RosterKind, own_only: bool, stats_disabled: bool) -> Self {
        let rows = match kind {
            RosterKind::Connections => RosterRows::Connections(Vec::new()),
            RosterKind::Consumers => RosterRows::Consumers(Vec::new()),
        };
        Self {
            rows,
            own_only,
            stats_disabled,
        }
    }

    pub fn len(&self) -> usize {
        match &self.rows {
            RosterRows::Connections(rows) => rows.len(),
            RosterRows::Consumers(rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One client connection in the Connections tab. Any figure can be missing:
/// the broker leaves out what it doesn't know.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClientConnection {
    /// The broker's name for it: the `peer -> broker` address pair.
    pub name: String,
    /// The client's own name for the connection, when it gave one.
    pub client_name: Option<String>,
    pub user: Option<String>,
    /// The client's `host:port`.
    pub peer: Option<String>,
    pub channels: Option<u64>,
    /// `running`, `blocked`, `flow`, `closing`, …
    pub state: Option<String>,
    pub connected_at: Option<OffsetDateTime>,
    /// Bytes per second received from the client. The broker's smoothed
    /// rates can dip below zero.
    pub recv_rate: Option<f64>,
    /// Bytes per second sent to the client.
    pub send_rate: Option<f64>,
}

impl ClientConnection {
    /// The name to show: the client's own, else the broker's address pair.
    pub fn display_name(&self) -> &str {
        self.client_name.as_deref().unwrap_or(&self.name)
    }
}

/// One consumer in the Consumers tab.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueConsumer {
    pub queue: String,
    pub tag: String,
    /// The channel it consumes on: the client's `host:port` and the channel
    /// number.
    pub channel: Option<String>,
    /// Whether the broker waits for acks (`false` for an auto-ack consumer).
    pub ack_required: Option<bool>,
    /// The channel's prefetch limit; `0` means unlimited.
    pub prefetch: Option<u64>,
    pub active: Option<bool>,
    /// Why it is or isn't receiving: `up`, `single_active`, `waiting`, …
    pub activity_status: Option<String>,
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
    /// [`Capabilities::uses_key_scan`], so a broker without a key scan skips it).
    async fn browse(&mut self, _req: BrowseReq) -> anyhow::Result<BrowsePage> {
        anyhow::bail!("this broker does not support browsing")
    }

    /// Inspect a single entry's value. Default: unsupported.
    async fn inspect(&mut self, _req: InspectReq) -> anyhow::Result<ValueView> {
        anyhow::bail!("this broker does not support inspecting values")
    }

    /// Fetch server statistics for the dashboard. Default: unsupported (gated
    /// off via [`Capabilities::can_dashboard`]).
    async fn stats(&mut self) -> anyhow::Result<BrokerStats> {
        anyhow::bail!("this broker does not support a stats dashboard")
    }

    /// Fetch the connections or consumers roster (RabbitMQ, from its
    /// management API). Asked for only while its tab is focused. Default:
    /// unsupported (gated off via [`Capabilities::lists_clients`]).
    async fn roster(&mut self, _kind: RosterKind) -> anyhow::Result<Roster> {
        anyhow::bail!("this broker does not list its clients")
    }

    /// Peek a bounded batch of the messages currently sitting in a queue (AMQP
    /// 1.0 or RabbitMQ), returning them as [`BrokerEvent`]s so they render like
    /// a tail. The read leaves the messages in the queue, is skipped, or
    /// consumes them per [`PeekReq::mode`]. Only issued when the user opens a
    /// queue in the destination browser ([`Capabilities::peeks_queues`]).
    /// Default: unsupported.
    async fn peek(&mut self, _req: PeekReq) -> anyhow::Result<Vec<BrokerEvent>> {
        anyhow::bail!("this broker does not support peeking messages")
    }

    /// List the broker's queues and exchanges for the destination browser
    /// (RabbitMQ, from its management API). `reprobe` first retries a
    /// management API the broker found unusable — the user's explicit refresh.
    /// The periodic refresh passes `false`, so a dead endpoint fails fast rather
    /// than stalling the connection's actor. Default: unsupported (AMQP 1.0
    /// discovers destinations over Jolokia instead, and Redis browses keys).
    async fn list_destinations(&mut self, _reprobe: bool) -> anyhow::Result<DestinationList> {
        anyhow::bail!("this broker does not list destinations")
    }

    /// Publish a single message to `req.spec`, returning once the broker took
    /// it; an `Err` says why it didn't (RabbitMQ: unroutable, NACKed or
    /// refused). The one *write* in the trait; default unsupported, so
    /// observe-only brokers keep this bail. Gated by
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
        let error = |spec: &str| SubSpec::parse(spec, 0).unwrap_err().to_string();
        let long = "x".repeat(256);
        assert_eq!(
            error(&format!("exchange:{long}")),
            "exchange name too long, max 255 bytes"
        );
        assert_eq!(
            error(&format!("exchange:ex/{long}")),
            "binding key too long, max 255 bytes"
        );
        // Bytes count, not characters: 128 two-byte chars are 256 bytes.
        assert_eq!(
            error(&format!("exchange:ex/{}", "é".repeat(128))),
            "binding key too long, max 255 bytes"
        );
        // Exactly the cap is allowed.
        let max = "x".repeat(255);
        assert!(SubSpec::parse(&format!("exchange:{max}"), 0).is_ok());
        assert!(SubSpec::parse(&format!("exchange:ex/{max}"), 0).is_ok());
    }

    #[test]
    fn the_tls_provider_is_aws_lc_rs_and_installing_it_again_is_harmless() {
        install_tls_provider();
        install_tls_provider();
        let installed = rustls::crypto::CryptoProvider::get_default().expect("installed");
        // Each backend's random source and key loader is its own type, which
        // `Debug` names: `AwsLcRs` here, `Ring` for ring.
        assert_eq!(format!("{:?}", installed.secure_random), "AwsLcRs");
        assert_eq!(format!("{:?}", installed.key_provider), "AwsLcRs");
        // What panicked while no provider was chosen.
        let _ = rustls::ClientConfig::builder();
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
        let r = Capabilities::redis();
        assert_eq!(r.r#type, BrokerType::Redis);
        assert!(r.can_browse && r.can_dashboard && r.can_console);
        // Redis browses via SCAN and tails alongside its console.
        assert!(r.uses_key_scan() && r.can_tail());

        let a = Capabilities::amqp();
        assert_eq!(a.r#type, BrokerType::Amqp);
        // AMQP has a (curated) destination browser and tails, but no dashboard
        // or console — and it never runs the Redis SCAN cadence.
        assert!(a.can_browse && a.can_tail());
        assert!(!a.can_dashboard && !a.can_console);
        assert!(!a.uses_key_scan());

        assert!(a.has_destinations() && a.curates_destinations());

        // RabbitMQ lands on the Browser for its tails: a destination pane (filled
        // by the management API later) but no key scan, dashboard or console.
        let rmq = Capabilities::rabbitmq();
        assert_eq!(rmq.r#type, BrokerType::Rabbitmq);
        assert!(rmq.can_browse && rmq.can_tail());
        assert!(!rmq.can_dashboard && !rmq.can_console);
        // It publishes over AMQP, so without the management API too.
        assert!(rmq.can_publish);
        assert!(!rmq.uses_key_scan());
        // Its list comes from the broker, so it isn't user-curated.
        assert!(rmq.has_destinations() && !rmq.curates_destinations());

        // Redis browses keys, not destinations.
        assert!(!r.has_destinations() && !r.curates_destinations());

        // The broker-supplied lists refresh themselves; AMQP's curated one
        // doesn't. Both destination browsers peek queues; Redis has none.
        assert!(r.auto_refresh() && rmq.auto_refresh() && !a.auto_refresh());
        assert!(a.peeks_queues() && rmq.peeks_queues() && !r.peeks_queues());
        // A RabbitMQ peek takes messages off the queue, so a publish doesn't
        // trigger one; AMQP 1.0 peeks the queue it published to again.
        assert!(a.repeeks_after_publish() && !rmq.repeeks_after_publish());

        // Only RabbitMQ lists its clients, through the management API that
        // also gives it the dashboard.
        assert!(!rmq.lists_clients() && !a.lists_clients() && !r.lists_clients());
        let probed = Capabilities {
            can_dashboard: true,
            ..Capabilities::rabbitmq()
        };
        assert!(probed.lists_clients());
    }

    #[test]
    fn a_roster_knows_its_rows() {
        let empty = Roster::empty(RosterKind::Consumers, true, false);
        assert!(matches!(&empty.rows, RosterRows::Consumers(rows) if rows.is_empty()));
        assert!(empty.is_empty() && empty.own_only && !empty.stats_disabled);
        let empty = Roster::empty(RosterKind::Connections, false, true);
        assert!(matches!(&empty.rows, RosterRows::Connections(rows) if rows.is_empty()));
        assert!(empty.stats_disabled);
        let roster = Roster {
            rows: RosterRows::Connections(vec![ClientConnection::default(); 2]),
            own_only: false,
            stats_disabled: false,
        };
        assert_eq!(roster.len(), 2);
        assert!(!roster.is_empty());
        assert_eq!(RosterKind::Connections.noun(), "connections");
        assert_eq!(RosterKind::Consumers.noun(), "consumers");

        let mut conn = ClientConnection {
            name: "172.17.0.1:56648 -> 172.17.0.2:5672".into(),
            ..Default::default()
        };
        assert_eq!(conn.display_name(), "172.17.0.1:56648 -> 172.17.0.2:5672");
        conn.client_name = Some("orders-api".into());
        assert_eq!(conn.display_name(), "orders-api");
    }

    #[test]
    fn destination_types_read_rabbitmqs_names() {
        for (wire, kind, label) in [
            ("classic", QueueType::Classic, "queue"),
            ("quorum", QueueType::Quorum, "quorum"),
            ("stream", QueueType::Stream, "stream"),
        ] {
            let parsed = QueueType::from(wire.to_string());
            assert_eq!(parsed, kind);
            assert_eq!(parsed.label(), label);
        }
        let delayed = QueueType::from("delayed".to_string());
        assert_eq!(delayed.label(), "delayed");

        for name in ["direct", "fanout", "topic", "headers"] {
            let parsed = ExchangeType::from(name.to_string());
            assert!(!matches!(parsed, ExchangeType::Other(_)), "{name}");
            assert_eq!(parsed.label(), name);
        }
        let plugin = ExchangeType::from("x-delayed-message".to_string());
        assert_eq!(plugin, ExchangeType::Other("x-delayed-message".into()));
        assert_eq!(plugin.label(), "x-delayed-message");
        assert_eq!(DestKind::Exchange.tag(), "exchange");
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
    fn node_alarms_name_what_is_wrong() {
        let healthy = NodeHealth {
            name: "rabbit@a".into(),
            running: Some(true),
            mem_alarm: Some(false),
            disk_free_alarm: Some(false),
            ..Default::default()
        };
        assert!(healthy.alarms().is_empty());
        // Metrics off: nothing is known, so nothing is flagged.
        assert!(NodeHealth::default().alarms().is_empty());
        let troubled = NodeHealth {
            running: Some(false),
            partitions: vec!["rabbit@b".into()],
            mem_alarm: Some(true),
            disk_free_alarm: Some(true),
            ..healthy.clone()
        };
        assert_eq!(
            troubled.alarms(),
            ["down", "partitioned", "memory alarm", "disk alarm"]
        );

        let mut stats = RabbitmqStats {
            nodes: vec![healthy.clone()],
            ..Default::default()
        };
        assert!(!stats.has_alarm());
        stats.nodes.push(NodeHealth {
            disk_free_alarm: Some(true),
            ..healthy
        });
        assert!(stats.has_alarm(), "any node's alarm counts");
        assert!(!RabbitmqStats::default().has_alarm());
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
