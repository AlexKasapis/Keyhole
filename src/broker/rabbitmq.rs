//! RabbitMQ (AMQP 0.9.1) implementation of [`BrokerConnection`], built on
//! `lapin`. Works against every RabbitMQ version (3.x and 4.x).
//!
//! Read + record only, exactly like the AMQP 1.0 broker (see
//! [`Capabilities::rabbitmq`]). The one capability is a
//! **non-destructive exchange tap**:
//!  1. declare a temporary, server-named, `exclusive` + `auto_delete` queue;
//!  2. bind it to the target exchange with the requested binding key;
//!  3. consume the copies routed to it (auto-ack).
//!
//! Because the spy queue is brand-new and bound *in addition* to whatever real
//! queues exist, the broker routes independent copies to it — the real queues
//! and their consumers never lose a message. Auto-acking only discards our own
//! copy, and the spy queue auto-deletes when the tail's connection drops. This
//! is a stronger non-destructive guarantee than AMQP 1.0's queue browse: we
//! never read from an existing queue at all.
//!
//! Each tail owns a dedicated connection + channel + consumer (mirroring the
//! Redis dedicated-socket model) so the returned stream is `'static` and the
//! actor's main connection stays free for liveness checks.
//!
//! Listing queues and exchanges, peeking and stats need the HTTP management API
//! ([`mgmt`]), which `connect` probes after the AMQP login. A failed probe leaves
//! the connection usable for tails.
//!
//! A **queue peek** ([`RabbitmqConnection::peek`]) is the one read that touches
//! a real queue: the management API's `/get` takes the messages off the queue
//! and, in browse mode, puts them straight back (`ack_requeue_true`) flagged
//! `redelivered`. It only runs when the user opens a queue.

// The browser lists queues and exchanges and peeks queues through the client;
// stats and the connection/consumer rosters use the rest of it in later
// phases. `expect` fails the build once everything is used, so this can't
// outlive its reason.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "stats and the connection/consumer rosters don't call the management client yet"
    )
)]
mod mgmt;

use std::collections::HashMap;

use anyhow::Context as _;
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use lapin::message::Delivery;
use lapin::options::{
    BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, Consumer, ExchangeKind};

use self::mgmt::{
    AckMode, ApiError, ExchangeInfo, GetMessage, Management, MessageProperties, MgmtClient,
    QueueInfo,
};
use super::{
    BrokerConnection, BrokerEvent, BrokerEventStream, Capabilities, DestKind, DestinationInfo,
    DestinationList, ExchangeType, Payload, PeekReq, QueueType, SubSpec, STREAM_PEEK_HINT,
};
use crate::config::{PeekMode, RabbitmqProfile};

/// The username and password lapin logs in with when the AMQP URL has none (the
/// AMQP URI spec's default). The management login defaults the same way, so both
/// channels authenticate as the same user.
const DEFAULT_CREDENTIAL: &str = "guest";

/// A live (or not-yet-connected) RabbitMQ (AMQP 0.9.1) connection.
pub struct RabbitmqConnection {
    profile: RabbitmqProfile,
    password: Option<String>,
    /// The main connection, kept open for liveness checks.
    conn: Option<Connection>,
    /// The HTTP management API and what its last probe found (tags, version,
    /// or the `mgmt_error` that disables it).
    mgmt: Management,
    /// Each exchange's type from the last listing, so a tap's advisory can
    /// tell whether the default `#` binding key fits the exchange.
    exchange_types: HashMap<String, ExchangeType>,
    /// The most body bytes a peek fetches per message (the
    /// `value_preview_bytes` setting); longer bodies come back truncated.
    preview_bytes: usize,
}

impl RabbitmqConnection {
    /// Build a connection from a profile, its resolved AMQP password, the
    /// resolved management API password (`management_password`, else the AMQP
    /// password), and the most body bytes a peek shows per message. Call
    /// [`BrokerConnection::connect`] to actually establish it.
    pub fn new(
        profile: RabbitmqProfile,
        password: Option<String>,
        management_password: Option<String>,
        preview_bytes: usize,
    ) -> Self {
        let mgmt = Management::new(management_client(&profile, management_password));
        Self {
            profile,
            password,
            conn: None,
            mgmt,
            exchange_types: HashMap::new(),
            preview_bytes: preview_bytes.max(1),
        }
    }

    /// Why the management API can't be used, naming where it was looked for.
    fn unavailable(&self) -> String {
        format!("management API unavailable at {}", self.mgmt.base_url())
    }

    /// Build an `amqp[s]://[user[:pass]@]host:port/vhost` URL with
    /// percent-encoded credentials and vhost. `tls` selects `amqps://` (the
    /// :5671 TLS listener).
    fn url(&self) -> String {
        let mut url = super::amqp_base_url(
            self.profile.tls,
            &self.profile.host,
            self.profile.port,
            self.profile.username.as_deref(),
            self.password.as_deref(),
        );
        // The vhost is a single percent-encoded path segment. The default "/"
        // must encode to "%2F" — a bare trailing "/" is read by AMQP as the
        // *empty* vhost, not the default one.
        url.push('/');
        url.push_str(&utf8_percent_encode(&self.profile.vhost, NON_ALPHANUMERIC).to_string());
        url
    }

    /// Connection properties carrying a recognisable name for the broker's
    /// management UI. lapin's default features bind it to the ambient tokio
    /// runtime, so no executor/reactor wiring is needed here.
    fn conn_props(&self) -> ConnectionProperties {
        let name = format!("keyhole-{}-{}", self.profile.name, super::next_conn_seq());
        ConnectionProperties::default().with_connection_name(name.into())
    }
}

#[async_trait]
impl BrokerConnection for RabbitmqConnection {
    async fn connect(&mut self) -> anyhow::Result<Capabilities> {
        let conn = Connection::connect(&self.url(), self.conn_props())
            .await
            // `.context` keeps the lapin error as the source (so its reply
            // code/text survives, shown via the `{:#}` chain) rather than
            // flattening it into the message here.
            .context("connecting to RabbitMQ")?;
        self.conn = Some(conn);
        // Find a dead or misconfigured management API once, up front; a failure
        // is recorded and the connection stays usable for tails.
        self.mgmt.run_probe().await;
        Ok(Capabilities::rabbitmq())
    }

    async fn ping(&mut self) -> anyhow::Result<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("connection is not established"))?;
        // Opening a channel round-trips to the broker (channel.open / open-ok),
        // so a dead peer surfaces here rather than lingering silently. The
        // channel closes when it drops at the end of this scope.
        conn.create_channel()
            .await
            .context("liveness check failed")?;
        Ok(())
    }

    async fn subscribe(&mut self, spec: SubSpec) -> anyhow::Result<BrokerEventStream> {
        let (exchange, binding_key) = match &spec {
            SubSpec::Exchange {
                exchange,
                binding_key,
            } => (exchange.clone(), binding_key.clone()),
            other => anyhow::bail!("{} is not a RabbitMQ destination", other.label()),
        };
        // Each tail is its own dedicated connection (and thus its own spy queue).
        open_exchange_tap(&self.url(), self.conn_props(), exchange, binding_key).await
    }

    async fn list_destinations(&mut self, reprobe: bool) -> anyhow::Result<DestinationList> {
        if reprobe && self.mgmt.error().is_some() {
            self.mgmt.run_probe().await;
        }
        let unavailable = self.unavailable();
        // A failed probe answers here without touching the network.
        let client = self.mgmt.client().context(unavailable.clone())?.clone();
        let probe = self.mgmt.probe();
        let stats_disabled = probe.is_some_and(|p| p.stats_disabled);
        // Only 3.x counts a requeue toward a quorum queue's delivery limit. An
        // unknown version is assumed to count, so the warning errs on the side
        // of showing.
        let requeue_counts = probe
            .and_then(mgmt::Probe::major_version)
            .is_none_or(|major| major < 4);
        let fetched = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            Ok((client.queues()?, client.exchanges()?))
        })
        .await
        .context("the listing task failed")?;
        let (queues, exchanges) = match fetched {
            Ok(lists) => lists,
            Err(e) => {
                // A refusal (an HTTP error reply) is cheap to repeat; a call that
                // got no reply would cost the timeouts again on every refresh.
                if e.downcast_ref::<ApiError>().is_none() {
                    self.mgmt.mark_unreachable(&e);
                }
                return Err(e.context(unavailable));
            }
        };
        let list = destination_list(queues, exchanges, stats_disabled, requeue_counts);
        self.exchange_types = list
            .items
            .iter()
            .filter_map(|d| Some((d.name.clone(), d.exchange_type.clone()?)))
            .collect();
        Ok(list)
    }

    /// Peek a queue through the management API's `/get`: browse mode puts the
    /// messages back (`ack_requeue_true`, which RabbitMQ flags `redelivered`),
    /// destructive mode removes them (`ack_requeue_false`). Bodies are capped at
    /// `preview_bytes`. A stream queue refuses `/get`; that comes back as the
    /// [`STREAM_PEEK_HINT`].
    async fn peek(&mut self, req: PeekReq) -> anyhow::Result<Vec<BrokerEvent>> {
        let queue = match &req.spec {
            SubSpec::Queue(q) => q.clone(),
            other => anyhow::bail!(
                "{} can't be peeked: only a queue holds messages",
                other.label()
            ),
        };
        let Some(ackmode) = ack_mode(req.mode) else {
            return Ok(Vec::new());
        };
        let unavailable = self.unavailable();
        // A failed probe answers here without touching the network.
        let client = self.mgmt.client().context(unavailable.clone())?.clone();
        let (limit, truncate) = (req.limit, self.preview_bytes);
        let name = queue.clone();
        let fetched =
            tokio::task::spawn_blocking(move || client.get(&name, limit, ackmode, Some(truncate)))
                .await
                .context("the peek task failed")?;
        match fetched {
            Ok(messages) => Ok(messages
                .into_iter()
                .map(|m| peek_event(&queue, m))
                .collect()),
            Err(e) => Err(match e.downcast_ref::<ApiError>() {
                // The listing didn't say it's a stream, but the broker did.
                Some(api) if is_stream_refusal(api) => anyhow::anyhow!(STREAM_PEEK_HINT),
                Some(_) => e,
                // No reply at all: later calls fail fast until a re-probe.
                None => {
                    self.mgmt.mark_unreachable(&e);
                    e.context(unavailable)
                }
            }),
        }
    }

    async fn tail_notice(&mut self, spec: &SubSpec) -> Option<String> {
        // The default `#` binding key matches every routing key on a *topic*
        // exchange and is ignored by a *fanout* exchange, but a *direct* exchange
        // matches it only against the literal key `#` — so the tap could attach
        // yet stay silent. A *headers* exchange ignores the key, and a binding
        // without header arguments matches every message, so the tap gets
        // everything (checked on 3.13) like a fanout. The last listing knows each
        // exchange's type; without it (no management API, or an exchange it
        // hasn't listed) flag the ambiguity whenever the catch-all default is
        // in use (mirroring the Redis keyspace notice).
        let SubSpec::Exchange {
            exchange,
            binding_key,
        } = spec
        else {
            return None;
        };
        if binding_key != "#" {
            return None;
        }
        match self.exchange_types.get(exchange) {
            Some(ExchangeType::Topic | ExchangeType::Fanout | ExchangeType::Headers) => None,
            Some(ExchangeType::Direct) => Some(format!(
                "`{exchange}` is a direct exchange: binding key `#` only matches messages \
                 routed with the key `#` — tap exchange:{exchange}/<routing-key> instead"
            )),
            Some(ExchangeType::Other(_)) | None => Some(
                "binding key `#` matches every key on a topic exchange and is ignored by \
                 fanout and headers exchanges, but on a direct exchange it matches only the \
                 key `#` — give an explicit key (exchange:name/key) if this tail stays \
                 silent"
                    .to_string(),
            ),
        }
    }
}

/// The `/get` settlement for a peek `mode`; `None` for [`PeekMode::Skip`],
/// which reads nothing.
fn ack_mode(mode: PeekMode) -> Option<AckMode> {
    match mode {
        // Never worse than `reject_requeue_true`, and on 4.x it doesn't raise
        // a quorum queue's delivery count (checked on 3.13 and 4.4).
        PeekMode::Browse => Some(AckMode::AckRequeueTrue),
        PeekMode::Destructive => Some(AckMode::AckRequeueFalse),
        PeekMode::Skip => None,
    }
}

/// Whether a `/get` refusal is a stream queue's: a 400 whose reason is `540
/// basic.get not supported by stream queues …` (3.13 and 4.4 alike).
fn is_stream_refusal(error: &ApiError) -> bool {
    error.status == 400
        && error
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("basic.get not supported"))
}

/// A message `/get` fetched from `queue`, as an event: the queue is the source,
/// and how the message arrived (exchange, routing key, redelivery) plus its
/// properties and headers ride along as metadata for the inspector's preview
/// and filter.
fn peek_event(queue: &str, msg: GetMessage) -> BrokerEvent {
    let mut body = msg.body().unwrap_or_else(|e| {
        // RabbitMQ sends valid base64. Should a body still not decode, show it
        // as sent rather than fail the whole batch: a destructive peek has
        // already taken it off the queue.
        tracing::debug!(error = %e, queue, "undecodable peeked body");
        msg.payload.clone().into_bytes()
    });
    // `truncate` cuts the body before encoding; `payload_bytes` is its full size.
    let full = msg.payload_bytes.unwrap_or(body.len() as u64);
    let truncated = (body.len() as u64) < full;
    if truncated {
        drop_cut_character(&mut body);
    }
    let exchange = if msg.exchange.is_empty() {
        "(default)".to_string()
    } else {
        msg.exchange
    };
    let mut meta = vec![
        ("exchange".to_string(), exchange),
        ("routing_key".to_string(), msg.routing_key),
        ("redelivered".to_string(), msg.redelivered.to_string()),
    ];
    if truncated {
        meta.push((
            "truncated".to_string(),
            format!("showing {} of {full} bytes", body.len()),
        ));
    }
    meta.extend(property_meta(msg.properties));
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: queue.to_string(),
        payload: Payload::classify(body),
        meta,
    }
}

/// Drop a multi-byte character that truncation cut in half, so a truncated
/// text body still reads as text rather than binary.
fn drop_cut_character(body: &mut Vec<u8>) {
    if let Err(e) = std::str::from_utf8(body) {
        // `error_len() == None`: the bytes only stop mid-character.
        if e.error_len().is_none() {
            body.truncate(e.valid_up_to());
        }
    }
}

/// The properties a message carries, as metadata (only those it set), then
/// each header as `headers.<name>`.
fn property_meta(p: MessageProperties) -> Vec<(String, String)> {
    let timestamp = p.timestamp.map(|secs| {
        i64::try_from(secs)
            .ok()
            .and_then(|s| OffsetDateTime::from_unix_timestamp(s).ok())
            .and_then(|t| t.format(&Rfc3339).ok())
            .unwrap_or_else(|| secs.to_string())
    });
    let delivery_mode = p.delivery_mode.map(|m| match m {
        1 => "1 (transient)".to_string(),
        2 => "2 (persistent)".to_string(),
        other => other.to_string(),
    });
    let fields = [
        ("content_type", p.content_type),
        ("message_id", p.message_id),
        ("correlation_id", p.correlation_id),
        ("delivery_mode", delivery_mode),
        ("content_encoding", p.content_encoding),
        ("priority", p.priority.map(|n| n.to_string())),
        ("timestamp", timestamp),
        ("type", p.message_type),
        ("app_id", p.app_id),
        ("user_id", p.user_id),
        ("reply_to", p.reply_to),
        ("expiration", p.expiration),
    ];
    let mut meta: Vec<(String, String)> = fields
        .into_iter()
        .filter_map(|(key, value)| Some((key.to_string(), value?)))
        .collect();
    meta.extend(
        p.headers
            .into_iter()
            .map(|(name, value)| (format!("headers.{name}"), header_text(value))),
    );
    meta
}

/// A header value as text: a string as itself, anything else (a number, a
/// bool, an array, a nested table such as `x-death`) as compact JSON.
fn header_text(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// The destination list from the management API's queue and exchange
/// listings: queues first, then exchanges, each in the API's name order. The
/// default exchange (`""`) is left out: it can't be bound, so it can't be
/// tapped. `requeue_counts` says whether the broker counts a requeue toward a
/// quorum queue's delivery limit (3.x).
fn destination_list(
    queues: mgmt::Listing<QueueInfo>,
    exchanges: mgmt::Listing<ExchangeInfo>,
    stats_disabled: bool,
    requeue_counts: bool,
) -> DestinationList {
    let truncated = queues.truncated || exchanges.truncated;
    let items = queues
        .items
        .into_iter()
        .map(|q| queue_info(q, requeue_counts))
        .chain(
            exchanges
                .items
                .into_iter()
                .filter(|e| !e.name.is_empty())
                .map(exchange_info),
        )
        .collect();
    DestinationList {
        items,
        truncated,
        stats_disabled,
    }
}

fn queue_info(q: QueueInfo, requeue_counts: bool) -> DestinationInfo {
    // Only 3.x lacks the `delivery_limit` field; a broker that reports it is
    // 4.x, where a requeue doesn't count either way.
    let counts = requeue_counts && q.queue_type == QueueType::Quorum && q.delivery_limit.is_none();
    let peek_delivery_limit = counts.then(|| configured_delivery_limit(&q)).flatten();
    DestinationInfo {
        peek_delivery_limit,
        name: q.name,
        kind: DestKind::Queue,
        messages: q.messages,
        ready: q.messages_ready,
        unacked: q.messages_unacknowledged,
        consumers: q.consumers,
        queue_type: Some(q.queue_type),
        exchange_type: None,
        publish_rate: q.message_stats.publish_rate,
        internal: false,
    }
}

/// A 3.x quorum queue's delivery limit: its `x-delivery-limit` argument or a
/// `delivery-limit` policy, the lower one when both are set (as the broker
/// applies it). 3.x has no default limit. A negative limit means unlimited.
fn configured_delivery_limit(q: &QueueInfo) -> Option<i64> {
    let argument = q.arguments.get("x-delivery-limit").and_then(|v| v.as_i64());
    let policy = q
        .effective_policy_definition
        .get("delivery-limit")
        .and_then(|v| v.as_i64());
    let limit = match (argument, policy) {
        (Some(a), Some(p)) => Some(a.min(p)),
        (a, p) => a.or(p),
    };
    limit.filter(|&n| n >= 0)
}

fn exchange_info(e: ExchangeInfo) -> DestinationInfo {
    DestinationInfo {
        name: e.name,
        kind: DestKind::Exchange,
        messages: None,
        ready: None,
        unacked: None,
        consumers: None,
        queue_type: None,
        exchange_type: Some(ExchangeType::from(e.exchange_type)),
        // Published into the exchange by clients (`publish_out` is what it
        // routed on to queues).
        publish_rate: e.message_stats.publish_in_rate,
        peek_delivery_limit: None,
        internal: e.internal,
    }
}

/// The management client for `profile`: its base URL and vhost, logging in as
/// `management_username` (else the AMQP `username`) with `password`. A missing
/// username or password defaults to lapin's `guest`, so the management login is
/// the same user the AMQP login was.
fn management_client(profile: &RabbitmqProfile, password: Option<String>) -> MgmtClient {
    let set = |s: &Option<String>| s.clone().filter(|s| !s.is_empty());
    let username = set(&profile.management_username)
        .or_else(|| set(&profile.username))
        .unwrap_or_else(|| DEFAULT_CREDENTIAL.to_string());
    MgmtClient::new(
        profile.management_base_url(),
        profile.vhost.clone(),
        username,
        password.unwrap_or_else(|| DEFAULT_CREDENTIAL.to_string()),
    )
}

/// Owns a tap's dedicated connection/channel/consumer so the stream stays alive.
/// Dropping it closes the connection, which deletes the exclusive spy queue.
struct TapState {
    // Kept alive for the life of the stream (dropped → spy queue auto-deletes).
    _connection: Connection,
    _channel: Channel,
    consumer: Consumer,
    /// The exchange name reported as the event source.
    exchange: String,
}

/// Open a dedicated, non-destructive tap on `exchange` and return its event
/// stream. Declares a temporary spy queue, binds it with `binding_key`, and
/// consumes the copies routed to it.
async fn open_exchange_tap(
    url: &str,
    props: ConnectionProperties,
    exchange: String,
    binding_key: String,
) -> anyhow::Result<BrokerEventStream> {
    // Each lapin call attaches human context with `.context`/`.with_context`,
    // keeping the lapin error as the source so its AMQP reply code (e.g.
    // NOT_FOUND vs ACCESS_REFUSED) survives in the `{:#}` chain rather than being
    // flattened — and the messages name what we were doing, not a guessed cause.
    let connection = Connection::connect(url, props)
        .await
        .context("opening tap connection")?;
    let channel = connection
        .create_channel()
        .await
        .context("opening tap channel")?;

    // Passively declare the exchange first so a missing/inaccessible exchange
    // fails here (with the broker's reply code in the chain) instead of as an
    // opaque bind error. In passive mode the broker only checks existence and
    // ignores `kind`, so the `Topic` placeholder never conflicts with the
    // exchange's real type.
    channel
        .exchange_declare(
            exchange.as_str().into(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("tapping exchange `{exchange}`"))?;

    // A temporary spy queue: server-named (empty name → broker generates one),
    // `exclusive` (only this connection may use it) and `auto_delete` (removed
    // when the connection drops). Never durable — it must not outlive the tail.
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("declaring the spy queue")?;
    let queue_name = queue.name().as_str().to_owned();

    // Bind the spy queue: the broker now routes a COPY of every matching message
    // to it, on top of whatever real queues are bound. Non-destructive by
    // construction — we never consume from a pre-existing queue.
    channel
        .queue_bind(
            queue_name.as_str().into(),
            exchange.as_str().into(),
            binding_key.as_str().into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| {
            format!("binding the spy queue to exchange `{exchange}` (key `{binding_key}`)")
        })?;

    // Consume with auto-ack: acking only discards our own copy from the spy
    // queue (which we drop on close anyway), so it never touches real queues.
    let consumer = channel
        .basic_consume(
            queue_name.as_str().into(),
            format!("keyhole-{}", super::next_conn_seq())
                .as_str()
                .into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("consuming the spy queue")?;

    let state = TapState {
        _connection: connection,
        _channel: channel,
        consumer,
        exchange,
    };
    let stream = stream::unfold(state, |mut st| async move {
        match st.consumer.next().await {
            Some(Ok(delivery)) => {
                let event = delivery_to_event(&st.exchange, delivery);
                Some((event, st))
            }
            // `None` = consumer cancelled / channel closed; `Some(Err)` = a
            // stream-level error. Either way the tap ends.
            ended => {
                if let Some(Err(e)) = ended {
                    tracing::debug!(error = %e, exchange = %st.exchange, "rabbitmq tap ended");
                }
                None
            }
        }
    });
    Ok(Box::pin(stream))
}

/// Build a [`BrokerEvent`] from a received delivery. Factored out of the lapin
/// [`Delivery`] (which is awkward to construct in tests) so the payload/meta
/// logic is unit-testable via [`build_event`].
fn delivery_to_event(exchange: &str, delivery: Delivery) -> BrokerEvent {
    build_event(
        exchange,
        delivery.routing_key.as_str(),
        delivery.redelivered,
        delivery.data,
    )
}

/// Assemble the event: the exchange is the observed source; the per-message
/// routing key (and a redelivery flag, when set) ride along as metadata, the
/// same way a Redis stream entry carries its id.
fn build_event(exchange: &str, routing_key: &str, redelivered: bool, data: Vec<u8>) -> BrokerEvent {
    let mut meta = vec![("routing_key".to_string(), routing_key.to_string())];
    if redelivered {
        meta.push(("redelivered".to_string(), "true".to_string()));
    }
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: exchange.to_string(),
        payload: Payload::classify(data),
        meta,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body bytes a test connection's peek fetches per message.
    const PREVIEW: usize = 4096;

    fn profile(tls: bool) -> RabbitmqProfile {
        RabbitmqProfile {
            name: "rmq".into(),
            host: "rabbit.example.com".into(),
            port: if tls { 5671 } else { 5672 },
            vhost: "/".into(),
            username: Some("user".into()),
            password: None,
            tls,
            management_url: None,
            management_username: None,
            management_password: None,
        }
    }

    #[test]
    fn url_includes_scheme_encoded_credentials_and_default_vhost() {
        let conn = RabbitmqConnection::new(profile(false), Some("p@ss/word".into()), None, PREVIEW);
        // The default vhost "/" must be percent-encoded to %2F (not a bare slash,
        // which would select the empty vhost).
        assert_eq!(
            conn.url(),
            "amqp://user:p%40ss%2Fword@rabbit.example.com:5672/%2F"
        );
        let tls = RabbitmqConnection::new(profile(true), Some("x".into()), None, PREVIEW);
        assert!(tls.url().starts_with("amqps://"));
        assert!(tls.url().ends_with(":5671/%2F"));
    }

    #[test]
    fn url_encodes_custom_vhost_and_omits_absent_userinfo() {
        let mut p = profile(false);
        p.username = None;
        p.vhost = "my/host".into();
        let conn = RabbitmqConnection::new(p, None, None, PREVIEW);
        assert_eq!(conn.url(), "amqp://rabbit.example.com:5672/my%2Fhost");
    }

    #[test]
    fn build_event_classifies_payload_and_attaches_routing_key() {
        let ev = build_event("orders", "order.created", false, b"hello".to_vec());
        assert_eq!(ev.source, "orders");
        assert_eq!(ev.payload, Payload::Utf8("hello".into()));
        assert_eq!(ev.meta("routing_key"), Some("order.created"));
        // No redelivery flag on a first delivery.
        assert_eq!(ev.meta("redelivered"), None);

        // JSON bodies are recognised as JSON.
        let j = build_event("ex", "k", false, br#"{"a":1}"#.to_vec());
        assert!(matches!(j.payload, Payload::Json(_)));

        // Non-UTF-8 bodies survive as binary (base64 when displayed/recorded).
        let b = build_event("ex", "k", true, vec![0x00, 0xff]);
        assert!(matches!(b.payload, Payload::Binary(_)));
        // The redelivery flag is surfaced when set.
        assert_eq!(b.meta("redelivered"), Some("true"));
    }

    #[tokio::test]
    async fn default_binding_key_tap_warns_about_non_topic_exchanges() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW);
        // The catch-all `#` default → advisory that direct exchanges
        // need an explicit key (else the tail is silently empty).
        let notice = conn
            .tail_notice(&SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "#".into(),
            })
            .await
            .expect("default `#` key should produce a notice");
        assert!(notice.contains("direct exchange"), "{notice}");
        // An explicit binding key → the user chose it, so no advisory.
        let none = conn
            .tail_notice(&SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "orders.*".into(),
            })
            .await;
        assert!(none.is_none());
    }

    /// The `Authorization` header the management probe sends for `profile`,
    /// whose API is pointed at a stub that answers the probe.
    async fn management_login(
        mut profile: RabbitmqProfile,
        management_password: Option<&str>,
    ) -> String {
        use crate::broker::http::stub::{header, stub_server};
        let (base_url, server) = stub_server(vec![
            (200, r#"{"name":"u","tags":["management"]}"#.into()),
            (200, r#"{"rabbitmq_version":"4.4.0"}"#.into()),
        ]);
        profile.management_url = Some(base_url);
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("amqp-pw".into()),
            management_password.map(str::to_string),
            PREVIEW,
        );
        conn.mgmt.run_probe().await;
        assert!(conn.mgmt.probe().is_some(), "{:?}", conn.mgmt.error());
        let requests = server.join().unwrap();
        header(&requests[0], "authorization").unwrap().to_string()
    }

    #[tokio::test]
    async fn management_logs_in_as_the_amqp_user_unless_overridden() {
        use crate::broker::http::basic_auth;
        // The AMQP username, with the password `start_connect` resolved for the
        // management API.
        assert_eq!(
            management_login(profile(false), Some("pw")).await,
            basic_auth("user", Some("pw"))
        );
        // `management_username` overrides the AMQP username.
        let mut p = profile(false);
        p.management_username = Some("monitor".into());
        assert_eq!(
            management_login(p, Some("pw")).await,
            basic_auth("monitor", Some("pw"))
        );
        // No credentials at all: lapin's `guest`/`guest`, like the AMQP login.
        let mut anon = profile(false);
        anon.username = None;
        assert_eq!(
            management_login(anon, None).await,
            basic_auth("guest", Some("guest"))
        );
    }

    /// A Phase 0 fixture from `rabbitmq/testdata/` (real broker replies).
    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!("rabbitmq/testdata/", $name, ".json")).to_string()
        };
    }

    /// A connection whose management API is a stub answering `replies` in
    /// order, after the probe's two (`stats_disabled` as the overview says).
    /// The join handle yields the raw requests.
    async fn listing_conn(
        stats_disabled: bool,
        replies: Vec<(u16, String)>,
    ) -> (RabbitmqConnection, std::thread::JoinHandle<Vec<String>>) {
        probed_conn("4.4.0", stats_disabled, replies).await
    }

    /// [`listing_conn`] for a broker reporting `version`.
    async fn probed_conn(
        version: &str,
        stats_disabled: bool,
        replies: Vec<(u16, String)>,
    ) -> (RabbitmqConnection, std::thread::JoinHandle<Vec<String>>) {
        use crate::broker::http::stub::stub_server;
        let mut all = vec![
            (200, fixture!("whoami.administrator.v4")),
            (
                200,
                format!(r#"{{"rabbitmq_version":"{version}","disable_stats":{stats_disabled}}}"#),
            ),
        ];
        all.extend(replies);
        let (base_url, server) = stub_server(all);
        let mut p = profile(false);
        p.management_url = Some(base_url);
        let mut conn = RabbitmqConnection::new(p, None, Some("pw".into()), PREVIEW);
        conn.mgmt.run_probe().await;
        assert!(conn.mgmt.probe().is_some(), "{:?}", conn.mgmt.error());
        (conn, server)
    }

    fn find<'a>(list: &'a DestinationList, name: &str, kind: DestKind) -> &'a DestinationInfo {
        list.items
            .iter()
            .find(|d| d.name == name && d.kind == kind)
            .unwrap_or_else(|| panic!("{name} listed"))
    }

    #[tokio::test]
    async fn lists_queues_then_exchanges_without_the_default_exchange() {
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("queues.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();

        // 17 queues, then the 13 exchanges less the default one.
        assert_eq!(list.items.len(), 17 + 12);
        assert!(!list.truncated && !list.stats_disabled);
        let first_exchange = list
            .items
            .iter()
            .position(|d| d.kind == DestKind::Exchange)
            .unwrap();
        assert_eq!(first_exchange, 17, "queues come first");
        assert!(list.items[first_exchange..]
            .iter()
            .all(|d| d.kind == DestKind::Exchange));
        assert!(
            list.items.iter().all(|d| !d.name.is_empty()),
            "the default exchange can't be bound, so it isn't listed"
        );

        let busy = find(&list, "spike.consumed", DestKind::Queue);
        assert_eq!(busy.queue_type, Some(QueueType::Classic));
        assert_eq!(busy.exchange_type, None);
        assert_eq!(
            (busy.messages, busy.ready, busy.unacked, busy.consumers),
            (Some(1895), Some(1875), Some(20), Some(2))
        );
        assert_eq!(busy.publish_rate, Some(9.8));
        // `type` decides the kind (4.x puts `x-queue-type` in every queue's
        // arguments).
        let quorum = find(&list, "spike.quorum", DestKind::Queue);
        assert_eq!(quorum.queue_type, Some(QueueType::Quorum));
        let stream = find(&list, "spike.stream", DestKind::Queue);
        assert_eq!(stream.queue_type, Some(QueueType::Stream));

        let topic = find(&list, "spike.topic", DestKind::Exchange);
        assert_eq!(topic.exchange_type, Some(ExchangeType::Topic));
        assert_eq!(topic.publish_rate, Some(9.8), "publish_in, not publish_out");
        assert_eq!(topic.messages, None);
        let headers = find(&list, "amq.headers", DestKind::Exchange);
        assert_eq!(headers.exchange_type, Some(ExchangeType::Headers));
        // `amq.*` stays, internal ones marked.
        let trace = find(&list, "amq.rabbitmq.trace", DestKind::Exchange);
        assert!(trace.internal);
        assert!(!find(&list, "amq.topic", DestKind::Exchange).internal);
    }

    #[tokio::test]
    async fn lists_with_metrics_disabled_carry_no_counts_or_rates() {
        let (mut conn, server) = listing_conn(
            true,
            vec![
                (200, fixture!("queues.no-metrics.v4")),
                (200, fixture!("exchanges.no-metrics.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert!(list.stats_disabled, "the probe's overview said why");
        assert_eq!(list.items.len(), 17 + 12);
        for d in &list.items {
            assert_eq!(
                (d.messages, d.ready, d.unacked, d.consumers, d.publish_rate),
                (None, None, None, None, None),
                "{}",
                d.name
            );
        }
        // The kinds still come through.
        let stream = find(&list, "spike.stream", DestKind::Queue);
        assert_eq!(stream.queue_type, Some(QueueType::Stream));
    }

    #[tokio::test]
    async fn listing_before_a_good_probe_fails_fast_and_names_the_url() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW);
        let err = conn.list_destinations(false).await.unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "management API unavailable at http://rabbit.example.com:15672: the management \
             API has not been probed yet"
        );
    }

    #[tokio::test]
    async fn a_refresh_reprobes_a_failed_api_and_the_periodic_one_does_not() {
        use crate::broker::http::stub::stub_server;
        let (base_url, server) = stub_server(vec![
            (200, fixture!("whoami.administrator.v3")),
            (200, r#"{"rabbitmq_version":"3.13.7"}"#.to_string()),
            (200, fixture!("queues.empty-vhost.v4")),
            (200, fixture!("queues.empty-vhost.v4")),
        ]);
        let mut p = profile(false);
        p.management_url = Some(base_url);
        let mut conn = RabbitmqConnection::new(p, None, None, PREVIEW);
        assert!(conn.mgmt.error().is_some(), "not probed yet");

        // Without `reprobe` the recorded error comes straight back.
        conn.list_destinations(false).await.unwrap_err();
        assert!(conn.mgmt.error().is_some());
        // With it, the API is probed again and then listed.
        let list = conn.list_destinations(true).await.expect("lists");
        assert!(list.items.is_empty() && !list.truncated);
        assert_eq!(server.join().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn an_api_that_stops_answering_fails_fast_until_a_reprobe() {
        // The stub answers the probe, then stops listening.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let err = format!("{:#}", conn.list_destinations(false).await.unwrap_err());
        assert!(err.contains("could not reach the management API"), "{err}");
        assert!(
            conn.mgmt.error().is_some(),
            "the next listing won't wait on the timeouts again"
        );
        let again = format!("{:#}", conn.list_destinations(false).await.unwrap_err());
        assert!(
            again.starts_with("management API unavailable at"),
            "{again}"
        );
    }

    #[tokio::test]
    async fn a_refused_listing_keeps_the_api_usable() {
        let (mut conn, server) =
            listing_conn(false, vec![(401, fixture!("error.401.vhost-access.v3"))]).await;
        let err = format!("{:#}", conn.list_destinations(false).await.unwrap_err());
        server.join().unwrap();
        assert!(err.contains("no permissions on vhost"), "{err}");
        assert!(conn.mgmt.client().is_ok(), "a refusal is cheap to retry");
    }

    #[tokio::test]
    async fn tap_notice_follows_the_listed_exchange_type() {
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("queues.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        let tap = |name: &str, key: &str| SubSpec::Exchange {
            exchange: name.into(),
            binding_key: key.into(),
        };
        // `#` fits a topic or fanout exchange, and a headers exchange ignores
        // it: the tap gets every message.
        assert_eq!(conn.tail_notice(&tap("spike.topic", "#")).await, None);
        assert_eq!(conn.tail_notice(&tap("spike.fanout", "#")).await, None);
        assert_eq!(conn.tail_notice(&tap("spike.headers", "#")).await, None);
        // A known direct exchange gets a specific advisory.
        let direct = conn.tail_notice(&tap("spike.direct", "#")).await.unwrap();
        assert!(direct.contains("direct exchange"), "{direct}");
        // An exchange the listing didn't include: the type is unknown.
        let unknown = conn.tail_notice(&tap("elsewhere", "#")).await.unwrap();
        assert!(
            unknown.contains("direct exchange it matches only"),
            "{unknown}"
        );
        // An explicit key is the user's choice: no advisory.
        assert_eq!(conn.tail_notice(&tap("spike.direct", "k")).await, None);
    }

    #[tokio::test]
    async fn only_3x_quorum_queues_with_a_limit_count_peeks_toward_it() {
        let limits = |list: &DestinationList| -> Vec<(String, Option<i64>)> {
            list.items
                .iter()
                .filter(|d| d.kind == DestKind::Queue)
                .map(|d| (d.name.clone(), d.peek_delivery_limit))
                .filter(|(_, limit)| limit.is_some())
                .collect()
        };

        // 3.x: a requeue raises `x-delivery-count`, so the queues with a limit
        // by argument or by policy are marked. The default quorum queue has no
        // limit on 3.x, and classic queues have none at all.
        let (mut conn, server) = probed_conn(
            "3.13.7",
            false,
            vec![
                (200, fixture!("queues.v3")),
                (200, fixture!("exchanges.v3")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert_eq!(
            limits(&list),
            [
                ("spike.quorum.limit".to_string(), Some(3)),
                ("spike.quorum.limit.dlx".to_string(), Some(3)),
                ("spike.quorum.policy".to_string(), Some(5)),
                ("spike.quorum.reject".to_string(), Some(3)),
            ]
        );

        // 4.x: `ack_requeue_true` raises only `x-acquired-count`, so no peek
        // counts toward a limit, the default 20 included.
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("queues.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert!(limits(&list).is_empty(), "{:?}", limits(&list));
    }

    #[test]
    fn a_3x_delivery_limit_is_the_lower_of_argument_and_policy() {
        let queue = |json: &str| -> QueueInfo { serde_json::from_str(json).unwrap() };
        let both = queue(
            r#"{"name":"q","type":"quorum","arguments":{"x-delivery-limit":7},
                "effective_policy_definition":{"delivery-limit":4}}"#,
        );
        assert_eq!(configured_delivery_limit(&both), Some(4));
        // An Erlang empty proplist (`[]`) and a negative (unlimited) value.
        let none = queue(
            r#"{"name":"q","type":"quorum","arguments":{"x-delivery-limit":-1},
                "effective_policy_definition":[]}"#,
        );
        assert_eq!(configured_delivery_limit(&none), None);
        // A 3.x broker counts it; an unknown version is assumed to.
        assert_eq!(queue_info(both.clone(), true).peek_delivery_limit, Some(4));
        assert_eq!(queue_info(both, false).peek_delivery_limit, None);
    }

    // -- peek ------------------------------------------------------------------

    /// The events a `/get` fixture maps to, peeked from queue `q`.
    fn peeked(json: &str) -> Vec<BrokerEvent> {
        let messages: Vec<GetMessage> = serde_json::from_str(json).expect("get reply parses");
        messages.into_iter().map(|m| peek_event("q", m)).collect()
    }

    #[test]
    fn peeked_messages_carry_their_bodies_properties_and_headers() {
        for json in [fixture!("get.classic.v3"), fixture!("get.classic.v4")] {
            let events = peeked(&json);
            assert_eq!(events.len(), 3);
            let [json_body, binary, plain] = &events[..] else {
                unreachable!()
            };
            for ev in &events {
                assert_eq!(ev.source, "q", "the source is the queue");
                assert_eq!(ev.meta("exchange"), Some("(default)"));
                assert_eq!(ev.meta("routing_key"), Some("spike.classic"));
                assert_eq!(ev.meta("truncated"), None);
            }

            // A JSON body with every property and typed headers.
            assert_eq!(
                json_body.payload,
                Payload::Json(
                    r#"{"order_id":42,"status":"created","items":[{"sku":"A-1","qty":2}],"note":"café"}"#
                        .into()
                )
            );
            for (key, value) in [
                ("redelivered", "true"),
                ("content_type", "application/json"),
                ("message_id", "msg-0001"),
                ("correlation_id", "corr-abc"),
                ("delivery_mode", "2 (persistent)"),
                ("content_encoding", "utf-8"),
                ("priority", "5"),
                ("timestamp", "2025-11-07T07:20:00Z"),
                ("type", "order.created"),
                ("app_id", "keyhole-spike"),
                ("user_id", "keyhole"),
                ("reply_to", "spike.replies"),
                ("expiration", "3600000"),
                // Header values: a string as itself, the rest as compact JSON.
                ("headers.x-source", "spike"),
                ("headers.x-attempt", "1"),
                ("headers.x-flag", "true"),
                ("headers.x-ratio", "0.5"),
                ("headers.x-tags", r#"["a","b"]"#),
                ("headers.x-nested", r#"{"k":"v"}"#),
            ] {
                assert_eq!(json_body.meta(key), Some(value), "{key}");
            }

            // A non-UTF-8 body stays binary.
            assert_eq!(
                binary.payload,
                Payload::Binary(b"\x00\x01\x02\xff\xfe\x80keyhole\xc3(".to_vec())
            );
            assert_eq!(binary.meta("delivery_mode"), Some("1 (transient)"));
            assert_eq!(binary.meta("redelivered"), Some("false"));

            // `properties: []`: only how the message arrived.
            assert_eq!(
                plain.payload,
                Payload::Utf8("plain text body, no properties".into())
            );
            let keys: Vec<&str> = plain.meta.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, ["exchange", "routing_key", "redelivered"]);
        }
    }

    #[test]
    fn a_truncated_body_says_how_much_is_shown() {
        for json in [fixture!("get.truncated.v3"), fixture!("get.truncated.v4")] {
            let ev = &peeked(&json)[0];
            // The first 10 of 81 bytes: no longer valid JSON, so plain text.
            assert_eq!(ev.payload, Payload::Utf8(r#"{"order_id"#.into()));
            assert_eq!(ev.meta("truncated"), Some("showing 10 of 81 bytes"));
        }

        // A cut through a multi-byte character drops its first half, so the
        // body still reads as text: "caf" plus half of "é", of 5 bytes.
        let cut = peeked(
            r#"[{"payload":"Y2Fmww==","payload_encoding":"base64","payload_bytes":5,
                 "redelivered":false,"exchange":"ex","routing_key":"k","properties":[]}]"#,
        );
        assert_eq!(cut[0].payload, Payload::Utf8("caf".into()));
        assert_eq!(cut[0].meta("truncated"), Some("showing 3 of 5 bytes"));
        assert_eq!(cut[0].meta("exchange"), Some("ex"));
    }

    #[test]
    fn dead_lettered_and_requeued_messages_show_their_headers() {
        for json in [
            fixture!("get.dead-lettered.v3"),
            fixture!("get.dead-lettered.v4"),
        ] {
            let ev = &peeked(&json)[0];
            assert_eq!(ev.meta("exchange"), Some("spike.dlx"));
            assert_eq!(
                ev.meta("headers.x-first-death-reason"),
                Some("delivery_limit")
            );
            let death = ev.meta("headers.x-death").expect("x-death");
            let parsed: serde_json::Value = serde_json::from_str(death).expect("compact JSON");
            assert_eq!(parsed[0]["reason"], "delivery_limit");
            assert!(!death.contains('\n'), "compact: {death}");
        }
        // The count a requeue raises: `x-delivery-count` on 3.x,
        // `x-acquired-count` on 4.x.
        let v3 = peeked(&fixture!("get.quorum.requeued.v3"));
        assert!(v3.iter().all(|e| e.meta("redelivered") == Some("true")));
        assert_eq!(v3[2].meta("headers.x-delivery-count"), Some("1"));
        let v4 = peeked(&fixture!("get.quorum.requeued.v4"));
        assert_eq!(v4[2].meta("headers.x-acquired-count"), Some("1"));
        assert_eq!(v4[2].meta("headers.x-delivery-count"), None);
    }

    fn peek_req(spec: SubSpec, mode: PeekMode) -> PeekReq {
        PeekReq {
            spec,
            mode,
            limit: 200,
        }
    }

    fn queue_peek(mode: PeekMode) -> PeekReq {
        peek_req(SubSpec::Queue("spike.classic".into()), mode)
    }

    #[tokio::test]
    async fn each_peek_mode_sends_its_ackmode() {
        use crate::broker::http::stub::header;
        for (mode, ackmode) in [
            (PeekMode::Browse, "ack_requeue_true"),
            (PeekMode::Destructive, "ack_requeue_false"),
        ] {
            let (mut conn, server) =
                listing_conn(false, vec![(200, fixture!("get.classic.v4"))]).await;
            let events = conn.peek(queue_peek(mode)).await.expect("peeks");
            assert_eq!(events.len(), 3);
            let raw = server.join().unwrap().pop().unwrap();
            // Every non-alphanumeric byte of the vhost and queue is encoded.
            assert!(
                raw.starts_with("POST /api/queues/%2F/spike%2Eclassic/get "),
                "{raw}"
            );
            assert_eq!(header(&raw, "connection"), Some("close"));
            let body: serde_json::Value =
                serde_json::from_str(raw.split("\r\n\r\n").nth(1).unwrap()).unwrap();
            // The batch limit, and bodies capped at the preview size.
            assert_eq!(
                body,
                serde_json::json!({
                    "count": 200, "ackmode": ackmode, "encoding": "base64", "truncate": PREVIEW
                })
            );
        }
    }

    #[tokio::test]
    async fn skip_mode_sends_no_request() {
        // The stub answers only the probe: a `/get` would find it gone.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        let events = conn.peek(queue_peek(PeekMode::Skip)).await.expect("skips");
        assert!(events.is_empty());
        assert_eq!(server.join().unwrap().len(), 2, "only the probe's requests");
    }

    #[tokio::test]
    async fn an_empty_queue_peeks_to_nothing() {
        let (mut conn, server) = listing_conn(false, vec![(200, fixture!("get.empty.v4"))]).await;
        let events = conn
            .peek(queue_peek(PeekMode::Browse))
            .await
            .expect("peeks");
        server.join().unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn a_stream_refusal_reads_as_the_stream_hint() {
        for json in [
            fixture!("error.400.stream-get.v3"),
            fixture!("error.400.stream-get.v4"),
        ] {
            let (mut conn, server) = listing_conn(false, vec![(400, json)]).await;
            let err = conn
                .peek(queue_peek(PeekMode::Browse))
                .await
                .expect_err("streams refuse /get");
            server.join().unwrap();
            assert_eq!(format!("{err:#}"), STREAM_PEEK_HINT);
            assert!(
                conn.mgmt.client().is_ok(),
                "a refusal leaves the API usable"
            );
        }
    }

    #[tokio::test]
    async fn a_refused_peek_gives_the_brokers_reason() {
        let (mut conn, server) = listing_conn(
            false,
            vec![(401, fixture!("error.401.queue-read-refused.v4"))],
        )
        .await;
        let err = conn.peek(queue_peek(PeekMode::Browse)).await.unwrap_err();
        server.join().unwrap();
        let err = format!("{err:#}");
        assert!(
            err.starts_with(
                "reading queue `spike.classic`: permission denied (HTTP 401): ACCESS_REFUSED"
            ),
            "{err}"
        );
        assert!(conn.mgmt.client().is_ok());
    }

    #[tokio::test]
    async fn a_peek_before_a_good_probe_fails_fast_and_an_unreachable_one_is_recorded() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW);
        let err = conn.peek(queue_peek(PeekMode::Browse)).await.unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "management API unavailable at http://rabbit.example.com:15672: the management \
             API has not been probed yet"
        );

        // The API answers the probe, then stops listening.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let err = format!(
            "{:#}",
            conn.peek(queue_peek(PeekMode::Browse)).await.unwrap_err()
        );
        assert!(err.starts_with("management API unavailable at"), "{err}");
        assert!(err.contains("could not reach the management API"), "{err}");
        assert!(conn.mgmt.error().is_some(), "later calls fail fast");
    }

    #[tokio::test]
    async fn only_a_queue_is_peeked() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW);
        let exchange = SubSpec::Exchange {
            exchange: "ex".into(),
            binding_key: "#".into(),
        };
        let err = conn
            .peek(peek_req(exchange, PeekMode::Browse))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("only a queue holds messages"),
            "{err}"
        );
    }

    #[test]
    fn management_client_targets_the_profile_url_and_starts_unprobed() {
        let conn = RabbitmqConnection::new(profile(true), None, None, PREVIEW);
        assert_eq!(conn.mgmt.base_url(), "https://rabbit.example.com:15671");
        assert!(conn.mgmt.probe().is_none());
        assert!(
            conn.mgmt.client().is_err(),
            "unusable until connect probes it"
        );
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized RabbitMQ (see `docker-compose.yml`): an AMQP
    //! 0.9.1 broker on `127.0.0.1:$KEYHOLE_TEST_RABBITMQ_PORT` (default 5673),
    //! creds `keyhole:keyhole` on vhost `/`. A non-`guest` user is required
    //! because RabbitMQ restricts `guest` to loopback, and the dockerized broker
    //! sees the host connection arriving from the bridge network. Each test uses
    //! a uniquely-named exchange so the suite is parallel-safe. The management
    //! API is on `127.0.0.1:$KEYHOLE_TEST_RABBITMQ_MGMT_PORT` (default 15673).
    use super::*;
    use crate::broker::SubSpec;

    /// The body bytes a test connection's peek fetches per message.
    const PREVIEW: usize = 4096;
    use lapin::options::{
        BasicGetOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions,
    };
    use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    fn test_port() -> u16 {
        std::env::var("KEYHOLE_TEST_RABBITMQ_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5673)
    }

    fn mgmt_port() -> u16 {
        std::env::var("KEYHOLE_TEST_RABBITMQ_MGMT_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(15673)
    }

    fn url() -> String {
        // vhost "/" → "%2F".
        format!("amqp://keyhole:keyhole@127.0.0.1:{}/%2F", test_port())
    }

    fn unique(prefix: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}.{}.{}", std::process::id(), n)
    }

    fn test_profile() -> RabbitmqProfile {
        RabbitmqProfile {
            name: "test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            vhost: "/".into(),
            username: Some("keyhole".into()),
            password: None,
            tls: false,
            // The compose file maps the API off its default port.
            management_url: Some(format!("http://127.0.0.1:{}", mgmt_port())),
            management_username: None,
            management_password: None,
        }
    }

    async fn connected() -> RabbitmqConnection {
        connect_with(test_profile(), "keyhole").await
    }

    /// Connect `profile` with the test password for AMQP and `mgmt_password`
    /// for the management API.
    async fn connect_with(profile: RabbitmqProfile, mgmt_password: &str) -> RabbitmqConnection {
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("keyhole".to_string()),
            Some(mgmt_password.to_string()),
            PREVIEW,
        );
        let caps = conn.connect().await.expect("connect to test RabbitMQ");
        assert_eq!(caps.r#type, crate::broker::BrokerType::Rabbitmq);
        conn
    }

    /// Declare a durable topic exchange over a throwaway connection.
    async fn declare_topic_exchange(name: &str) {
        let connection = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        channel
            .exchange_declare(
                name.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        connection.close(200, "bye".into()).await.ok();
    }

    /// Publish one message to `exchange` with `routing_key`.
    async fn publish(exchange: &str, routing_key: &str, body: &str) {
        let connection = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        connection.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn exchange_tap_receives_published_message_with_routing_key() {
        let exchange = unique("keyhole.it.topic");
        declare_topic_exchange(&exchange).await;

        let mut conn = connected().await;
        // Default `#` binding key → matches every routing key on a topic exchange.
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");

        // The spy queue is bound and consuming by the time `subscribe` returns,
        // and queues retain messages, so a single publish afterwards is observed
        // without the publish/attach race the non-retaining AMQP topic tail has.
        publish(&exchange, "order.created", "hello rabbit").await;

        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(ev.source, exchange);
        assert_eq!(ev.payload, Payload::Utf8("hello rabbit".into()));
        assert_eq!(ev.meta("routing_key"), Some("order.created"));
    }

    #[tokio::test]
    async fn exchange_tap_is_live_only_and_leaves_the_real_queue_intact() {
        let exchange = unique("keyhole.it.topic");
        declare_topic_exchange(&exchange).await;

        // A real queue bound to the exchange — the "production" consumer we must
        // neither steal from nor replay into. Exclusive so it auto-cleans on close.
        let real_queue = unique("keyhole.it.realq");
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let setup_ch = setup.create_channel().await.unwrap();
        setup_ch
            .queue_declare(
                real_queue.as_str().into(),
                QueueDeclareOptions {
                    exclusive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        setup_ch
            .queue_bind(
                real_queue.as_str().into(),
                exchange.as_str().into(),
                "#".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        // Publish BEFORE the tap exists: this reaches the real queue but not the
        // not-yet-created spy queue.
        publish(&exchange, "evt", "backlog-msg").await;

        // Open the tap (which declares + binds its own spy queue), then publish a
        // second message that fans out to both the real queue and the spy queue.
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        publish(&exchange, "evt", "live-msg").await;

        // The tap must observe ONLY the live message, never the pre-subscription
        // backlog: a destructive or replaying implementation that drained/read an
        // existing queue would surface "backlog-msg" here and fail this assertion.
        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(
            ev.payload,
            Payload::Utf8("live-msg".into()),
            "the tap is live-only: it must not see traffic published before it attached"
        );
        drop(stream); // close the tap connection → spy queue auto-deletes

        // The real queue must still hold BOTH messages: the tap ran on a parallel
        // spy queue and consumed only its own copies, taking nothing from here.
        let mut bodies = timeout(Duration::from_secs(5), async {
            let mut out: Vec<String> = Vec::new();
            while out.len() < 2 {
                match setup_ch
                    .basic_get(real_queue.as_str().into(), BasicGetOptions { no_ack: true })
                    .await
                {
                    Ok(Some(msg)) => out.push(String::from_utf8(msg.data.clone()).unwrap()),
                    Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
                    // Surface a real channel error instead of spinning to the timeout.
                    Err(e) => panic!("basic_get on the real queue failed: {e}"),
                }
            }
            out
        })
        .await
        .expect("real queue should still hold both messages");
        bodies.sort();
        assert_eq!(
            bodies,
            vec!["backlog-msg".to_string(), "live-msg".to_string()],
            "exchange tap must leave every message the real queue would have received"
        );

        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn tapping_a_missing_exchange_errors() {
        let mut conn = connected().await;
        let missing = unique("keyhole.it.nope");
        // `BrokerEventStream` is not `Debug`, so match rather than `expect_err`.
        let err = match conn
            .subscribe(SubSpec::Exchange {
                exchange: missing,
                binding_key: "#".into(),
            })
            .await
        {
            Ok(_) => panic!("a non-existent exchange must fail the tap"),
            Err(e) => e,
        };
        // The top-level message names the operation; the broker's reply code is
        // preserved as the error source and shown in the `{:#}` chain (so a
        // NOT_FOUND is distinguishable from e.g. an ACCESS_REFUSED).
        let chain = format!("{err:#}");
        assert!(
            chain.contains("tapping exchange"),
            "error should name the operation: {chain}"
        );
        assert!(
            chain.contains("NOT_FOUND") || chain.to_lowercase().contains("no exchange"),
            "the AMQP reply code must survive in the error chain: {chain}"
        );
    }

    #[tokio::test]
    async fn connect_probes_the_management_api() {
        let conn = connected().await;
        let probe = conn
            .mgmt
            .probe()
            .unwrap_or_else(|| panic!("probe failed: {:?}", conn.mgmt.error()));
        assert_eq!(probe.user, "keyhole");
        assert!(
            probe.tags.iter().any(|t| t == "administrator"),
            "{:?}",
            probe.tags
        );
        assert!(probe.can_monitor());
        let major = probe.major_version().expect("a version like 3.13.7");
        assert!(major >= 3, "{:?}", probe.version);
    }

    #[tokio::test]
    async fn an_unreachable_management_api_leaves_tails_working() {
        // Bind then drop, so the port is (almost certainly) closed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut profile = test_profile();
        profile.management_url = Some(format!("http://{}", listener.local_addr().unwrap()));
        drop(listener);

        let mut conn = connect_with(profile, "keyhole").await;
        let error = conn.mgmt.error().expect("mgmt_error is recorded");
        assert!(
            error.contains("could not reach the management API"),
            "{error}"
        );
        assert!(conn.mgmt.client().is_err(), "later calls fail fast");

        // The AMQP side is unaffected: liveness and an exchange tap still work.
        conn.ping().await.expect("ping");
        let exchange = unique("keyhole.it.topic");
        declare_topic_exchange(&exchange).await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        publish(&exchange, "k", "still tailing").await;
        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(ev.payload, Payload::Utf8("still tailing".into()));
    }

    #[tokio::test]
    async fn a_wrong_management_password_is_explained() {
        let conn = connect_with(test_profile(), "not-the-password").await;
        let error = conn.mgmt.error().expect("mgmt_error is recorded");
        assert!(error.contains("rejected the credentials"), "{error}");
        assert!(error.contains("management_password"), "{error}");
    }

    #[tokio::test]
    async fn list_destinations_lists_a_real_queue_and_the_exchanges() {
        use crate::broker::QueueType;
        use lapin::options::QueueDeleteOptions;

        // A uniquely named queue holding one message. It expires a minute after
        // its last use, so a failed run cleans up too.
        let queue = unique("keyhole.it.listed");
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = setup.create_channel().await.unwrap();
        let mut args = FieldTable::default();
        args.insert("x-expires".into(), lapin::types::AMQPValue::LongInt(60_000));
        channel
            .queue_declare(queue.as_str().into(), QueueDeclareOptions::default(), args)
            .await
            .unwrap();
        publish("", &queue, "count me").await;

        let mut conn = connected().await;
        // The count lands with the broker's next stats emission (every 5 s).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let list = loop {
            let list = conn.list_destinations(false).await.expect("lists");
            let counted = list
                .items
                .iter()
                .any(|d| d.name == queue && d.messages == Some(1));
            if counted || tokio::time::Instant::now() > deadline {
                break list;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert!(!list.stats_disabled);
        let row = list
            .items
            .iter()
            .find(|d| d.name == queue)
            .expect("the new queue is listed");
        assert_eq!(row.kind, DestKind::Queue);
        assert_eq!(row.queue_type, Some(QueueType::Classic));
        assert_eq!((row.messages, row.ready), (Some(1), Some(1)));

        // Queues come first, then the exchanges — `amq.*` included, the
        // default `""` left out.
        let first_exchange = list
            .items
            .iter()
            .position(|d| d.kind == DestKind::Exchange)
            .expect("exchanges are listed");
        assert!(list.items[..first_exchange]
            .iter()
            .all(|d| d.kind == DestKind::Queue));
        let exchange_type = |name: &str| {
            list.items
                .iter()
                .find(|d| d.kind == DestKind::Exchange && d.name == name)
                .and_then(|d| d.exchange_type.clone())
        };
        assert_eq!(exchange_type("amq.topic"), Some(ExchangeType::Topic));
        assert_eq!(exchange_type("amq.direct"), Some(ExchangeType::Direct));
        assert_eq!(exchange_type("amq.fanout"), Some(ExchangeType::Fanout));
        assert_eq!(exchange_type("amq.headers"), Some(ExchangeType::Headers));
        assert!(list.items.iter().all(|d| !d.name.is_empty()));

        // Deleted on the server, it drops out of the next listing.
        channel
            .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
        let list = conn.list_destinations(false).await.expect("lists again");
        assert!(list.items.iter().all(|d| d.name != queue));
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn an_internal_exchange_can_be_tapped() {
        // Clients can't publish to an internal exchange, but a tap only binds
        // a queue to it. The firehose tracer is off, so nothing arrives; the
        // tap just has to attach.
        let mut conn = connected().await;
        let tap = conn
            .subscribe(SubSpec::Exchange {
                exchange: "amq.rabbitmq.trace".into(),
                binding_key: "#".into(),
            })
            .await;
        assert!(tap.is_ok(), "{:#}", tap.err().unwrap());
    }

    #[tokio::test]
    async fn a_headers_exchange_tap_receives_every_message() {
        use lapin::options::ExchangeDeleteOptions;
        use lapin::types::AMQPValue;

        let exchange = unique("keyhole.it.headers");
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = setup.create_channel().await.unwrap();
        channel
            .exchange_declare(
                exchange.as_str().into(),
                ExchangeKind::Headers,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        // The tap binds with `#` and no arguments, which a headers exchange
        // matches against every message.
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("headers exchange tap");

        let mut headers = FieldTable::default();
        headers.insert("kind".into(), AMQPValue::LongString("order".into()));
        for (body, props) in [
            (
                "with headers",
                BasicProperties::default().with_headers(headers),
            ),
            ("without headers", BasicProperties::default()),
        ] {
            channel
                .basic_publish(
                    exchange.as_str().into(),
                    "".into(),
                    BasicPublishOptions::default(),
                    body.as_bytes(),
                    props,
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut bodies = Vec::new();
        for _ in 0..2 {
            match timeout(Duration::from_secs(8), stream.next()).await {
                Ok(Some(ev)) => bodies.push(ev.payload),
                _ => break,
            }
        }
        drop(stream);
        channel
            .exchange_delete(exchange.as_str().into(), ExchangeDeleteOptions::default())
            .await
            .unwrap();
        setup.close(200, "bye".into()).await.ok();

        assert_eq!(
            bodies,
            [
                Payload::Utf8("with headers".into()),
                Payload::Utf8("without headers".into()),
            ]
        );
    }

    /// A throwaway connection and channel for setting up a test's queues.
    async fn setup_channel() -> (Connection, Channel) {
        let connection = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        (connection, channel)
    }

    /// Declare `queue` with `args`. Unless it's durable, it expires a minute
    /// after its last use, so a failed run cleans up too.
    async fn declare_queue(channel: &Channel, queue: &str, durable: bool, mut args: FieldTable) {
        if !durable {
            args.insert("x-expires".into(), lapin::types::AMQPValue::LongInt(60_000));
        }
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable,
                    ..Default::default()
                },
                args,
            )
            .await
            .unwrap_or_else(|e| panic!("declaring {queue}: {e:#}"));
    }

    /// Peek `queue` in `mode`.
    async fn peek_queue(
        conn: &mut RabbitmqConnection,
        queue: &str,
        mode: PeekMode,
    ) -> anyhow::Result<Vec<BrokerEvent>> {
        conn.peek(PeekReq {
            spec: SubSpec::Queue(queue.to_string()),
            mode,
            limit: 200,
        })
        .await
    }

    /// Drain `queue` over `channel` (auto-ack), waiting for `n` messages:
    /// each body and whether it was redelivered, in queue order.
    async fn drain(channel: &Channel, queue: &str, n: usize) -> Vec<(String, bool)> {
        timeout(Duration::from_secs(5), async {
            let mut out = Vec::new();
            while out.len() < n {
                match channel
                    .basic_get(queue.into(), BasicGetOptions { no_ack: true })
                    .await
                {
                    Ok(Some(msg)) => out.push((
                        String::from_utf8(msg.data.clone()).unwrap(),
                        msg.redelivered,
                    )),
                    Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
                    Err(e) => panic!("basic_get on {queue} failed: {e}"),
                }
            }
            out
        })
        .await
        .unwrap_or_else(|_| panic!("{queue} should hold {n} messages"))
    }

    fn bodies(events: &[BrokerEvent]) -> Vec<String> {
        events.iter().map(|e| e.payload.as_text()).collect()
    }

    #[tokio::test]
    async fn browse_peek_leaves_every_message_on_the_queue_flagged_redelivered() {
        let queue = unique("keyhole.it.peek");
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, false, FieldTable::default()).await;
        for body in ["first", "second", "third"] {
            publish("", &queue, body).await;
        }

        let mut conn = connected().await;
        let events = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("browse peek");
        assert_eq!(bodies(&events), ["first", "second", "third"]);
        for ev in &events {
            assert_eq!(ev.source, queue);
            assert_eq!(ev.meta("exchange"), Some("(default)"));
            assert_eq!(ev.meta("routing_key"), Some(queue.as_str()));
            assert_eq!(ev.meta("redelivered"), Some("false"));
        }
        // A second peek reads the same messages, in order, now flagged.
        let again = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("second peek");
        assert_eq!(bodies(&again), ["first", "second", "third"]);
        assert!(again.iter().all(|e| e.meta("redelivered") == Some("true")));

        // The real queue still holds every message, in order, flagged
        // redelivered: the peeks took nothing.
        assert_eq!(
            drain(&channel, &queue, 3).await,
            [
                ("first".to_string(), true),
                ("second".to_string(), true),
                ("third".to_string(), true),
            ]
        );
        let rest = channel
            .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap();
        assert!(rest.is_none(), "and nothing more");
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn destructive_peek_takes_the_messages_off_the_queue() {
        let queue = unique("keyhole.it.peekd");
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, false, FieldTable::default()).await;
        publish("", &queue, "one").await;
        publish("", &queue, "two").await;

        let mut conn = connected().await;
        let events = peek_queue(&mut conn, &queue, PeekMode::Destructive)
            .await
            .expect("destructive peek");
        assert_eq!(bodies(&events), ["one", "two"]);
        let empty = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("peek again");
        assert!(empty.is_empty(), "the destructive peek removed them");
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn peeking_a_stream_queue_explains_why_not() {
        use lapin::options::QueueDeleteOptions;
        let queue = unique("keyhole.it.stream");
        let (setup, channel) = setup_channel().await;
        let mut args = FieldTable::default();
        args.insert(
            "x-queue-type".into(),
            lapin::types::AMQPValue::LongString("stream".into()),
        );
        declare_queue(&channel, &queue, true, args).await;

        let mut conn = connected().await;
        let err = peek_queue(&mut conn, &queue, PeekMode::Browse).await;
        channel
            .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
            .await
            .ok();
        setup.close(200, "bye".into()).await.ok();
        let err = err.expect_err("a stream refuses basic.get");
        assert_eq!(format!("{err:#}"), STREAM_PEEK_HINT);
    }

    #[tokio::test]
    async fn the_listing_marks_a_limited_quorum_queue_on_3x_only() {
        use crate::broker::QueueType;
        use lapin::options::QueueDeleteOptions;
        let queue = unique("keyhole.it.quorum");
        let (setup, channel) = setup_channel().await;
        let mut args = FieldTable::default();
        args.insert(
            "x-queue-type".into(),
            lapin::types::AMQPValue::LongString("quorum".into()),
        );
        args.insert(
            "x-delivery-limit".into(),
            lapin::types::AMQPValue::LongInt(3),
        );
        declare_queue(&channel, &queue, true, args).await;

        let mut conn = connected().await;
        let major = conn.mgmt.probe().and_then(mgmt::Probe::major_version);
        let list = conn.list_destinations(false).await;
        channel
            .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
            .await
            .ok();
        setup.close(200, "bye".into()).await.ok();
        let list = list.expect("lists");
        let row = list
            .items
            .iter()
            .find(|d| d.name == queue)
            .expect("the quorum queue is listed");
        assert_eq!(row.queue_type, Some(QueueType::Quorum));
        let expected = if major == Some(3) { Some(3) } else { None };
        assert_eq!(row.peek_delivery_limit, expected, "RabbitMQ {major:?}");
    }

    /// Poll `fetch` on the blocking pool until `done` holds (the stats behind
    /// connection and consumer lists land a moment after the event).
    async fn eventually<T: Send + 'static>(
        fetch: impl Fn() -> anyhow::Result<T> + Send + Sync + Clone + 'static,
        done: impl Fn(&T) -> bool,
    ) -> T {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let f = fetch.clone();
            let value = tokio::task::spawn_blocking(f)
                .await
                .unwrap()
                .expect("request");
            if done(&value) || tokio::time::Instant::now() > deadline {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    #[tokio::test]
    async fn management_client_lists_and_reads_a_real_queue() {
        let mut conn = connected().await;
        let client = conn.mgmt.client().expect("probe succeeded").clone();

        // A queue whose name needs encoding (`#`, `/`, space, non-ASCII). It
        // expires a minute after its last use, so a failed run cleans up.
        let queue = unique("keyhole.it.q #1/ü");
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = setup.create_channel().await.unwrap();
        let mut args = FieldTable::default();
        args.insert("x-expires".into(), lapin::types::AMQPValue::LongInt(60_000));
        channel
            .queue_declare(queue.as_str().into(), QueueDeclareOptions::default(), args)
            .await
            .unwrap();
        publish("", &queue, "peek me").await;

        let c = client.clone();
        let listing = tokio::task::spawn_blocking(move || c.queues())
            .await
            .unwrap()
            .expect("lists queues");
        let row = listing
            .items
            .iter()
            .find(|q| q.name == queue)
            .expect("the new queue is listed");
        assert_eq!(row.queue_type, crate::broker::QueueType::Classic);

        // A requeueing get reads the message and puts it back, flagged.
        for redelivered in [false, true] {
            let (c, q) = (client.clone(), queue.clone());
            let msgs =
                tokio::task::spawn_blocking(move || c.get(&q, 1, AckMode::AckRequeueTrue, None))
                    .await
                    .unwrap()
                    .expect("gets the message");
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].body().unwrap(), b"peek me");
            assert_eq!(msgs[0].redelivered, redelivered);
            assert_eq!(msgs[0].routing_key, queue);
        }

        // A missing queue's 404 doesn't poison the next request (the 3.13
        // keep-alive quirk).
        let c = client.clone();
        let err = tokio::task::spawn_blocking(move || {
            c.get("keyhole.it.missing", 1, AckMode::AckRequeueTrue, None)
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(format!("{err:#}").contains("queue not found"), "{err:#}");
        let c = client.clone();
        tokio::task::spawn_blocking(move || c.exchanges())
            .await
            .unwrap()
            .expect("the next request still works");

        // This connection, and the consumer behind an open tap, are listed.
        let _tap = conn
            .subscribe(SubSpec::Exchange {
                exchange: "amq.topic".into(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        let c = client.clone();
        let connections = eventually(
            move || c.connections(),
            |rows: &Vec<mgmt::ConnectionInfo>| {
                rows.iter().any(|r| {
                    r.user_provided_name
                        .as_deref()
                        .is_some_and(|n| n.starts_with("keyhole-test-"))
                })
            },
        )
        .await;
        let ours = connections
            .iter()
            .find(|r| {
                r.user_provided_name
                    .as_deref()
                    .is_some_and(|n| n.starts_with("keyhole-test-"))
            })
            .expect("our connection is listed");
        assert_eq!(ours.user.as_deref(), Some("keyhole"));
        assert!(ours.peer_port.is_some() && ours.channels.is_some());
        let c = client.clone();
        let consumers = eventually(
            move || c.consumers(),
            |rows: &Vec<mgmt::ConsumerInfo>| {
                rows.iter().any(|r| r.queue.name.starts_with("amq.gen-"))
            },
        )
        .await;
        let tap = consumers
            .iter()
            .find(|r| r.queue.name.starts_with("amq.gen-"))
            .expect("the tap's consumer is listed");
        assert_eq!(tap.ack_required, Some(false), "the tap auto-acks");
        assert!(tap.channel_details.name.is_some());

        setup.close(200, "bye".into()).await.ok();
    }
}
