//! AMQP 1.0 implementation of [`BrokerConnection`] (Apache ActiveMQ / Amazon MQ
//! / RabbitMQ 4.x), built on `fe2o3-amqp`.
//!
//! Surfaces a destination *browser*: a user-curated list of topics/queues (AMQP
//! 1.0 cannot enumerate them), a queue message peek (navigable, filterable, with
//! a per-message detail view of the body plus its standard/header/application
//! metadata — see [`message_meta`]), live tails, and a single-message
//! [`publish`](AmqpConnection::publish) — the one *write* the browser performs.
//! There is no stats dashboard or command console (those need a management plane
//! AMQP 1.0 lacks; deferred to the RabbitMQ phase — see [`Capabilities::amqp`]).
//!
//! Tailing a destination:
//! - **Topic** (`topic:name`) — a non-destructive multicast subscription: every
//!   subscriber gets its own copy, so observing never steals messages.
//! - **Queue** (`queue:name`) — opened in **browse** mode (distribution-mode
//!   `copy`), so messages are read without being consumed. Once the link
//!   attaches we check the broker's negotiated source and refuse the tail if it
//!   downgraded to a destructive (non-`copy`) distribution mode, upholding the
//!   non-destructive guarantee even against a broker that ignores `copy`.
//!
//! Peeking a queue ([`AmqpConnection::peek`]) reuses the same machinery as a
//! browse tail but drains a bounded batch and detaches. Its
//! [`PeekMode`](crate::config::PeekMode) decides whether the read is
//! non-destructive (`copy`), skipped, or destructive (a consuming receiver).
//!
//! Each tail/peek owns a dedicated connection + session + receiver (mirroring the
//! Redis dedicated-socket model) so a tail's stream is `'static` and the actor's
//! main connection stays free for liveness checks.

use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream;
use time::OffsetDateTime;

use fe2o3_amqp::connection::{ConnectionHandle, OpenError};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::types::messaging::{Body, Data, DistributionMode, Message, Source};
use fe2o3_amqp::types::primitives::{Binary, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};

use super::{
    BrokerConnection, BrokerEvent, BrokerEventStream, Capabilities, Payload, PeekReq, PublishReq,
    SubSpec,
};
use crate::config::{AmqpProfile, PeekMode};

/// Idle timeouts bounding a queue peek. The broker gets a generous window to
/// deliver the first message after the link attaches (the credit/flow
/// round-trip), then a short window between messages so an exhausted or empty
/// queue returns promptly rather than blocking the inspector.
const PEEK_FIRST_TIMEOUT: Duration = Duration::from_millis(2000);
const PEEK_IDLE_TIMEOUT: Duration = Duration::from_millis(400);

/// A process-unique AMQP container-id with the given prefix. Container-ids must
/// be unique per connection (a broker may reject or confuse two connections
/// sharing one id), so this draws from the shared connection sequence.
fn unique_container_id(prefix: &str) -> String {
    format!("{prefix}-{}", super::next_conn_seq())
}

/// Open an AMQP 1.0 connection, defaulting to **SASL ANONYMOUS** when the URL
/// carries no credentials.
///
/// `fe2o3-amqp` only engages a SASL layer when the URL embeds a username *and*
/// password (it derives [`SaslProfile::Plain`] from the userinfo); with neither
/// it sends a bare AMQP protocol header and no SASL. Apache ActiveMQ (the
/// primary target, and Amazon MQ for ActiveMQ) *requires* the SASL layer even
/// for anonymous access: it answers a bare handshake with a SASL protocol
/// header, which the bare client rejects with `Expecting ProtocolHeader { id:
/// Amqp, .. }, found .. Sasl`. Seeding the builder with [`SaslProfile::Anonymous`]
/// makes a credential-less connect negotiate SASL ANONYMOUS instead. When the
/// URL *does* carry full credentials, `open` derives [`SaslProfile::Plain`] from
/// it and overrides this default (see `fe2o3_amqp`'s `Builder::open`), so the
/// authenticated path is unchanged.
async fn open_connection(
    container_id: String,
    url: &str,
) -> Result<ConnectionHandle<()>, OpenError> {
    Connection::builder()
        .container_id(container_id)
        .sasl_profile(SaslProfile::Anonymous)
        .open(url)
        .await
}

/// A live (or not-yet-connected) AMQP 1.0 connection.
pub struct AmqpConnection {
    profile: AmqpProfile,
    password: Option<String>,
    container_id: String,
    /// The main connection, kept open for liveness checks.
    conn: Option<ConnectionHandle<()>>,
}

impl AmqpConnection {
    /// Build a connection from a profile and its resolved password. Call
    /// [`BrokerConnection::connect`] to actually establish it.
    pub fn new(profile: AmqpProfile, password: Option<String>) -> Self {
        let container_id = unique_container_id(&format!("keyhole-{}", profile.name));
        Self {
            profile,
            password,
            container_id,
            conn: None,
        }
    }

    /// Build an `amqp[s]://[user[:pass]@]host:port` URL with percent-encoded
    /// credentials. `tls` selects `amqps://` (e.g. Amazon MQ on :5671).
    fn url(&self) -> String {
        super::amqp_base_url(
            self.profile.tls,
            &self.profile.host,
            self.profile.port,
            self.profile.username.as_deref(),
            self.password.as_deref(),
        )
    }
}

#[async_trait]
impl BrokerConnection for AmqpConnection {
    async fn connect(&mut self) -> anyhow::Result<Capabilities> {
        let conn = open_connection(self.container_id.clone(), self.url().as_str())
            .await
            .map_err(|e| anyhow::anyhow!("connecting to AMQP broker: {e}"))?;
        self.conn = Some(conn);
        Ok(Capabilities::amqp())
    }

    async fn ping(&mut self) -> anyhow::Result<()> {
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("connection is not established"))?;
        // A session begin/end round-trips to the broker, so a dead peer surfaces
        // here as a disconnect rather than silently lingering.
        let mut session = Session::begin(conn)
            .await
            .map_err(|e| anyhow::anyhow!("liveness check failed: {e}"))?;
        session.end().await.ok();
        Ok(())
    }

    async fn subscribe(&mut self, spec: SubSpec) -> anyhow::Result<BrokerEventStream> {
        let (address, browse, name) = match &spec {
            SubSpec::Topic(t) => (format!("topic://{t}"), false, t.clone()),
            SubSpec::Queue(q) => (format!("queue://{q}"), true, q.clone()),
            other => anyhow::bail!("{} is not an AMQP destination", other.label()),
        };
        // Each tail is a separate connection, so it needs its own container-id.
        let tail_id = unique_container_id(&format!("keyhole-{}-tail", self.profile.name));
        open_tail(self.url(), tail_id, address, name, browse).await
    }

    async fn peek(&mut self, req: PeekReq) -> anyhow::Result<Vec<BrokerEvent>> {
        let queue = match &req.spec {
            SubSpec::Queue(q) => q.clone(),
            // Topics don't retain messages, so there is nothing sitting in them
            // to peek — the UI offers a live tail instead.
            SubSpec::Topic(_) => anyhow::bail!(
                "topics do not retain messages, so there is nothing to peek — tail it instead"
            ),
            other => anyhow::bail!("{} is not an AMQP destination", other.label()),
        };
        // Skip reads nothing — selecting a queue simply shows an empty inspector.
        if matches!(req.mode, PeekMode::Skip) {
            return Ok(Vec::new());
        }
        let destructive = matches!(req.mode, PeekMode::Destructive);
        // A peek opens its own short-lived connection, so it needs its own id.
        let peek_id = unique_container_id(&format!("keyhole-{}-peek", self.profile.name));
        peek_queue(self.url(), peek_id, queue, destructive, req.limit).await
    }

    async fn publish(&mut self, req: PublishReq) -> anyhow::Result<()> {
        let address = match &req.spec {
            SubSpec::Topic(t) => format!("topic://{t}"),
            SubSpec::Queue(q) => format!("queue://{q}"),
            other => anyhow::bail!("{} is not an AMQP destination", other.label()),
        };
        // A publish opens its own short-lived connection, so it needs its own id.
        let pub_id = unique_container_id(&format!("keyhole-{}-pub", self.profile.name));
        publish_message(self.url(), pub_id, address, req.body).await
    }
}

/// Owns a tail's dedicated connection/session/receiver so the stream stays alive.
struct TailState {
    // Kept alive for the life of the stream (dropped → links/connection close).
    _connection: ConnectionHandle<()>,
    _session: fe2o3_amqp::session::SessionHandle<()>,
    receiver: Receiver,
    /// The destination name reported as the event source.
    source: String,
}

/// Open a dedicated tail on `address` and return its event stream. `browse`
/// requests distribution-mode `copy` (non-destructive queue read).
async fn open_tail(
    url: String,
    container_id: String,
    address: String,
    name: String,
    browse: bool,
) -> anyhow::Result<BrokerEventStream> {
    let mut connection = open_connection(container_id, url.as_str())
        .await
        .map_err(|e| anyhow::anyhow!("opening tail connection: {e}"))?;
    let mut session = Session::begin(&mut connection)
        .await
        .map_err(|e| anyhow::anyhow!("beginning tail session: {e}"))?;

    let source = if browse {
        Source::builder()
            .address(address.clone())
            .distribution_mode(DistributionMode::Copy)
            .build()
    } else {
        Source::builder().address(address.clone()).build()
    };
    let receiver = Receiver::builder()
        .name(format!("keyhole-{name}"))
        .source(source)
        .attach(&mut session)
        .await
        .map_err(|e| anyhow::anyhow!("attaching to `{address}`: {e}"))?;

    // Non-destructive guarantee: a queue browse must run on a `copy` link.
    // `Source::distribution_mode` here is the value the broker echoed back on
    // attach; if it is anything other than `copy`, settling deliveries would
    // consume them, so refuse rather than silently consume.
    if browse {
        ensure_browse_nondestructive(receiver.source(), &name)?;
    }

    let state = TailState {
        _connection: connection,
        _session: session,
        receiver,
        source: name,
    };
    let stream = stream::unfold(state, |mut st| async move {
        match st.receiver.recv::<Body<Value>>().await {
            Ok(delivery) => {
                // Settle our copy; non-destructive for topics and browse-mode queues.
                let _ = st.receiver.accept(&delivery).await;
                let event = message_to_event(&st.source, delivery.into_message());
                Some((event, st))
            }
            // Link/connection closed (or a decode error) ends the tail.
            Err(e) => {
                tracing::debug!(error = %e, source = %st.source, "amqp tail ended");
                None
            }
        }
    });
    Ok(Box::pin(stream))
}

/// Peek up to `limit` messages currently sitting in `queue` and return them.
/// Mirrors [`open_tail`]'s connect/attach but drains a bounded batch (bounded by
/// `limit` and an idle timeout) and then detaches, rather than handing back a
/// live stream. `destructive` selects a consuming receiver (default distribution
/// mode); otherwise a `copy` link reads non-destructively, guarded by
/// [`ensure_browse_nondestructive`]. Either way each delivery is accepted: on a
/// `copy` link that only discards our copy; on a consuming link it removes the
/// message from the queue.
async fn peek_queue(
    url: String,
    container_id: String,
    queue: String,
    destructive: bool,
    limit: usize,
) -> anyhow::Result<Vec<BrokerEvent>> {
    let address = format!("queue://{queue}");
    let mut connection = open_connection(container_id, url.as_str())
        .await
        .map_err(|e| anyhow::anyhow!("opening peek connection: {e}"))?;
    let mut session = Session::begin(&mut connection)
        .await
        .map_err(|e| anyhow::anyhow!("beginning peek session: {e}"))?;

    let source = if destructive {
        Source::builder().address(address.clone()).build()
    } else {
        Source::builder()
            .address(address.clone())
            .distribution_mode(DistributionMode::Copy)
            .build()
    };
    let mut receiver = Receiver::builder()
        .name(format!("keyhole-peek-{queue}"))
        .source(source)
        .attach(&mut session)
        .await
        .map_err(|e| anyhow::anyhow!("attaching to `{address}`: {e}"))?;

    // Non-destructive guarantee for a browse peek: refuse if the broker
    // downgraded the link to a consuming distribution mode (reused from the tail).
    if !destructive {
        ensure_browse_nondestructive(receiver.source(), &queue)?;
    }

    let mut events = Vec::new();
    while events.len() < limit {
        // The first message gets a generous window (link credit/flow round-trip);
        // subsequent ones a short one, so an exhausted/empty queue returns fast.
        let wait = if events.is_empty() {
            PEEK_FIRST_TIMEOUT
        } else {
            PEEK_IDLE_TIMEOUT
        };
        match tokio::time::timeout(wait, receiver.recv::<Body<Value>>()).await {
            Ok(Ok(delivery)) => {
                let _ = receiver.accept(&delivery).await;
                events.push(message_to_event(&queue, delivery.into_message()));
            }
            // A receive error ends the peek with whatever was read so far.
            Ok(Err(e)) => {
                tracing::debug!(error = %e, queue = %queue, "amqp peek ended");
                break;
            }
            // Idle timeout: the queue is exhausted (or empty). Return what we have.
            Err(_elapsed) => break,
        }
    }

    // Detach the link and close the connection so the peek leaves nothing behind;
    // best-effort, the results are already in hand.
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    Ok(events)
}

/// Publish `body` to `address` (`topic://name` or `queue://name`) as a single
/// AMQP message with one data section, then detach. Like a tail/peek it opens
/// its own short-lived connection + session, so the actor's main connection is
/// untouched. The body is sent verbatim as opaque bytes (a data section), the
/// most broker-neutral form and a faithful round-trip of keyhole's binary-safe
/// payloads. This is the browser's only write — every other AMQP operation is a
/// non-destructive read.
async fn publish_message(
    url: String,
    container_id: String,
    address: String,
    body: Vec<u8>,
) -> anyhow::Result<()> {
    let mut connection = open_connection(container_id, url.as_str())
        .await
        .map_err(|e| anyhow::anyhow!("opening publish connection: {e}"))?;
    let mut session = Session::begin(&mut connection)
        .await
        .map_err(|e| anyhow::anyhow!("beginning publish session: {e}"))?;
    let mut sender = Sender::attach(
        &mut session,
        format!("keyhole-pub-{address}"),
        address.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("attaching sender to `{address}`: {e}"))?;

    let message = Message::from(Data(Binary::from(body)));
    let outcome = sender
        .send(message)
        .await
        .map_err(|e| anyhow::anyhow!("publishing to `{address}`: {e}"))?;
    // The broker may accept the message or reject/release it; treat anything
    // other than an accept as a failure so the user isn't told it landed when
    // the broker refused it.
    outcome.accepted_or_else(|state| {
        anyhow::anyhow!("broker did not accept the message for `{address}`: {state:?}")
    })?;

    // Detach the link and close the connection (best-effort: the send already
    // succeeded, so a teardown hiccup must not turn a success into an error).
    let _ = sender.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    Ok(())
}

/// Enforce the non-destructive guarantee for a queue browse: the broker's
/// negotiated `source` must not carry a distribution mode other than `copy`.
/// A `move` (or any non-`copy`) mode means settling a delivery consumes it, so
/// we refuse the tail. An *absent* mode is tolerated — not every broker echoes
/// the field back (Apache ActiveMQ, the primary target, is one), and the spec
/// default for an unspecified mode does not imply destructive consumption.
fn ensure_browse_nondestructive(source: &Option<Source>, queue: &str) -> anyhow::Result<()> {
    if let Some(mode) = source.as_ref().and_then(|s| s.distribution_mode.as_ref()) {
        if !matches!(mode, DistributionMode::Copy) {
            anyhow::bail!(
                "broker did not grant non-destructive browse for queue `{queue}` \
                 (distribution-mode `{mode:?}`); refusing to consume messages"
            );
        }
    }
    Ok(())
}

/// Build a [`BrokerEvent`] from a received AMQP message, keeping the body
/// binary-safe (data sections and non-UTF-8 strings become base64 downstream)
/// and surfacing the message's standard properties, header flags, and
/// application properties as event metadata (see [`message_meta`]).
fn message_to_event(source: &str, msg: Message<Body<Value>>) -> BrokerEvent {
    let meta = message_meta(&msg);
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: source.to_string(),
        payload: body_to_payload(msg.body),
        meta,
    }
}

/// Extract the displayable metadata from an AMQP message: the standard
/// properties (`message-id`, `correlation-id`, `subject`, content type, …), the
/// non-default header flags (`durable`, `priority`, `delivery-count`, `ttl`),
/// and every application property under an `app.<key>` name. Only fields the
/// sender actually set are emitted, so an empty message yields no metadata.
///
/// The message-id is surfaced under the key `id` (not `message-id`) so it shows
/// inline in the one-line message list, matching how a Redis stream entry's id
/// rides under the same key.
fn message_meta(msg: &Message<Body<Value>>) -> Vec<(String, String)> {
    let mut meta = Vec::new();
    if let Some(p) = &msg.properties {
        if let Some(id) = &p.message_id {
            meta.push(("id".to_string(), message_id_to_string(id)));
        }
        if let Some(id) = &p.correlation_id {
            meta.push(("correlation-id".to_string(), message_id_to_string(id)));
        }
        if let Some(s) = &p.subject {
            meta.push(("subject".to_string(), s.clone()));
        }
        if let Some(ct) = &p.content_type {
            meta.push(("content-type".to_string(), ct.0.clone()));
        }
        if let Some(ce) = &p.content_encoding {
            meta.push(("content-encoding".to_string(), ce.0.clone()));
        }
        if let Some(to) = &p.to {
            meta.push(("to".to_string(), to.clone()));
        }
        if let Some(rt) = &p.reply_to {
            meta.push(("reply-to".to_string(), rt.clone()));
        }
        if let Some(gid) = &p.group_id {
            meta.push(("group-id".to_string(), gid.clone()));
        }
        if let Some(uid) = &p.user_id {
            meta.push(("user-id".to_string(), bytes_to_string(uid.as_ref())));
        }
        if let Some(ts) = &p.creation_time {
            meta.push((
                "creation-time".to_string(),
                timestamp_to_string(ts.milliseconds()),
            ));
        }
    }
    if let Some(h) = &msg.header {
        if h.durable {
            meta.push(("durable".to_string(), "true".to_string()));
        }
        // The spec default priority is 4; only surface a non-default value.
        if h.priority.0 != 4 {
            meta.push(("priority".to_string(), h.priority.0.to_string()));
        }
        // A non-zero delivery-count means the message was redelivered — a useful
        // signal when browsing a queue, so it is only shown when it matters.
        if h.delivery_count > 0 {
            meta.push(("delivery-count".to_string(), h.delivery_count.to_string()));
        }
        if let Some(ttl) = h.ttl {
            meta.push(("ttl".to_string(), format!("{ttl}ms")));
        }
    }
    if let Some(ap) = &msg.application_properties {
        for (k, v) in ap.0.iter() {
            meta.push((format!("app.{k}"), simple_value_to_string(v)));
        }
    }
    meta
}

/// Render an AMQP `MessageId` (which may be a ulong, uuid, binary, or string) as
/// a display string.
fn message_id_to_string(id: &fe2o3_amqp::types::messaging::MessageId) -> String {
    use fe2o3_amqp::types::messaging::MessageId;
    match id {
        MessageId::Ulong(n) => n.to_string(),
        MessageId::Uuid(u) => uuid_to_string(u.as_inner()),
        MessageId::Binary(b) => bytes_to_string(b.as_ref()),
        MessageId::String(s) => s.clone(),
    }
}

/// Render an application-property value compactly. The common scalar cases
/// (string, symbol, bool, the integer widths) render bare; rarer types fall back
/// to their debug shape so nothing is silently dropped.
fn simple_value_to_string(v: &fe2o3_amqp::types::primitives::SimpleValue) -> String {
    use fe2o3_amqp::types::primitives::SimpleValue as S;
    match v {
        S::Null => String::new(),
        S::Bool(b) => b.to_string(),
        S::Ubyte(n) => n.to_string(),
        S::Ushort(n) => n.to_string(),
        S::Uint(n) => n.to_string(),
        S::Ulong(n) => n.to_string(),
        S::Byte(n) => n.to_string(),
        S::Short(n) => n.to_string(),
        S::Int(n) => n.to_string(),
        S::Long(n) => n.to_string(),
        S::Float(n) => n.0.to_string(),
        S::Double(n) => n.0.to_string(),
        S::Char(c) => c.to_string(),
        S::Timestamp(t) => timestamp_to_string(t.milliseconds()),
        S::Uuid(u) => uuid_to_string(u.as_inner()),
        S::Binary(b) => bytes_to_string(b.as_ref()),
        S::String(s) => s.clone(),
        S::Symbol(s) => s.0.clone(),
        other => format!("{other:?}"),
    }
}

/// Render raw bytes for display: the UTF-8 text if valid, else base64 — the same
/// binary-safe convention the body payloads use.
fn bytes_to_string(bytes: &[u8]) -> String {
    Payload::classify(bytes.to_vec()).as_text()
}

/// Format the 16 bytes of an AMQP UUID as a canonical hyphenated string.
fn uuid_to_string(b: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    )
}

/// Format a millisecond Unix timestamp as an ISO-8601 UTC string, falling back
/// to the raw millisecond count if it is out of the representable range.
fn timestamp_to_string(millis: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(millis as i128 * 1_000_000)
        .map(|dt| dt.to_string())
        .unwrap_or_else(|_| format!("{millis}ms"))
}

/// Convert an AMQP message body into a binary-safe [`Payload`].
fn body_to_payload(body: Body<Value>) -> Payload {
    match body {
        Body::Data(batch) => {
            let mut bytes = Vec::new();
            for data in batch.into_iter() {
                bytes.extend_from_slice(data.0.as_ref());
            }
            Payload::classify(bytes)
        }
        Body::Value(value) => value_to_payload(value.0),
        // An amqp-sequence body has no canonical text form; render its debug
        // shape and classify it rather than silently dropping the content (an
        // observation tool must never swallow a non-empty body).
        Body::Sequence(seq) => Payload::classify(format!("{seq:?}").into_bytes()),
        Body::Empty => Payload::Utf8(String::new()),
    }
}

/// Convert a single AMQP value into a [`Payload`] (strings/binary preserved
/// exactly; other scalars rendered debug-style and then classified).
fn value_to_payload(value: Value) -> Payload {
    match value {
        Value::String(s) => Payload::classify(s.into_bytes()),
        Value::Binary(b) => Payload::classify(b.into_vec()),
        Value::Null => Payload::Utf8(String::new()),
        other => Payload::classify(format!("{other:?}").into_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp::types::messaging::AmqpValue;

    fn profile(tls: bool) -> AmqpProfile {
        AmqpProfile {
            name: "mq".into(),
            host: "broker.example.com".into(),
            port: if tls { 5671 } else { 5672 },
            username: Some("user".into()),
            password: None,
            tls,
            destinations: Vec::new(),
        }
    }

    #[test]
    fn url_includes_scheme_and_encoded_credentials() {
        let conn = AmqpConnection::new(profile(false), Some("p@ss/word".into()));
        assert_eq!(
            conn.url(),
            "amqp://user:p%40ss%2Fword@broker.example.com:5672"
        );
        let tls = AmqpConnection::new(profile(true), Some("x".into()));
        assert!(tls.url().starts_with("amqps://"));
        assert!(tls.url().ends_with(":5671"));
    }

    #[test]
    fn url_without_credentials_omits_userinfo() {
        let mut p = profile(false);
        p.username = None;
        let conn = AmqpConnection::new(p, None);
        assert_eq!(conn.url(), "amqp://broker.example.com:5672");
    }

    #[test]
    fn string_value_body_classifies_as_text_or_json() {
        assert_eq!(
            body_to_payload(Body::Value(AmqpValue(Value::String("hello".into())))),
            Payload::Utf8("hello".into())
        );
        match body_to_payload(Body::Value(AmqpValue(Value::String(r#"{"a":1}"#.into())))) {
            Payload::Json(s) => assert_eq!(s, r#"{"a":1}"#),
            other => panic!("expected json, got {other:?}"),
        }
    }

    #[test]
    fn non_string_scalar_value_is_classified_via_debug() {
        // A numeric AMQP value has no direct text form; it is rendered and then
        // classified (here, the debug form is plain text).
        match body_to_payload(Body::Value(AmqpValue(Value::Long(42)))) {
            Payload::Utf8(s) => assert!(s.contains("42")),
            other => panic!("expected utf8, got {other:?}"),
        }
    }

    #[test]
    fn empty_body_is_empty_text() {
        assert_eq!(body_to_payload(Body::Empty), Payload::Utf8(String::new()));
    }

    #[test]
    fn binary_value_body_is_base64_safe() {
        // Non-UTF-8 bytes survive as Binary (base64 when displayed/recorded).
        match body_to_payload(Body::Value(AmqpValue(Value::Binary(
            vec![0x00, 0xff].into(),
        )))) {
            Payload::Binary(b) => assert_eq!(b, vec![0x00, 0xff]),
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn browse_guard_refuses_explicit_non_copy_mode() {
        // A broker that echoes `move` would consume on settle — refuse the tail.
        let moved = Some(
            Source::builder()
                .address("q")
                .distribution_mode(DistributionMode::Move)
                .build(),
        );
        assert!(ensure_browse_nondestructive(&moved, "q").is_err());
    }

    #[test]
    fn browse_guard_allows_copy_or_unspecified_mode() {
        // Explicit `copy` is fine.
        let copy = Some(
            Source::builder()
                .address("q")
                .distribution_mode(DistributionMode::Copy)
                .build(),
        );
        assert!(ensure_browse_nondestructive(&copy, "q").is_ok());
        // An unspecified mode is tolerated (not every broker echoes it back).
        let unspecified = Some(Source::builder().address("q").build());
        assert!(ensure_browse_nondestructive(&unspecified, "q").is_ok());
        assert!(ensure_browse_nondestructive(&None, "q").is_ok());
    }

    #[tokio::test]
    async fn peek_rejects_a_topic() {
        // Topics don't retain, so peeking one is meaningless — it must error
        // before any connection is attempted (so this needs no live broker).
        let mut conn = AmqpConnection::new(profile(false), None);
        let err = conn
            .peek(PeekReq {
                spec: SubSpec::Topic("events".into()),
                mode: PeekMode::Browse,
                limit: 10,
            })
            .await
            .expect_err("peeking a topic must error");
        assert!(
            format!("{err:#}").contains("topics do not retain"),
            "error should explain topics can't be peeked: {err:#}"
        );
    }

    #[tokio::test]
    async fn peek_skip_returns_empty_without_connecting() {
        // `Skip` short-circuits before opening a connection, so it succeeds with
        // no broker present and yields nothing.
        let mut conn = AmqpConnection::new(profile(false), None);
        let out = conn
            .peek(PeekReq {
                spec: SubSpec::Queue("q".into()),
                mode: PeekMode::Skip,
                limit: 10,
            })
            .await
            .expect("skip peek never touches the broker");
        assert!(out.is_empty(), "skip mode reads nothing");
    }

    use fe2o3_amqp::types::messaging::{
        ApplicationProperties, Header, Message, MessageId, Priority, Properties,
    };
    use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Timestamp};

    /// Find a metadata value by key in an extracted `meta` list.
    fn meta_get<'a>(meta: &'a [(String, String)], key: &str) -> Option<&'a str> {
        meta.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    #[test]
    fn message_meta_surfaces_properties_header_and_app_properties() {
        let props = Properties::builder()
            .message_id(MessageId::String("m-1".into()))
            .correlation_id(MessageId::Ulong(7))
            .subject("orders")
            .content_type(Symbol("application/json".into()))
            .build();
        let header = Header {
            durable: true,
            // The default priority (4) must be omitted; a redelivery count shown.
            priority: Priority::default(),
            ttl: Some(60_000),
            first_acquirer: false,
            delivery_count: 3,
        };
        let app = ApplicationProperties::builder()
            .insert("region", SimpleValue::String("eu".into()))
            .insert("retries", SimpleValue::Int(7))
            .build();
        let msg = Message {
            header: Some(header),
            delivery_annotations: None,
            message_annotations: None,
            properties: Some(props),
            application_properties: Some(app),
            body: Body::Value(AmqpValue(Value::String(r#"{"a":1}"#.into()))),
            footer: None,
        };

        let meta = message_meta(&msg);
        // The message-id rides under `id` so it shows inline in the list line.
        assert_eq!(meta_get(&meta, "id"), Some("m-1"));
        assert_eq!(meta_get(&meta, "correlation-id"), Some("7"));
        assert_eq!(meta_get(&meta, "subject"), Some("orders"));
        assert_eq!(meta_get(&meta, "content-type"), Some("application/json"));
        assert_eq!(meta_get(&meta, "durable"), Some("true"));
        assert_eq!(meta_get(&meta, "delivery-count"), Some("3"));
        assert_eq!(meta_get(&meta, "ttl"), Some("60000ms"));
        assert_eq!(meta_get(&meta, "app.region"), Some("eu"));
        assert_eq!(meta_get(&meta, "app.retries"), Some("7"));
        // The default priority is not emitted.
        assert_eq!(meta_get(&meta, "priority"), None);
    }

    #[test]
    fn message_meta_is_empty_for_a_bare_message() {
        let msg = Message {
            header: None,
            delivery_annotations: None,
            message_annotations: None,
            properties: None,
            application_properties: None,
            body: Body::Value(AmqpValue(Value::String("x".into()))),
            footer: None,
        };
        assert!(message_meta(&msg).is_empty());
    }

    #[test]
    fn message_to_event_carries_body_and_meta() {
        let props = Properties::builder()
            .message_id(MessageId::String("abc".into()))
            .build();
        let msg = Message {
            header: None,
            delivery_annotations: None,
            message_annotations: None,
            properties: Some(props),
            application_properties: None,
            body: Body::Value(AmqpValue(Value::String("hello".into()))),
            footer: None,
        };
        let ev = message_to_event("orders", msg);
        assert_eq!(ev.source, "orders");
        assert_eq!(ev.payload, Payload::Utf8("hello".into()));
        assert_eq!(ev.meta("id"), Some("abc"));
    }

    #[test]
    fn timestamp_renders_iso_and_uuid_is_hyphenated() {
        // 0 ms since the epoch is the Unix epoch instant.
        assert!(timestamp_to_string(0).starts_with("1970-01-01"));
        let bytes = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            uuid_to_string(&bytes),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn simple_value_renders_common_scalars() {
        assert_eq!(
            simple_value_to_string(&SimpleValue::String("hi".into())),
            "hi"
        );
        assert_eq!(simple_value_to_string(&SimpleValue::Int(42)), "42");
        assert_eq!(simple_value_to_string(&SimpleValue::Bool(true)), "true");
        assert_eq!(
            simple_value_to_string(&SimpleValue::Symbol(Symbol("sym".into()))),
            "sym"
        );
        assert_eq!(simple_value_to_string(&SimpleValue::Null), "");
        // A timestamp app-property renders the same ISO form as creation-time.
        assert!(
            simple_value_to_string(&SimpleValue::Timestamp(Timestamp::from_milliseconds(0)))
                .starts_with("1970-01-01")
        );
    }

    #[test]
    fn message_id_renders_each_variant() {
        assert_eq!(message_id_to_string(&MessageId::Ulong(12)), "12");
        assert_eq!(message_id_to_string(&MessageId::String("s".into())), "s");
        // Binary message-ids fall back to the binary-safe text convention.
        assert_eq!(
            message_id_to_string(&MessageId::Binary(vec![0x68, 0x69].into())),
            "hi"
        );
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized ActiveMQ (see `docker-compose.yml`): an AMQP 1.0
    //! broker on `127.0.0.1:$KEYHOLE_TEST_AMQP_PORT` (default 5674), creds
    //! `admin:admin`. Each test uses a uniquely-named destination so the suite is
    //! parallel-safe.
    use super::*;
    use crate::broker::SubSpec;
    use fe2o3_amqp::{Connection, Receiver, Sender, Session};
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    fn test_port() -> u16 {
        std::env::var("KEYHOLE_TEST_AMQP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5674)
    }

    fn url() -> String {
        format!("amqp://admin:admin@127.0.0.1:{}", test_port())
    }

    fn unique(prefix: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}.{}.{}", std::process::id(), n)
    }

    async fn connected() -> AmqpConnection {
        let profile = AmqpProfile {
            name: "test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            username: Some("admin".into()),
            password: None,
            tls: false,
            destinations: Vec::new(),
        };
        let mut conn = AmqpConnection::new(profile, Some("admin".to_string()));
        let caps = conn.connect().await.expect("connect to test ActiveMQ");
        assert_eq!(caps.r#type, crate::broker::BrokerType::Amqp);
        conn
    }

    #[tokio::test]
    async fn connect_without_credentials_uses_sasl_anonymous() {
        // Regression: ActiveMQ requires the SASL layer even for anonymous access,
        // answering a bare AMQP handshake with a SASL protocol header. A
        // credential-less profile must therefore negotiate SASL ANONYMOUS rather
        // than the bare handshake `fe2o3-amqp` does by default (which the broker
        // rejects with "Expecting ProtocolHeader Amqp, found Sasl").
        let profile = AmqpProfile {
            name: "anon".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            username: None,
            password: None,
            tls: false,
            destinations: Vec::new(),
        };
        let mut conn = AmqpConnection::new(profile, None);
        let caps = conn
            .connect()
            .await
            .expect("a credential-less connect must negotiate SASL ANONYMOUS");
        assert_eq!(caps.r#type, crate::broker::BrokerType::Amqp);
        // The connection is live enough to round-trip a session (begin/end).
        conn.ping()
            .await
            .expect("ping after an anonymous connect should succeed");
    }

    /// Send one text message to `address` over a throwaway connection.
    async fn send_one(address: &str, body: &str) {
        let mut connection = Connection::open(unique("it-pub"), url().as_str())
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut sender = Sender::attach(&mut session, "it-sender", address)
            .await
            .unwrap();
        sender
            .send(body)
            .await
            .unwrap()
            .accepted_or_else(|s| format!("{s:?}"))
            .unwrap();
        sender.close().await.ok();
        session.end().await.ok();
        connection.close().await.ok();
    }

    /// Destructively consume one message from `address` within `secs`, if any.
    async fn consume_one(address: &str, secs: u64) -> Option<String> {
        let mut connection = Connection::open(unique("it-con"), url().as_str())
            .await
            .unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut receiver = Receiver::attach(&mut session, "it-con-link", address)
            .await
            .unwrap();
        let got = match timeout(Duration::from_secs(secs), receiver.recv::<String>()).await {
            Ok(Ok(delivery)) => {
                receiver.accept(&delivery).await.ok();
                Some(delivery.body().clone())
            }
            _ => None,
        };
        receiver.close().await.ok();
        session.end().await.ok();
        connection.close().await.ok();
        got
    }

    #[tokio::test]
    async fn topic_tail_receives_published_message() {
        let topic = unique("keyhole.it.topic");
        let mut conn = connected().await;
        // Attach the subscriber first — topics don't retain, so a message must be
        // published while the receiver is live.
        let mut stream = conn
            .subscribe(SubSpec::Topic(topic.clone()))
            .await
            .expect("topic subscribe");

        // Publish repeatedly until the tail observes one: this removes the race
        // between the link attaching and the consumer becoming credit-ready (a
        // single publish can slip through the gap on a non-retaining topic).
        let address = format!("topic://{topic}");
        let publisher = tokio::spawn(async move {
            let mut connection = Connection::open(unique("it-pub"), url().as_str())
                .await
                .unwrap();
            let mut session = Session::begin(&mut connection).await.unwrap();
            let mut sender = Sender::attach(&mut session, "it-sender", address.as_str())
                .await
                .unwrap();
            while sender.send("hello amqp tail").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        });

        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tail timed out")
            .expect("stream ended");
        publisher.abort();
        assert_eq!(ev.source, topic);
        assert_eq!(ev.payload, Payload::Utf8("hello amqp tail".into()));
    }

    #[tokio::test]
    async fn queue_browse_is_non_destructive() {
        let queue = unique("keyhole.it.queue");
        // Seed a message; queues retain until consumed.
        send_one(&format!("queue://{queue}"), "queued-msg").await;

        // Browse it via the AmqpConnection (distribution-mode copy).
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Queue(queue.clone()))
            .await
            .expect("queue browse");
        let ev = timeout(Duration::from_secs(6), stream.next())
            .await
            .expect("browse timed out")
            .expect("stream ended");
        assert_eq!(ev.payload, Payload::Utf8("queued-msg".into()));
        drop(stream); // close the browse link

        // A normal consumer must still find the message: the browse left it in place.
        let still_there = consume_one(&format!("queue://{queue}"), 5).await;
        assert_eq!(
            still_there.as_deref(),
            Some("queued-msg"),
            "queue browse must not consume the message"
        );
    }

    #[tokio::test]
    async fn peek_browse_returns_messages_and_leaves_them() {
        let queue = unique("keyhole.it.peekq");
        send_one(&format!("queue://{queue}"), "m1").await;
        send_one(&format!("queue://{queue}"), "m2").await;

        let mut conn = connected().await;
        let events = conn
            .peek(PeekReq {
                spec: SubSpec::Queue(queue.clone()),
                mode: PeekMode::Browse,
                limit: 10,
            })
            .await
            .expect("browse peek");
        let mut bodies: Vec<String> = events.iter().map(|e| e.payload.as_text()).collect();
        bodies.sort();
        assert_eq!(bodies, vec!["m1".to_string(), "m2".to_string()]);

        // Both messages must still be in the queue afterwards (non-destructive).
        let mut left = Vec::new();
        while left.len() < 2 {
            match consume_one(&format!("queue://{queue}"), 5).await {
                Some(b) => left.push(b),
                None => break,
            }
        }
        left.sort();
        assert_eq!(
            left,
            vec!["m1".to_string(), "m2".to_string()],
            "browse peek must leave every message in the queue"
        );
    }

    #[tokio::test]
    async fn peek_destructive_consumes_messages() {
        let queue = unique("keyhole.it.peekqd");
        send_one(&format!("queue://{queue}"), "gone").await;

        let mut conn = connected().await;
        let events = conn
            .peek(PeekReq {
                spec: SubSpec::Queue(queue.clone()),
                mode: PeekMode::Destructive,
                limit: 10,
            })
            .await
            .expect("destructive peek");
        let bodies: Vec<String> = events.iter().map(|e| e.payload.as_text()).collect();
        assert_eq!(bodies, vec!["gone".to_string()]);

        // The queue must now be empty: a destructive peek consumed the message.
        let after = consume_one(&format!("queue://{queue}"), 2).await;
        assert_eq!(after, None, "destructive peek must consume the message");
    }

    #[tokio::test]
    async fn peek_empty_queue_returns_empty() {
        let queue = unique("keyhole.it.peekempty");
        let mut conn = connected().await;
        // Nothing was sent: attaching auto-creates an empty queue, so the peek
        // drains nothing and returns on the idle timeout.
        let events = conn
            .peek(PeekReq {
                spec: SubSpec::Queue(queue.clone()),
                mode: PeekMode::Browse,
                limit: 10,
            })
            .await
            .expect("peek empty queue");
        assert!(events.is_empty(), "an empty queue peeks to nothing");
    }

    #[tokio::test]
    async fn publish_sends_a_retrievable_message() {
        let queue = unique("keyhole.it.publish");
        let mut conn = connected().await;
        conn.publish(PublishReq {
            spec: SubSpec::Queue(queue.clone()),
            body: b"published-bytes".to_vec(),
        })
        .await
        .expect("publish to queue");

        // Read it back through our own (destructive) peek, proving the message
        // landed and round-trips to the expected payload.
        let events = conn
            .peek(PeekReq {
                spec: SubSpec::Queue(queue.clone()),
                mode: PeekMode::Destructive,
                limit: 5,
            })
            .await
            .expect("peek the published message");
        let bodies: Vec<String> = events.iter().map(|e| e.payload.as_text()).collect();
        assert_eq!(bodies, vec!["published-bytes".to_string()]);
    }

    #[tokio::test]
    async fn publish_to_a_topic_reaches_a_live_tail() {
        let topic = unique("keyhole.it.pubtopic");
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Topic(topic.clone()))
            .await
            .expect("topic subscribe");

        // Topics don't retain, so publish in a loop until the tail observes one
        // (the same attach/credit race the tail test handles).
        let mut pub_conn = connected().await;
        let topic_for_pub = topic.clone();
        let publisher = tokio::spawn(async move {
            while pub_conn
                .publish(PublishReq {
                    spec: SubSpec::Topic(topic_for_pub.clone()),
                    body: b"pub-to-topic".to_vec(),
                })
                .await
                .is_ok()
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        });

        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tail timed out")
            .expect("stream ended");
        publisher.abort();
        assert_eq!(ev.payload, Payload::Utf8("pub-to-topic".into()));
    }
}
