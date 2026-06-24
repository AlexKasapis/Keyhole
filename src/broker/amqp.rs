//! AMQP 1.0 implementation of [`BrokerConnection`] (Apache ActiveMQ / Amazon MQ
//! / RabbitMQ 4.x), built on `fe2o3-amqp`.
//!
//! Surfaces a destination *browser*: a user-curated list of topics/queues (AMQP
//! 1.0 cannot enumerate them), a queue message peek, and live tails. There is no
//! stats dashboard or command console (those need a management plane AMQP 1.0
//! lacks; deferred to the RabbitMQ phase — see [`Capabilities::amqp`]).
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

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::types::messaging::{Body, DistributionMode, Source};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::{Connection, Receiver, Session};

use super::{
    BrokerConnection, BrokerEvent, BrokerEventStream, Capabilities, Payload, PeekReq, SubSpec,
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
        let conn = Connection::open(self.container_id.clone(), self.url().as_str())
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
    let mut connection = Connection::open(container_id, url.as_str())
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
                let event = delivery_to_event(&st.source, delivery.into_body());
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
    let mut connection = Connection::open(container_id, url.as_str())
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
                events.push(delivery_to_event(&queue, delivery.into_body()));
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

/// Build a [`BrokerEvent`] from a received AMQP message body, keeping it
/// binary-safe (data sections and non-UTF-8 strings become base64 downstream).
fn delivery_to_event(source: &str, body: Body<Value>) -> BrokerEvent {
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: source.to_string(),
        payload: body_to_payload(body),
        meta: Vec::new(),
    }
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
        assert_eq!(caps.kind, crate::broker::BrokerKind::Amqp);
        conn
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
}
