//! AMQP 1.0 implementation of [`BrokerConnection`] (Apache ActiveMQ / Amazon MQ
//! / RabbitMQ 4.x), built on `fe2o3-amqp`.
//!
//! Only the **read + record** surface is implemented, matching the v1 mandate:
//! there is no key browser, dashboard, or command console (see
//! [`Capabilities::amqp`]). The one capability is tailing a destination:
//! - **Topic** (`topic:name`) — a non-destructive multicast subscription: every
//!   subscriber gets its own copy, so observing never steals messages.
//! - **Queue** (`queue:name`) — opened in **browse** mode (distribution-mode
//!   `copy`), so messages are read without being consumed. Still non-destructive,
//!   upholding the "no destructive ops" rule.
//!
//! Each tail owns a dedicated connection + session + receiver (mirroring the
//! Redis dedicated-socket model) so the returned stream is `'static` and the
//! actor's main connection stays free for liveness checks.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures_util::stream;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use time::OffsetDateTime;

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::types::messaging::{Body, DistributionMode, Source};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp::{Connection, Receiver, Session};

use super::{BrokerConnection, BrokerEvent, BrokerEventStream, Capabilities, Payload, SubSpec};
use crate::config::AmqpProfile;

/// AMQP container-ids must be unique per connection: a broker may reject or
/// confuse two connections sharing one id. This counter makes every connection
/// (main + each tail) distinct, even for several connections to one broker.
static CONTAINER_SEQ: AtomicU64 = AtomicU64::new(0);

/// A process-unique AMQP container-id with the given prefix.
fn unique_container_id(prefix: &str) -> String {
    format!("{prefix}-{}", CONTAINER_SEQ.fetch_add(1, Ordering::Relaxed))
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
        let container_id = unique_container_id(&format!("brokertui-{}", profile.name));
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
        let enc = |s: &str| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string();
        let scheme = if self.profile.tls { "amqps" } else { "amqp" };
        let mut url = format!("{scheme}://");
        let user = self.profile.username.as_deref();
        let pass = self.password.as_deref();
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
        url.push_str(&self.profile.host);
        url.push(':');
        url.push_str(&self.profile.port.to_string());
        url
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
        let tail_id = unique_container_id(&format!("brokertui-{}-tail", self.profile.name));
        open_tail(self.url(), tail_id, address, name, browse).await
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
        .name(format!("brokertui-{name}"))
        .source(source)
        .attach(&mut session)
        .await
        .map_err(|e| anyhow::anyhow!("attaching to `{address}`: {e}"))?;

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
        Body::Sequence(_) | Body::Empty => Payload::Utf8(String::new()),
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
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized ActiveMQ (see `docker-compose.yml`): an AMQP 1.0
    //! broker on `127.0.0.1:$BROKERTUI_TEST_AMQP_PORT` (default 5674), creds
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
        std::env::var("BROKERTUI_TEST_AMQP_PORT")
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
        let topic = unique("brokertui.it.topic");
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
        let queue = unique("brokertui.it.queue");
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
}
