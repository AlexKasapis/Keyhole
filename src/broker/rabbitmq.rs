//! RabbitMQ (AMQP 0.9.1) implementation of [`BrokerConnection`], built on
//! `lapin`. Works against every RabbitMQ version (3.x and 4.x).
//!
//! Read + record only, exactly like the AMQP 1.0 broker — so it reuses the same
//! Realtime page (see [`Capabilities::rabbitmq`]). The one capability is a
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

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use time::OffsetDateTime;

use lapin::message::Delivery;
use lapin::options::{
    BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, Consumer, ExchangeKind};

use super::{BrokerConnection, BrokerEvent, BrokerEventStream, Capabilities, Payload, SubSpec};
use crate::config::RabbitmqProfile;

/// A connection name shows up in RabbitMQ's management UI, which is handy when
/// observing the broker. This counter keeps every connection (main + each tail)
/// distinct, even for several connections to one broker.
static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

fn next_seq() -> u64 {
    CONN_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// A live (or not-yet-connected) RabbitMQ (AMQP 0.9.1) connection.
pub struct RabbitmqConnection {
    profile: RabbitmqProfile,
    password: Option<String>,
    /// The main connection, kept open for liveness checks.
    conn: Option<Connection>,
}

impl RabbitmqConnection {
    /// Build a connection from a profile and its resolved password. Call
    /// [`BrokerConnection::connect`] to actually establish it.
    pub fn new(profile: RabbitmqProfile, password: Option<String>) -> Self {
        Self {
            profile,
            password,
            conn: None,
        }
    }

    /// Build an `amqp[s]://[user[:pass]@]host:port/vhost` URL with
    /// percent-encoded credentials and vhost. `tls` selects `amqps://` (the
    /// :5671 TLS listener).
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
        // The vhost is a single percent-encoded path segment. The default "/"
        // must encode to "%2F" — a bare trailing "/" is read by AMQP as the
        // *empty* vhost, not the default one.
        url.push('/');
        url.push_str(&enc(&self.profile.vhost));
        url
    }

    /// Connection properties carrying a recognisable name for the broker's
    /// management UI. lapin's default features bind it to the ambient tokio
    /// runtime, so no executor/reactor wiring is needed here.
    fn conn_props(&self) -> ConnectionProperties {
        ConnectionProperties::default()
            .with_connection_name(format!("brokertui-{}-{}", self.profile.name, next_seq()).into())
    }
}

#[async_trait]
impl BrokerConnection for RabbitmqConnection {
    async fn connect(&mut self) -> anyhow::Result<Capabilities> {
        let conn = Connection::connect(&self.url(), self.conn_props())
            .await
            .map_err(|e| anyhow::anyhow!("connecting to RabbitMQ: {e}"))?;
        self.conn = Some(conn);
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
            .map_err(|e| anyhow::anyhow!("liveness check failed: {e}"))?;
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
    let connection = Connection::connect(url, props)
        .await
        .map_err(|e| anyhow::anyhow!("opening tap connection: {e}"))?;
    let channel = connection
        .create_channel()
        .await
        .map_err(|e| anyhow::anyhow!("opening tap channel: {e}"))?;

    // Passively declare the exchange first so a missing/inaccessible exchange
    // fails with a clear message instead of an opaque bind error. In passive
    // mode the broker only checks existence and ignores `kind`, so the `Topic`
    // placeholder never conflicts with the exchange's real type.
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
        .map_err(|e| anyhow::anyhow!("exchange `{exchange}` not found or not accessible: {e}"))?;

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
        .map_err(|e| anyhow::anyhow!("declaring spy queue: {e}"))?;
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
        .map_err(|e| {
            anyhow::anyhow!("binding spy queue to `{exchange}` (key `{binding_key}`): {e}")
        })?;

    // Consume with auto-ack: acking only discards our own copy from the spy
    // queue (which we drop on close anyway), so it never touches real queues.
    let consumer = channel
        .basic_consume(
            queue_name.as_str().into(),
            format!("brokertui-{}", next_seq()).as_str().into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("consuming spy queue: {e}"))?;

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

    fn profile(tls: bool) -> RabbitmqProfile {
        RabbitmqProfile {
            name: "rmq".into(),
            host: "rabbit.example.com".into(),
            port: if tls { 5671 } else { 5672 },
            vhost: "/".into(),
            username: Some("user".into()),
            password: None,
            tls,
        }
    }

    #[test]
    fn url_includes_scheme_encoded_credentials_and_default_vhost() {
        let conn = RabbitmqConnection::new(profile(false), Some("p@ss/word".into()));
        // The default vhost "/" must be percent-encoded to %2F (not a bare slash,
        // which would select the empty vhost).
        assert_eq!(
            conn.url(),
            "amqp://user:p%40ss%2Fword@rabbit.example.com:5672/%2F"
        );
        let tls = RabbitmqConnection::new(profile(true), Some("x".into()));
        assert!(tls.url().starts_with("amqps://"));
        assert!(tls.url().ends_with(":5671/%2F"));
    }

    #[test]
    fn url_encodes_custom_vhost_and_omits_absent_userinfo() {
        let mut p = profile(false);
        p.username = None;
        p.vhost = "my/host".into();
        let conn = RabbitmqConnection::new(p, None);
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
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized RabbitMQ (see `docker-compose.yml`): an AMQP
    //! 0.9.1 broker on `127.0.0.1:$BROKERTUI_TEST_RABBITMQ_PORT` (default 5673),
    //! creds `brokertui:brokertui` on vhost `/`. A non-`guest` user is required
    //! because RabbitMQ restricts `guest` to loopback, and the dockerized broker
    //! sees the host connection arriving from the bridge network. Each test uses
    //! a uniquely-named exchange so the suite is parallel-safe.
    use super::*;
    use crate::broker::SubSpec;
    use lapin::options::{
        BasicGetOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions,
    };
    use lapin::{BasicProperties, Connection, ConnectionProperties};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    fn test_port() -> u16 {
        std::env::var("BROKERTUI_TEST_RABBITMQ_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5673)
    }

    fn url() -> String {
        // vhost "/" → "%2F".
        format!("amqp://brokertui:brokertui@127.0.0.1:{}/%2F", test_port())
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
            username: Some("brokertui".into()),
            password: None,
            tls: false,
        }
    }

    async fn connected() -> RabbitmqConnection {
        let mut conn = RabbitmqConnection::new(test_profile(), Some("brokertui".to_string()));
        let caps = conn.connect().await.expect("connect to test RabbitMQ");
        assert_eq!(caps.kind, crate::broker::BrokerKind::Rabbitmq);
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
        let exchange = unique("brokertui.it.topic");
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
    async fn exchange_tap_is_non_destructive_to_a_real_queue() {
        let exchange = unique("brokertui.it.fanout");
        declare_topic_exchange(&exchange).await;

        // A real, durable queue bound to the exchange — the "production" consumer
        // we must not steal from.
        let real_queue = unique("brokertui.it.realq");
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let setup_ch = setup.create_channel().await.unwrap();
        setup_ch
            .queue_declare(
                real_queue.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
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

        // Start the tap (binds an additional spy queue), then publish once.
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        publish(&exchange, "evt", "shared-copy").await;

        // The tap observes its own copy …
        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(ev.payload, Payload::Utf8("shared-copy".into()));
        drop(stream); // close the tap connection → spy queue auto-deletes

        // … and the real queue still has its own copy: the tap did not consume it.
        let got = timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(Some(msg)) = setup_ch
                    .basic_get(real_queue.as_str().into(), BasicGetOptions { no_ack: true })
                    .await
                {
                    break String::from_utf8(msg.data.clone()).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("real queue should still hold the message");
        assert_eq!(
            got, "shared-copy",
            "exchange tap must not consume real queues"
        );

        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn tapping_a_missing_exchange_errors() {
        let mut conn = connected().await;
        let missing = unique("brokertui.it.nope");
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
        assert!(
            err.to_string().contains("not found") || err.to_string().contains("not accessible"),
            "error should explain the exchange is missing: {err}"
        );
    }
}
