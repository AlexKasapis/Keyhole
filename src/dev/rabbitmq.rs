//! RabbitMQ (AMQP 0.9.1) live publisher for `keyhole dev`.
//!
//! The app's RabbitMQ broker has no publish path, so this uses the raw `lapin`
//! client to declare a durable topic exchange, with durable queues bound to it
//! ([`fixtures::RABBITMQ_QUEUES`], capped at
//! [`fixtures::RABBITMQ_QUEUE_MAX_LENGTH`] messages), and publish to it. Tap the
//! exchange in the TUI via `exchange:keyhole.demo`, or open a queue to peek what
//! it collected.

use std::time::Duration;

use anyhow::Context;
use lapin::options::{
    BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use lapin::protocol::{AMQPErrorKind, AMQPSoftError};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use tokio_util::sync::CancellationToken;

use super::fixtures;
use crate::config::RabbitmqProfile;

/// Build an `amqp[s]://[user:pass@]host:port/vhost` URL (vhost percent-encoded;
/// the default `/` becomes `%2F`), reusing the app's base-URL builder. Fails on
/// a vhost over AMQP's 255 bytes, which lapin would panic on.
fn url(profile: &RabbitmqProfile, password: Option<&str>) -> anyhow::Result<String> {
    crate::broker::rabbitmq::short_string("vhost", &profile.vhost)?;
    let mut url = crate::broker::amqp_base_url(
        profile.tls,
        &profile.host,
        profile.port,
        profile.username.as_deref(),
        password,
    );
    url.push('/');
    url.push_str(&utf8_percent_encode(&profile.vhost, NON_ALPHANUMERIC).to_string());
    Ok(url)
}

/// Open a connection + channel and declare the demo topic exchange (durable, so
/// the TUI's passive declare in the tap succeeds) and the demo queues bound to
/// it.
async fn open(
    profile: &RabbitmqProfile,
    password: Option<&str>,
) -> anyhow::Result<(Connection, Channel)> {
    let conn = Connection::connect(&url(profile, password)?, ConnectionProperties::default())
        .await
        .context("connecting to RabbitMQ")?;
    let mut channel = conn.create_channel().await.context("opening channel")?;
    channel
        .exchange_declare(
            fixtures::RABBITMQ_EXCHANGE.into(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("declaring exchange `{}`", fixtures::RABBITMQ_EXCHANGE))?;
    for queue in &fixtures::RABBITMQ_QUEUES {
        channel = declare_queue(&conn, channel, queue).await?;
    }
    Ok((conn, channel))
}

/// Declare a durable demo queue (a quorum queue when asked, capped at
/// [`fixtures::RABBITMQ_QUEUE_MAX_LENGTH`]) and bind it to the demo exchange.
/// Declaring an existing queue with the same settings is a no-op, so every run
/// can do it.
///
/// One declared with other settings (by a `keyhole dev` from before the cap)
/// is refused with `PRECONDITION_FAILED`, which also closes the channel. It
/// holds only demo messages, so it's deleted and declared afresh on a new
/// channel. Returns the channel to carry on with.
async fn declare_queue(
    conn: &Connection,
    channel: Channel,
    queue: &fixtures::RabbitmqQueue,
) -> anyhow::Result<Channel> {
    let context = || format!("declaring queue `{}`", queue.name);
    let channel = match declare(&channel, queue).await {
        Ok(()) => channel,
        Err(e) if is_precondition_failed(&e) => {
            let channel = conn.create_channel().await.context("reopening channel")?;
            channel
                .queue_delete(queue.name.into(), QueueDeleteOptions::default())
                .await
                .with_context(|| format!("deleting the outdated queue `{}`", queue.name))?;
            declare(&channel, queue).await.with_context(context)?;
            println!(
                "rabbitmq: re-created queue {} (it had other settings: {e})",
                queue.name
            );
            channel
        }
        Err(e) => return Err(e).with_context(context),
    };
    channel
        .queue_bind(
            queue.name.into(),
            fixtures::RABBITMQ_EXCHANGE.into(),
            queue.binding_key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("binding queue `{}`", queue.name))?;
    Ok(channel)
}

/// Declare `queue` with the demo settings.
async fn declare(channel: &Channel, queue: &fixtures::RabbitmqQueue) -> lapin::Result<()> {
    let mut args = FieldTable::default();
    if queue.quorum {
        args.insert(
            "x-queue-type".into(),
            AMQPValue::LongString("quorum".into()),
        );
    }
    args.insert(
        "x-max-length".into(),
        AMQPValue::LongInt(fixtures::RABBITMQ_QUEUE_MAX_LENGTH),
    );
    channel
        .queue_declare(
            queue.name.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await?;
    Ok(())
}

/// Whether the broker refused a declare because the queue exists with other
/// settings.
fn is_precondition_failed(e: &lapin::Error) -> bool {
    matches!(
        e.kind(),
        lapin::ErrorKind::ProtocolError(err)
            if *err.kind() == AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED)
    )
}

/// Continuous: publish fake messages to the demo exchange until `token` is
/// cancelled.
pub async fn publish(
    profile: RabbitmqProfile,
    password: Option<String>,
    interval: Duration,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let (conn, channel) = open(&profile, password.as_deref()).await?;
    let queues: Vec<&str> = fixtures::RABBITMQ_QUEUES.iter().map(|q| q.name).collect();
    println!(
        "rabbitmq → {}:{} (exchange:{}, queues {}) every {interval:?}",
        profile.host,
        profile.port,
        fixtures::RABBITMQ_EXCHANGE,
        queues.join(", ")
    );
    let mut seq: u64 = 0;
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(interval) => {
                seq += 1;
                publish_once(&channel, seq).await?;
            }
        }
    }
    // Best-effort close on shutdown; surface (don't swallow) a failure so a
    // misbehaving broker is at least visible, without failing the command.
    if let Err(e) = conn.close(200, "bye".into()).await {
        eprintln!("warning: failed to close the RabbitMQ connection cleanly: {e}");
    }
    Ok(())
}

/// Publish one message, rotating through the demo routing keys.
pub async fn publish_once(channel: &Channel, seq: u64) -> anyhow::Result<()> {
    let keys = fixtures::RABBITMQ_ROUTING_KEYS;
    let routing_key = keys[(seq as usize) % keys.len()];
    let body = fixtures::order_event_json(seq, routing_key);
    channel
        .basic_publish(
            fixtures::RABBITMQ_EXCHANGE.into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            body.as_bytes(),
            BasicProperties::default(),
        )
        .await
        .context("publishing")?
        .await
        .context("confirming publish")?;
    Ok(())
}

#[cfg(test)]
mod url_tests {
    use super::*;

    fn profile() -> RabbitmqProfile {
        RabbitmqProfile {
            name: "dev".into(),
            host: "localhost".into(),
            port: 5672,
            vhost: "/".into(),
            username: None,
            password: None,
            tls: false,
            management_url: None,
            management_username: None,
            management_password: None,
        }
    }

    #[test]
    fn default_vhost_is_percent_encoded() {
        assert_eq!(url(&profile(), None).unwrap(), "amqp://localhost:5672/%2F");
    }

    #[test]
    fn named_vhost_passes_through() {
        let p = RabbitmqProfile {
            vhost: "prod".into(),
            ..profile()
        };
        assert_eq!(url(&p, None).unwrap(), "amqp://localhost:5672/prod");
    }

    #[tokio::test]
    async fn an_overlong_vhost_is_an_error_not_a_panic() {
        let p = RabbitmqProfile {
            vhost: "v".repeat(256),
            ..profile()
        };
        let expected = "vhost too long (max 255 bytes)";
        assert_eq!(url(&p, None).unwrap_err().to_string(), expected);
        // Refused before dialling, which would fail with another error.
        let err = open(&p, None).await.unwrap_err();
        assert_eq!(format!("{err:#}"), expected);
        let max = RabbitmqProfile {
            vhost: "v".repeat(255),
            ..profile()
        };
        assert!(url(&max, None).is_ok());
    }

    #[test]
    fn url_percent_encodes_credentials() {
        let p = RabbitmqProfile {
            username: Some("u".into()),
            ..profile()
        };
        assert_eq!(
            url(&p, Some("p@ss")).unwrap(),
            "amqp://u:p%40ss@localhost:5672/%2F"
        );
    }

    #[test]
    fn tls_selects_the_amqps_scheme() {
        let p = RabbitmqProfile {
            tls: true,
            ..profile()
        };
        assert!(url(&p, None).unwrap().starts_with("amqps://"));
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized RabbitMQ: `just test-int` (3.x),
    //! `just test-int-rabbitmq4` (4.x), or `cargo test --features integration`
    //! with the broker on `127.0.0.1:$KEYHOLE_TEST_RABBITMQ_PORT` (default
    //! 5673), creds `keyhole/keyhole`.
    use super::*;

    /// Held by the tests that use the demo exchange, so they take turns: one
    /// binds a quorum queue to it and deletes it, and RabbitMQ 3.13 crashes a
    /// channel publishing to the exchange meanwhile (`INTERNAL_ERROR`).
    static DEMO_EXCHANGE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn test_port() -> u16 {
        std::env::var("KEYHOLE_TEST_RABBITMQ_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5673)
    }

    fn test_profile() -> RabbitmqProfile {
        RabbitmqProfile {
            name: "dev-test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            vhost: "/".into(),
            username: Some("keyhole".into()),
            password: None,
            tls: false,
            management_url: None,
            management_username: None,
            management_password: None,
        }
    }

    /// The messages `queue` holds, from a passive declare.
    async fn depth(channel: &Channel, queue: &str) -> u32 {
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap_or_else(|e| panic!("queue `{queue}` exists: {e:#}"))
            .message_count()
    }

    /// Whether `queue` exists as the demo settings declare it: a non-passive
    /// declare succeeds only when its arguments are equivalent. The settings
    /// are spelled out here rather than reusing [`declare`].
    async fn is_capped(conn: &Connection, queue: &fixtures::RabbitmqQueue) -> bool {
        let channel = conn.create_channel().await.unwrap();
        let mut args = FieldTable::default();
        if queue.quorum {
            args.insert(
                "x-queue-type".into(),
                AMQPValue::LongString("quorum".into()),
            );
        }
        args.insert("x-max-length".into(), AMQPValue::LongInt(1000));
        let durable = QueueDeclareOptions {
            durable: true,
            ..Default::default()
        };
        let ok = channel
            .queue_declare(queue.name.into(), durable, args)
            .await
            .is_ok();
        channel.close(200, "bye".into()).await.ok();
        ok
    }

    /// Take the oldest messages off `queue` until it's at least `room` short
    /// of the cap. A full queue drops its oldest message for each new one
    /// (`x-max-length` overflows from the head), so its depth can't show a
    /// publish arriving, and these would have been dropped anyway.
    async fn make_room(channel: &Channel, queue: &str, room: u32) {
        use lapin::options::BasicGetOptions;
        let cap = fixtures::RABBITMQ_QUEUE_MAX_LENGTH.unsigned_abs();
        let excess = (depth(channel, queue).await + room).saturating_sub(cap);
        for _ in 0..excess {
            channel
                .basic_get(queue.into(), BasicGetOptions { no_ack: true })
                .await
                .unwrap_or_else(|e| panic!("making room in `{queue}`: {e:#}"));
        }
    }

    #[tokio::test]
    async fn declares_the_exchange_and_queues_and_publishes() {
        let _turn = DEMO_EXCHANGE.lock().await;
        // A second run re-declares everything without a conflict.
        let (first, _) = open(&test_profile(), Some("keyhole"))
            .await
            .expect("first open");
        first.close(200, "bye".into()).await.ok();
        let (conn, channel) = open(&test_profile(), Some("keyhole"))
            .await
            .expect("connect + declare exchange and queues");
        for queue in &fixtures::RABBITMQ_QUEUES {
            assert!(is_capped(&conn, queue).await, "{} is capped", queue.name);
            // A long-lived dev broker's queues fill up: leave room for our
            // three publishes, and a few from a running `keyhole dev`.
            make_room(&channel, queue.name, 10).await;
        }
        let before: Vec<u32> = futures_util::future::join_all(
            fixtures::RABBITMQ_QUEUES
                .iter()
                .map(|q| depth(&channel, q.name)),
        )
        .await;
        for seq in 1..=3 {
            publish_once(&channel, seq)
                .await
                .unwrap_or_else(|e| panic!("publish seq {seq}: {e:#}"));
        }
        // Each queue's binding matches every routing key the publisher uses.
        // (At least: a running `keyhole dev` may publish too.) Without
        // publisher confirms the last publish may still be routing, so poll.
        for (queue, before) in fixtures::RABBITMQ_QUEUES.iter().zip(before) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let mut after = depth(&channel, queue.name).await;
            while after < before + 3 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
                after = depth(&channel, queue.name).await;
            }
            assert!(after >= before + 3, "{}: {before} → {after}", queue.name);
        }
        conn.close(200, "bye".into()).await.ok();
    }
    #[tokio::test]
    async fn a_demo_queue_declared_with_other_settings_is_replaced() {
        let _turn = DEMO_EXCHANGE.lock().await;
        // As a `keyhole dev` from before the cap left it: durable and
        // uncapped. (A test queue, so a running `keyhole dev` is unaffected.)
        let name = format!("keyhole.it.demo.{}", std::process::id());
        let queue = fixtures::RabbitmqQueue {
            name: Box::leak(name.into_boxed_str()),
            binding_key: "#",
            quorum: true,
        };
        let (conn, channel) = open(&test_profile(), Some("keyhole"))
            .await
            .expect("connect + declare");
        let mut quorum = FieldTable::default();
        quorum.insert(
            "x-queue-type".into(),
            AMQPValue::LongString("quorum".into()),
        );
        channel
            .queue_declare(
                queue.name.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                quorum,
            )
            .await
            .expect("an uncapped quorum queue");
        let capped_before = is_capped(&conn, &queue).await;

        // The demo declare is refused, then deletes it and declares it afresh,
        // on a channel that still works.
        let channel = declare_queue(&conn, channel, &queue).await;
        let capped = is_capped(&conn, &queue).await;
        // Deleted before asserting, so a failure leaves nothing behind.
        let cleanup = conn.create_channel().await.unwrap();
        cleanup
            .queue_delete(queue.name.into(), QueueDeleteOptions::default())
            .await
            .ok();
        assert!(!capped_before, "declared uncapped");
        let channel = channel.expect("re-created");
        assert!(capped, "{} is capped", queue.name);
        publish_once(&channel, 1).await.expect("the channel works");
        conn.close(200, "bye".into()).await.ok();
    }
}
