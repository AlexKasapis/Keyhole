//! RabbitMQ (AMQP 0.9.1) live publisher for `keyhole dev`.
//!
//! The app's RabbitMQ broker only taps exchanges (it has no publish path), so
//! this uses the raw `lapin` client to declare a durable topic exchange and
//! publish to it. Tap it in the TUI via `exchange:keyhole.demo`.

use std::time::Duration;

use anyhow::Context;
use lapin::options::{BasicPublishOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use tokio_util::sync::CancellationToken;

use super::fixtures;
use crate::config::RabbitmqProfile;

/// Build an `amqp[s]://[user:pass@]host:port/vhost` URL (vhost percent-encoded;
/// the default `/` becomes `%2F`), reusing the app's base-URL builder.
fn url(profile: &RabbitmqProfile, password: Option<&str>) -> String {
    let mut url = crate::broker::amqp_base_url(
        profile.tls,
        &profile.host,
        profile.port,
        profile.username.as_deref(),
        password,
    );
    url.push('/');
    url.push_str(&utf8_percent_encode(&profile.vhost, NON_ALPHANUMERIC).to_string());
    url
}

/// Open a connection + channel and declare the demo topic exchange (durable, so
/// the TUI's passive declare in the tap succeeds).
async fn open(
    profile: &RabbitmqProfile,
    password: Option<&str>,
) -> anyhow::Result<(Connection, Channel)> {
    let conn = Connection::connect(&url(profile, password), ConnectionProperties::default())
        .await
        .context("connecting to RabbitMQ")?;
    let channel = conn.create_channel().await.context("opening channel")?;
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
    Ok((conn, channel))
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
    println!(
        "rabbitmq → {}:{} (exchange:{}) every {interval:?}",
        profile.host,
        profile.port,
        fixtures::RABBITMQ_EXCHANGE
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
    conn.close(200, "bye".into()).await.ok();
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

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized RabbitMQ: `just test-int`, or
    //! `cargo test --features integration` with the broker on
    //! `127.0.0.1:$KEYHOLE_TEST_RABBITMQ_PORT` (default 5673), creds
    //! `keyhole/keyhole`.
    use super::*;

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
        }
    }

    #[tokio::test]
    async fn declares_the_exchange_and_publishes() {
        let (conn, channel) = open(&test_profile(), Some("keyhole"))
            .await
            .expect("connect + declare exchange");
        for seq in 1..=3 {
            publish_once(&channel, seq)
                .await
                .unwrap_or_else(|e| panic!("publish seq {seq}: {e:#}"));
        }
        conn.close(200, "bye".into()).await.ok();
    }
}
