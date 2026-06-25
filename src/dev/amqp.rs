//! AMQP 1.0 live publisher for `keyhole dev`.
//!
//! Reuses the app's own `BrokerConnection::publish` (via the factory) so the
//! publish address can never drift from the address the TUI subscribes to — both
//! derive `topic://name` / `queue://name` from the same `SubSpec`.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::fixtures;
use crate::broker::factory::connection_for;
use crate::broker::{BrokerConnection, PublishReq, SubSpec};
use crate::config::ConnectionConfig;

/// Continuous: publish fake messages to the demo topic and queue until `token`
/// is cancelled. `connection` must be the `amqp` profile from the config.
pub async fn publish(
    connection: ConnectionConfig,
    password: Option<String>,
    interval: Duration,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let address = connection.address();
    // `preview_bytes` is a Redis-only concern; AMQP ignores it.
    let mut conn = connection_for(connection, password, 4096)?;
    // Fail fast if the broker is unreachable rather than per-message later.
    conn.connect().await?;
    println!(
        "amqp     → {address} (topic:{} / queue:{}) every {interval:?}",
        fixtures::AMQP_TOPIC,
        fixtures::AMQP_QUEUE
    );
    let mut seq: u64 = 0;
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(interval) => {
                seq += 1;
                publish_once(conn.as_mut(), seq).await?;
            }
        }
    }
    Ok(())
}

/// Publish one message, alternating between the demo topic and queue.
pub async fn publish_once(conn: &mut dyn BrokerConnection, seq: u64) -> anyhow::Result<()> {
    let (spec, kind) = if seq.is_multiple_of(2) {
        (
            SubSpec::Topic(fixtures::AMQP_TOPIC.to_string()),
            "order.shipped",
        )
    } else {
        (
            SubSpec::Queue(fixtures::AMQP_QUEUE.to_string()),
            "order.created",
        )
    };
    let body = fixtures::order_event_json(seq, kind).into_bytes();
    conn.publish(PublishReq { spec, body }).await
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized ActiveMQ: `just test-int`, or
    //! `cargo test --features integration` with the broker on
    //! `127.0.0.1:$KEYHOLE_TEST_AMQP_PORT` (default 5674). ActiveMQ accepts SASL
    //! ANONYMOUS, so no credentials are needed.
    use super::*;
    use crate::config::AmqpProfile;

    fn test_port() -> u16 {
        std::env::var("KEYHOLE_TEST_AMQP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5674)
    }

    fn test_connection() -> ConnectionConfig {
        ConnectionConfig::Amqp(AmqpProfile {
            name: "dev-test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            username: None,
            password: None,
            tls: false,
            destinations: fixtures::amqp_destination_specs().to_vec(),
            management_url: None,
            management_username: None,
            management_password: None,
        })
    }

    #[tokio::test]
    async fn publishes_to_the_demo_topic_and_queue() {
        let mut conn = connection_for(test_connection(), None, 4096).expect("build");
        conn.connect().await.expect("connect to ActiveMQ");
        // Even seq → topic, odd seq → queue: one of each in two ticks.
        for seq in 1..=2 {
            publish_once(conn.as_mut(), seq)
                .await
                .unwrap_or_else(|e| panic!("publish seq {seq}: {e:#}"));
        }
    }
}
