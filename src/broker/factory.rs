//! Construct a [`BrokerConnection`] from a saved connection profile.
//!
//! This is the one place that maps a [`ConnectionConfig`] to a concrete broker
//! client. The TUI (`app::App::start_connect`) goes through it, so the dispatch
//! lives here once instead of being duplicated.

use crate::broker::amqp::AmqpConnection;
use crate::broker::rabbitmq::RabbitmqConnection;
use crate::broker::redis::RedisConnection;
use crate::broker::BrokerConnection;
use crate::config::ConnectionConfig;

/// Build the broker connection for `profile`, with its `password` already
/// resolved. `preview_bytes` bounds how much of a value the Redis inspector
/// fetches; brokers without a value inspector ignore it.
///
/// The returned connection is not yet open — the caller drives
/// [`BrokerConnection::connect`].
pub fn connection_for(
    profile: ConnectionConfig,
    password: Option<String>,
    preview_bytes: usize,
) -> anyhow::Result<Box<dyn BrokerConnection>> {
    let conn: Box<dyn BrokerConnection> = match profile {
        ConnectionConfig::Redis(p) => Box::new(RedisConnection::new(p, password, preview_bytes)),
        ConnectionConfig::Amqp(p) => Box::new(AmqpConnection::new(p, password)),
        ConnectionConfig::Rabbitmq(p) => Box::new(RabbitmqConnection::new(p, password)),
    };
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedisProfile;

    fn redis_profile() -> ConnectionConfig {
        // `RedisConnection::new` only stores config — no socket is opened — so
        // building one is a pure, side-effect-free construction check.
        ConnectionConfig::Redis(RedisProfile {
            name: "local".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            tls: false,
        })
    }

    #[test]
    fn builds_a_redis_connection() {
        assert!(connection_for(redis_profile(), None, 4096).is_ok());
    }

    #[test]
    fn builds_an_amqp_connection() {
        use crate::config::AmqpProfile;
        let profile = ConnectionConfig::Amqp(AmqpProfile {
            name: "mq".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5672,
            username: None,
            password: None,
            tls: false,
        });
        assert!(connection_for(profile, None, 4096).is_ok());
    }

    #[test]
    fn builds_a_rabbitmq_connection() {
        use crate::config::RabbitmqProfile;
        let profile = ConnectionConfig::Rabbitmq(RabbitmqProfile {
            name: "rmq".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5672,
            vhost: "/".to_string(),
            username: None,
            password: None,
            tls: false,
        });
        assert!(connection_for(profile, None, 4096).is_ok());
    }
}
