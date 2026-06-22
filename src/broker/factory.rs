//! Construct a [`BrokerConnection`] from a saved connection profile.
//!
//! This is the one place that maps a [`ConnectionConfig`] to a concrete broker
//! client and knows which brokers were compiled into the build. Both the TUI
//! (`app::App::start_connect`) and the headless `record` command go through it,
//! so the feature-gated dispatch lives here once instead of being duplicated.

#[cfg(feature = "amqp")]
use crate::broker::amqp::AmqpConnection;
#[cfg(feature = "rabbitmq")]
use crate::broker::rabbitmq::RabbitmqConnection;
use crate::broker::redis::RedisConnection;
use crate::broker::BrokerConnection;
use crate::config::ConnectionConfig;

/// Build the broker connection for `profile`, with its `password` already
/// resolved. `preview_bytes` bounds how much of a value the Redis inspector
/// fetches; brokers without a value inspector ignore it.
///
/// The returned connection is not yet open — the caller drives
/// [`BrokerConnection::connect`]. Returns an error (rather than panicking) when
/// the profile names a broker whose support was not compiled into this build:
/// the minimal headless/musl build drops AMQP and RabbitMQ.
pub fn connection_for(
    profile: ConnectionConfig,
    password: Option<String>,
    preview_bytes: usize,
) -> anyhow::Result<Box<dyn BrokerConnection>> {
    let conn: Box<dyn BrokerConnection> = match profile {
        ConnectionConfig::Redis(p) => Box::new(RedisConnection::new(p, password, preview_bytes)),
        #[cfg(feature = "amqp")]
        ConnectionConfig::Amqp(p) => Box::new(AmqpConnection::new(p, password)),
        #[cfg(not(feature = "amqp"))]
        ConnectionConfig::Amqp(_) => anyhow::bail!("AMQP support is not compiled in this build"),
        #[cfg(feature = "rabbitmq")]
        ConnectionConfig::Rabbitmq(p) => Box::new(RabbitmqConnection::new(p, password)),
        #[cfg(not(feature = "rabbitmq"))]
        ConnectionConfig::Rabbitmq(_) => {
            anyhow::bail!("RabbitMQ support is not compiled in this build")
        }
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

    #[cfg(feature = "amqp")]
    #[test]
    fn builds_an_amqp_connection_when_compiled() {
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

    #[cfg(not(feature = "amqp"))]
    #[test]
    fn amqp_errors_cleanly_without_the_feature() {
        use crate::config::AmqpProfile;
        let profile = ConnectionConfig::Amqp(AmqpProfile {
            name: "mq".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5672,
            username: None,
            password: None,
            tls: false,
        });
        // `Box<dyn BrokerConnection>` is not `Debug`, so match rather than
        // `unwrap_err()` (which would require the `Ok` type to be `Debug`).
        match connection_for(profile, None, 4096) {
            Ok(_) => panic!("expected an error without the amqp feature"),
            Err(e) => assert!(e.to_string().contains("AMQP support is not compiled")),
        }
    }

    #[cfg(feature = "rabbitmq")]
    #[test]
    fn builds_a_rabbitmq_connection_when_compiled() {
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

    #[cfg(not(feature = "rabbitmq"))]
    #[test]
    fn rabbitmq_errors_cleanly_without_the_feature() {
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
        match connection_for(profile, None, 4096) {
            Ok(_) => panic!("expected an error without the rabbitmq feature"),
            Err(e) => assert!(e.to_string().contains("RabbitMQ support is not compiled")),
        }
    }
}
