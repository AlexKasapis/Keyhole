//! Pure fake-data definitions for the `keyhole dev` tooling: the sample Redis
//! keyspace, the message payloads, and the destination / exchange / channel name
//! constants shared by the publishers (and mirrored in `config.dev.toml`).
//!
//! This module is deliberately I/O-free so it is exercised by the default
//! `cargo test` run; the broker writes live in the sibling `redis` / `amqp` /
//! `rabbitmq` modules.

/// Default namespace for every seeded and live-published Redis key. Mirrored by
/// the `dev seed --prefix` default and the keys the live publisher touches.
pub const DEFAULT_PREFIX: &str = "keyhole:demo";

/// AMQP 1.0 topic the publisher emits to — must match the `topic:` entry in
/// `config.dev.toml`'s `dev-activemq` destinations.
pub const AMQP_TOPIC: &str = "keyhole.demo.events";
/// AMQP 1.0 queue the publisher emits to — must match the `queue:` entry in
/// `config.dev.toml`'s `dev-activemq` destinations.
pub const AMQP_QUEUE: &str = "keyhole.demo.orders";

/// RabbitMQ topic exchange the publisher declares and emits to. Tap it in the
/// TUI via `exchange:keyhole.demo` (or `exchange:keyhole.demo/order.*`).
pub const RABBITMQ_EXCHANGE: &str = "keyhole.demo";
/// Routing keys the RabbitMQ publisher rotates through.
pub const RABBITMQ_ROUTING_KEYS: [&str; 3] = ["order.created", "order.shipped", "order.cancelled"];

/// The AMQP destination specs exactly as they must appear in `config.dev.toml`'s
/// `dev-activemq` profile — the single source of truth the consistency test
/// checks the committed file against. Test-only: the publisher works from the
/// bare `AMQP_TOPIC`/`AMQP_QUEUE` names.
#[cfg(test)]
pub fn amqp_destination_specs() -> [String; 2] {
    [format!("topic:{AMQP_TOPIC}"), format!("queue:{AMQP_QUEUE}")]
}

// --- Redis live-publisher key / channel names (relative to a prefix) ---

/// Pub/sub channel the live publisher PUBLISHes to.
pub fn redis_channel(prefix: &str) -> String {
    format!("{prefix}:channel:events")
}
/// Stream key the live publisher XADDs to (also created by the seed).
pub fn redis_stream(prefix: &str) -> String {
    format!("{prefix}:stream:events")
}
/// Counter key the live publisher INCRs.
pub fn redis_counter(prefix: &str) -> String {
    format!("{prefix}:counter:visits")
}
/// A rotating transient key the live publisher SETs / EXPIREs / DELs so the
/// keyspace-notification tail and `MONITOR` see churn.
pub fn redis_live_key(prefix: &str, seq: u64) -> String {
    format!("{prefix}:live:item:{seq}")
}

// --- One-shot seed dataset ---

/// One seeded value, paired with a key suffix (joined to the prefix as
/// `{prefix}:{suffix}`).
pub struct SeedKey {
    pub suffix: &'static str,
    pub value: SeedValue,
}

/// A sample value to write — one per Redis container type, plus a TTL'd string
/// and a counter, so the browser and inspector show one of everything.
pub enum SeedValue {
    /// Plain string.
    Str(&'static str),
    /// String with a TTL (seconds): shows the TTL column and fires an expiry.
    StrTtl(&'static str, u64),
    /// Integer counter (a Redis string under the hood, written via SET).
    Counter(i64),
    List(&'static [&'static str]),
    Set(&'static [&'static str]),
    Hash(&'static [(&'static str, &'static str)]),
    /// `(member, score)` pairs.
    ZSet(&'static [(&'static str, f64)]),
    /// Stream entries; each entry is a list of `(field, value)` pairs.
    Stream(&'static [&'static [(&'static str, &'static str)]]),
}

impl SeedValue {
    /// A short tag naming the value's Redis type — used by tests to assert the
    /// seed set covers every container type.
    #[cfg(test)]
    pub fn type_tag(&self) -> &'static str {
        match self {
            SeedValue::Str(_) | SeedValue::StrTtl(..) | SeedValue::Counter(_) => "string",
            SeedValue::List(_) => "list",
            SeedValue::Set(_) => "set",
            SeedValue::Hash(_) => "hash",
            SeedValue::ZSet(_) => "zset",
            SeedValue::Stream(_) => "stream",
        }
    }
}

/// The full one-shot seed set: at least one key of every Redis container type,
/// plus a short-TTL string and a counter.
pub fn redis_seed_set() -> Vec<SeedKey> {
    vec![
        SeedKey {
            suffix: "string:greeting",
            value: SeedValue::Str("hello from keyhole dev"),
        },
        SeedKey {
            suffix: "string:session",
            value: SeedValue::StrTtl("session-token-abc123", 120),
        },
        SeedKey {
            suffix: "counter:visits",
            value: SeedValue::Counter(42),
        },
        SeedKey {
            suffix: "list:recent_orders",
            value: SeedValue::List(&["1001", "1002", "1003", "1004"]),
        },
        SeedKey {
            suffix: "set:active_users",
            value: SeedValue::Set(&["alice", "bob", "carol", "dave"]),
        },
        SeedKey {
            suffix: "hash:order:1001",
            value: SeedValue::Hash(&[
                ("id", "1001"),
                ("customer", "alice"),
                ("total", "59.99"),
                ("status", "shipped"),
            ]),
        },
        SeedKey {
            suffix: "zset:leaderboard",
            value: SeedValue::ZSet(&[
                ("alice", 320.0),
                ("bob", 280.0),
                ("carol", 410.0),
                ("dave", 150.0),
            ]),
        },
        SeedKey {
            suffix: "stream:events",
            value: SeedValue::Stream(&[
                &[("type", "order.created"), ("order", "1001")],
                &[("type", "order.shipped"), ("order", "1001")],
                &[("type", "order.created"), ("order", "1002")],
            ]),
        },
    ]
}

// --- Message payloads ---

const CUSTOMERS: [&str; 4] = ["alice", "bob", "carol", "dave"];

/// A JSON order/event payload for the live publishers. `seq` makes each message
/// distinct and `kind` labels the event. Always valid JSON, so the UI
/// classifies it as `Payload::Json`.
pub fn order_event_json(seq: u64, kind: &str) -> String {
    let order = 1000 + seq;
    let customer = CUSTOMERS[(seq as usize) % CUSTOMERS.len()];
    let total = 10 + (seq % 90);
    format!(
        r#"{{"seq":{seq},"event":"{kind}","order":{order},"customer":"{customer}","total":{total}}}"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn seed_set_covers_every_redis_container_type() {
        let tags: HashSet<&str> = redis_seed_set()
            .iter()
            .map(|k| k.value.type_tag())
            .collect();
        for expected in ["string", "list", "set", "hash", "zset", "stream"] {
            assert!(tags.contains(expected), "seed set is missing a {expected}");
        }
    }

    #[test]
    fn seed_set_includes_a_ttl_key_and_a_counter() {
        let set = redis_seed_set();
        assert!(
            set.iter().any(
                |k| matches!(k.value, SeedValue::StrTtl(_, ttl) if (1..=86_400).contains(&ttl))
            ),
            "expected a string key with a sane TTL"
        );
        assert!(
            set.iter().any(|k| matches!(k.value, SeedValue::Counter(_))),
            "expected a counter key"
        );
    }

    #[test]
    fn order_event_payloads_are_valid_json() {
        for (seq, kind) in [(1u64, "order.created"), (2, "order.shipped"), (90, "x")] {
            let body = order_event_json(seq, kind);
            let parsed: serde_json::Value =
                serde_json::from_str(&body).unwrap_or_else(|_| panic!("invalid JSON: {body}"));
            assert_eq!(parsed["seq"], seq);
            assert_eq!(parsed["event"], kind);
        }
    }

    #[test]
    fn redis_key_helpers_share_the_prefix() {
        let p = DEFAULT_PREFIX;
        for key in [
            redis_channel(p),
            redis_stream(p),
            redis_counter(p),
            redis_live_key(p, 7),
        ] {
            assert!(key.starts_with(&format!("{p}:")), "{key} is not namespaced");
        }
    }

    #[test]
    fn amqp_and_rabbitmq_constants_are_well_formed() {
        // AMQP destination specs are `kind:name` with a non-empty name.
        for spec in amqp_destination_specs() {
            let (kind, name) = spec.split_once(':').expect("spec is kind:name");
            assert!(matches!(kind, "topic" | "queue"), "bad kind in {spec}");
            assert!(!name.is_empty(), "empty destination name in {spec}");
        }
        // RabbitMQ routing keys are dotted, non-empty tokens.
        assert!(!RABBITMQ_EXCHANGE.is_empty());
        assert!(RABBITMQ_ROUTING_KEYS
            .iter()
            .all(|k| !k.is_empty() && k.contains('.')));
    }

    /// Guard the committed example config against drift: its profiles must point
    /// at the docker-compose host ports, and its AMQP destinations must equal
    /// the topic/queue the publisher emits to (the publisher reads this same
    /// file at runtime, so a mismatch here means an empty browse surface).
    #[test]
    fn config_dev_toml_matches_publisher_constants() {
        use crate::config::{self, ConnectionConfig};
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.dev.toml");
        let cfg = config::load(&path).expect("config.dev.toml should parse");

        let find = |kind: &str| {
            cfg.connections
                .iter()
                .find(|c| c.type_label() == kind)
                .unwrap_or_else(|| panic!("config.dev.toml is missing a {kind} profile"))
        };
        // Docker-compose host ports (NOT the app's 5672 factory defaults).
        assert_eq!(find("redis").address(), "127.0.0.1:6379");
        assert_eq!(find("amqp").address(), "127.0.0.1:5674");
        assert_eq!(find("rabbitmq").address(), "127.0.0.1:5673");

        let ConnectionConfig::Amqp(amqp) = find("amqp") else {
            unreachable!("type_label == amqp")
        };
        assert_eq!(
            amqp.destinations,
            amqp_destination_specs().to_vec(),
            "config.dev.toml destinations drifted from the publisher constants"
        );
    }
}
