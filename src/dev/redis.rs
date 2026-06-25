//! Redis seeder + live publisher for `keyhole dev`.
//!
//! The app's Redis broker is observe-only (browse / inspect / `INFO`), so there
//! is no write path to reuse — this uses the raw `redis` client directly.

use std::time::Duration;

use anyhow::Context;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use redis::aio::MultiplexedConnection;
use tokio_util::sync::CancellationToken;

use super::fixtures::{self, SeedKey, SeedValue};
use crate::config::RedisProfile;

/// Build a `redis://[user:pass@]host:port/db` URL. Dev brokers usually have no
/// auth, but credentials are honoured if a profile sets them.
fn url(profile: &RedisProfile, password: Option<&str>) -> String {
    let enc = |s: &str| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string();
    let mut url = String::from("redis://");
    if profile.username.is_some() || password.is_some() {
        if let Some(u) = &profile.username {
            url.push_str(&enc(u));
        }
        if let Some(p) = password {
            url.push(':');
            url.push_str(&enc(p));
        }
        url.push('@');
    }
    url.push_str(&profile.host);
    url.push(':');
    url.push_str(&profile.port.to_string());
    url.push('/');
    url.push_str(&profile.db.to_string());
    url
}

async fn connect(
    profile: &RedisProfile,
    password: Option<&str>,
) -> anyhow::Result<MultiplexedConnection> {
    redis::Client::open(url(profile, password))
        .context("opening redis client")?
        .get_multiplexed_async_connection()
        .await
        .context("connecting to redis")
}

/// One-shot: write the full sample keyspace under `prefix`. Returns the number
/// of keys written. Idempotent — each key is replaced.
pub async fn seed(
    profile: &RedisProfile,
    password: Option<&str>,
    prefix: &str,
) -> anyhow::Result<usize> {
    let mut conn = connect(profile, password).await?;
    let set = fixtures::redis_seed_set();
    for key in &set {
        write_seed_key(&mut conn, prefix, key).await?;
    }
    Ok(set.len())
}

async fn write_seed_key(
    conn: &mut MultiplexedConnection,
    prefix: &str,
    key: &SeedKey,
) -> anyhow::Result<()> {
    let k = format!("{prefix}:{}", key.suffix);
    // Replace any pre-existing key so re-seeding is idempotent.
    let _: redis::Value = redis::cmd("DEL").arg(&k).query_async(&mut *conn).await?;
    match &key.value {
        SeedValue::Str(v) => {
            let _: redis::Value = redis::cmd("SET")
                .arg(&k)
                .arg(*v)
                .query_async(&mut *conn)
                .await?;
        }
        SeedValue::StrTtl(v, ttl) => {
            let _: redis::Value = redis::cmd("SET")
                .arg(&k)
                .arg(*v)
                .arg("EX")
                .arg(*ttl)
                .query_async(&mut *conn)
                .await?;
        }
        SeedValue::Counter(n) => {
            let _: redis::Value = redis::cmd("SET")
                .arg(&k)
                .arg(*n)
                .query_async(&mut *conn)
                .await?;
        }
        SeedValue::List(items) => {
            let mut cmd = redis::cmd("RPUSH");
            cmd.arg(&k);
            for it in *items {
                cmd.arg(*it);
            }
            let _: redis::Value = cmd.query_async(&mut *conn).await?;
        }
        SeedValue::Set(items) => {
            let mut cmd = redis::cmd("SADD");
            cmd.arg(&k);
            for it in *items {
                cmd.arg(*it);
            }
            let _: redis::Value = cmd.query_async(&mut *conn).await?;
        }
        SeedValue::Hash(fields) => {
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&k);
            for (f, v) in *fields {
                cmd.arg(*f).arg(*v);
            }
            let _: redis::Value = cmd.query_async(&mut *conn).await?;
        }
        SeedValue::ZSet(members) => {
            let mut cmd = redis::cmd("ZADD");
            cmd.arg(&k);
            for (m, s) in *members {
                cmd.arg(*s).arg(*m);
            }
            let _: redis::Value = cmd.query_async(&mut *conn).await?;
        }
        SeedValue::Stream(entries) => {
            for entry in *entries {
                let mut cmd = redis::cmd("XADD");
                cmd.arg(&k).arg("*");
                for (f, v) in *entry {
                    cmd.arg(*f).arg(*v);
                }
                let _: redis::Value = cmd.query_async(&mut *conn).await?;
            }
        }
    }
    Ok(())
}

/// Continuous: emit live Redis traffic until `token` is cancelled.
pub async fn publish(
    profile: RedisProfile,
    password: Option<String>,
    prefix: String,
    interval: Duration,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let mut conn = connect(&profile, password.as_deref()).await?;
    println!(
        "redis    → {}:{} (pub/sub, stream, keyspace churn) every {interval:?}",
        profile.host, profile.port
    );
    let mut seq: u64 = 0;
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep(interval) => {
                seq += 1;
                publish_once(&mut conn, &prefix, seq).await?;
            }
        }
    }
    Ok(())
}

/// One tick of live Redis traffic: a pub/sub message, a stream entry, a
/// transient key (SET + EXPIRE, with an occasional DEL), and a counter bump —
/// enough to light up every Redis tail kind (`SUBSCRIBE`/`PSUBSCRIBE`,
/// `XREAD`, keyspace notifications, and `MONITOR`).
pub async fn publish_once(
    conn: &mut MultiplexedConnection,
    prefix: &str,
    seq: u64,
) -> anyhow::Result<()> {
    let body = fixtures::order_event_json(seq, "order.created");

    let _: redis::Value = redis::cmd("PUBLISH")
        .arg(fixtures::redis_channel(prefix))
        .arg(&body)
        .query_async(&mut *conn)
        .await?;

    let _: redis::Value = redis::cmd("XADD")
        .arg(fixtures::redis_stream(prefix))
        .arg("*")
        .arg("event")
        .arg("order.created")
        .arg("seq")
        .arg(seq)
        .query_async(&mut *conn)
        .await?;

    let _: redis::Value = redis::cmd("SET")
        .arg(fixtures::redis_live_key(prefix, seq))
        .arg(&body)
        .arg("EX")
        .arg(30)
        .query_async(&mut *conn)
        .await?;
    // Delete an older transient key so the keyspace tail sees `del` events too.
    if seq > 3 {
        let _: redis::Value = redis::cmd("DEL")
            .arg(fixtures::redis_live_key(prefix, seq - 3))
            .query_async(&mut *conn)
            .await?;
    }

    let _: redis::Value = redis::cmd("INCR")
        .arg(fixtures::redis_counter(prefix))
        .query_async(&mut *conn)
        .await?;

    Ok(())
}

#[cfg(test)]
mod url_tests {
    use super::*;

    fn profile() -> RedisProfile {
        RedisProfile {
            name: "dev".into(),
            host: "localhost".into(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            tls: false,
        }
    }

    #[test]
    fn plain_url_has_no_userinfo() {
        assert_eq!(url(&profile(), None), "redis://localhost:6379/0");
    }

    #[test]
    fn url_includes_the_selected_db() {
        let p = RedisProfile { db: 3, ..profile() };
        assert_eq!(url(&p, None), "redis://localhost:6379/3");
    }

    #[test]
    fn url_percent_encodes_credentials() {
        let p = RedisProfile {
            username: Some("u".into()),
            ..profile()
        };
        assert_eq!(
            url(&p, Some("p@ss/word")),
            "redis://u:p%40ss%2Fword@localhost:6379/0"
        );
    }

    #[test]
    fn url_with_username_only() {
        let p = RedisProfile {
            username: Some("admin".into()),
            ..profile()
        };
        assert_eq!(url(&p, None), "redis://admin@localhost:6379/0");
    }

    #[test]
    fn url_with_password_only() {
        assert_eq!(
            url(&profile(), Some("secret")),
            "redis://:secret@localhost:6379/0"
        );
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized Redis: `just test-int`, or
    //! `cargo test --features integration` with Redis reachable on
    //! `127.0.0.1:$KEYHOLE_TEST_REDIS_PORT` (default 6380). Each test uses a
    //! uniquely-namespaced prefix and cleans up, so the suite is parallel-safe.
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_port() -> u16 {
        std::env::var("KEYHOLE_TEST_REDIS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(6380)
    }

    fn test_profile() -> RedisProfile {
        RedisProfile {
            name: "dev-test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            db: 0,
            username: None,
            password: None,
            tls: false,
        }
    }

    fn unique_prefix() -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        format!(
            "keyhole:devtest:{}:{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn redis_type(conn: &mut MultiplexedConnection, key: &str) -> String {
        redis::cmd("TYPE")
            .arg(key)
            .query_async(&mut *conn)
            .await
            .expect("TYPE")
    }

    #[tokio::test]
    async fn seed_writes_one_of_every_container_type() {
        let profile = test_profile();
        let prefix = unique_prefix();
        let written = seed(&profile, None, &prefix).await.expect("seed");
        assert!(written >= 6, "expected the full seed set");

        let mut conn = connect(&profile, None).await.expect("connect");
        let expected = [
            ("string:greeting", "string"),
            ("list:recent_orders", "list"),
            ("set:active_users", "set"),
            ("hash:order:1001", "hash"),
            ("zset:leaderboard", "zset"),
            ("stream:events", "stream"),
        ];
        for (suffix, vtype) in expected {
            let key = format!("{prefix}:{suffix}");
            assert_eq!(
                redis_type(&mut conn, &key).await,
                vtype,
                "wrong type for {key}"
            );
        }

        // Clean up every seeded key under the unique prefix.
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{prefix}:*"))
            .query_async(&mut conn)
            .await
            .expect("KEYS");
        if !keys.is_empty() {
            let mut del = redis::cmd("DEL");
            for k in &keys {
                del.arg(k);
            }
            let _: redis::Value = del.query_async(&mut conn).await.expect("cleanup DEL");
        }
    }

    #[tokio::test]
    async fn publish_once_emits_without_error() {
        let profile = test_profile();
        let prefix = unique_prefix();
        let mut conn = connect(&profile, None).await.expect("connect");
        for seq in 1..=5 {
            publish_once(&mut conn, &prefix, seq)
                .await
                .expect("publish_once");
        }
        // The counter must have advanced once per tick.
        let visits: i64 = redis::cmd("GET")
            .arg(fixtures::redis_counter(&prefix))
            .query_async(&mut conn)
            .await
            .expect("GET counter");
        assert_eq!(visits, 5);

        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{prefix}:*"))
            .query_async(&mut conn)
            .await
            .expect("KEYS");
        if !keys.is_empty() {
            let mut del = redis::cmd("DEL");
            for k in &keys {
                del.arg(k);
            }
            let _: redis::Value = del.query_async(&mut conn).await.expect("cleanup DEL");
        }
    }
}
