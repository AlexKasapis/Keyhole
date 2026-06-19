//! Redis implementation of [`BrokerConnection`].
//!
//! Phase 1 uses a single [`ConnectionManager`] (multiplexed, auto-reconnecting)
//! for browse / inspect / `INFO`, driven serially by the connection actor.
//! `SELECT` is issued before each browse/inspect so the right database is used
//! even across reconnects. Blocking tail connections (pub/sub, streams) get
//! their own dedicated sockets in Phase 2.

mod info;
mod value;

use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use redis::aio::ConnectionManager;

use super::{
    BrokerConnection, BrowsePage, BrowseReq, Capabilities, EntryMeta, InspectReq, StreamEntry, Ttl,
    ValueType, ValueView,
};
use crate::config::RedisProfile;

/// A live (or not-yet-connected) Redis connection.
pub struct RedisConnection {
    profile: RedisProfile,
    password: Option<String>,
    preview_bytes: usize,
    conn: Option<ConnectionManager>,
}

impl RedisConnection {
    /// Build a connection from a profile and its resolved password. Call
    /// [`BrokerConnection::connect`] to actually establish it.
    pub fn new(profile: RedisProfile, password: Option<String>, preview_bytes: usize) -> Self {
        Self {
            profile,
            password,
            preview_bytes: preview_bytes.max(1),
            conn: None,
        }
    }

    /// Build a `redis://` connection URL with percent-encoded credentials.
    /// (redis 1.2 made `ConnectionInfo` fields private, so a URL is the
    /// supported way to set db + auth programmatically.)
    fn connection_url(&self) -> String {
        let enc = |s: &str| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string();
        let mut url = String::from("redis://");
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
        url.push('/');
        url.push_str(&self.profile.db.to_string());
        url
    }

    /// A cheap clone of the multiplexed manager for issuing a command.
    fn manager(&self) -> anyhow::Result<ConnectionManager> {
        self.conn
            .clone()
            .ok_or_else(|| anyhow::anyhow!("connection is not established"))
    }

    /// Discover the configured number of databases (defaults to 16).
    async fn database_count(&self) -> u32 {
        let Ok(mut conn) = self.manager() else {
            return 16;
        };
        let reply: redis::RedisResult<Vec<String>> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("databases")
            .query_async(&mut conn)
            .await;
        reply
            .ok()
            .and_then(|kv| kv.get(1).and_then(|v| v.parse::<u32>().ok()))
            .unwrap_or(16)
    }
}

#[async_trait]
impl BrokerConnection for RedisConnection {
    async fn connect(&mut self) -> anyhow::Result<Capabilities> {
        if self.profile.tls {
            anyhow::bail!("TLS connections require building brokertui with --features tls");
        }
        let client = redis::Client::open(self.connection_url())?;
        let manager = ConnectionManager::new(client)
            .await
            .map_err(|e| anyhow::anyhow!("connecting to redis: {e}"))?;
        self.conn = Some(manager);

        let databases = self.database_count().await;
        Ok(Capabilities { databases })
    }

    async fn ping(&mut self) -> anyhow::Result<()> {
        let mut conn = self.manager()?;
        let _: redis::Value = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(())
    }

    async fn browse(&mut self, req: BrowseReq) -> anyhow::Result<BrowsePage> {
        let mut conn = self.manager()?;
        let _: redis::Value = redis::cmd("SELECT")
            .arg(req.db)
            .query_async(&mut conn)
            .await?;

        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(req.cursor)
            .arg("MATCH")
            .arg(&req.pattern)
            .arg("COUNT")
            .arg(req.page_size)
            .query_async(&mut conn)
            .await?;

        let mut entries = Vec::with_capacity(keys.len());
        if !keys.is_empty() {
            let mut type_pipe = redis::pipe();
            for key in &keys {
                type_pipe.cmd("TYPE").arg(key);
            }
            let types: Vec<String> = type_pipe.query_async(&mut conn).await?;

            let mut ttl_pipe = redis::pipe();
            for key in &keys {
                ttl_pipe.cmd("TTL").arg(key);
            }
            let ttls: Vec<i64> = ttl_pipe.query_async(&mut conn).await?;

            for (i, key) in keys.into_iter().enumerate() {
                let vtype = types
                    .get(i)
                    .map(|t| ValueType::from_redis(t))
                    .unwrap_or(ValueType::Unknown);
                let ttl = ttls
                    .get(i)
                    .map(|t| Ttl::from_redis(*t))
                    .unwrap_or(Ttl::Unknown);
                entries.push(EntryMeta { key, vtype, ttl });
            }
        }

        Ok(BrowsePage {
            db: req.db,
            entries,
            next_cursor,
        })
    }

    async fn inspect(&mut self, req: InspectReq) -> anyhow::Result<ValueView> {
        let mut conn = self.manager()?;
        let _: redis::Value = redis::cmd("SELECT")
            .arg(req.db)
            .query_async(&mut conn)
            .await?;

        let type_reply: String = redis::cmd("TYPE")
            .arg(&req.key)
            .query_async(&mut conn)
            .await?;
        let start = req.offset as isize;
        let stop = (req.offset + req.limit) as isize - 1;

        let view = match ValueType::from_redis(&type_reply) {
            ValueType::None | ValueType::Unknown => ValueView::Missing,
            ValueType::String => {
                let total: usize = redis::cmd("STRLEN")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                let bytes: Vec<u8> = if total > self.preview_bytes {
                    redis::cmd("GETRANGE")
                        .arg(&req.key)
                        .arg(0)
                        .arg(self.preview_bytes as isize - 1)
                        .query_async(&mut conn)
                        .await?
                } else {
                    redis::cmd("GET")
                        .arg(&req.key)
                        .query_async(&mut conn)
                        .await?
                };
                value::render_string(bytes, total)
            }
            ValueType::List => {
                let len: usize = redis::cmd("LLEN")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                let items: Vec<String> = redis::cmd("LRANGE")
                    .arg(&req.key)
                    .arg(start)
                    .arg(stop)
                    .query_async(&mut conn)
                    .await?;
                ValueView::List {
                    len,
                    offset: req.offset,
                    items,
                }
            }
            ValueType::Set => {
                let len: usize = redis::cmd("SCARD")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                let (_cursor, members): (u64, Vec<String>) = redis::cmd("SSCAN")
                    .arg(&req.key)
                    .arg(0)
                    .arg("COUNT")
                    .arg(req.limit)
                    .query_async(&mut conn)
                    .await?;
                ValueView::Set { len, members }
            }
            ValueType::Hash => {
                let len: usize = redis::cmd("HLEN")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                let (_cursor, flat): (u64, Vec<String>) = redis::cmd("HSCAN")
                    .arg(&req.key)
                    .arg(0)
                    .arg("COUNT")
                    .arg(req.limit)
                    .query_async(&mut conn)
                    .await?;
                let fields = flat
                    .chunks_exact(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                ValueView::Hash { len, fields }
            }
            ValueType::ZSet => {
                let len: usize = redis::cmd("ZCARD")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                let flat: Vec<String> = redis::cmd("ZRANGE")
                    .arg(&req.key)
                    .arg(start)
                    .arg(stop)
                    .arg("WITHSCORES")
                    .query_async(&mut conn)
                    .await?;
                let items = flat
                    .chunks_exact(2)
                    .filter_map(|c| c[1].parse::<f64>().ok().map(|score| (c[0].clone(), score)))
                    .collect();
                ValueView::ZSet { len, items }
            }
            ValueType::Stream => {
                let len: usize = redis::cmd("XLEN")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                let raw: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
                    .arg(&req.key)
                    .arg("-")
                    .arg("+")
                    .arg("COUNT")
                    .arg(req.limit)
                    .query_async(&mut conn)
                    .await?;
                let entries: Vec<StreamEntry> = raw
                    .into_iter()
                    .map(|(id, flat)| StreamEntry {
                        id,
                        fields: flat
                            .chunks_exact(2)
                            .map(|c| (c[0].clone(), c[1].clone()))
                            .collect(),
                    })
                    .collect();
                let last_id = entries.last().map(|e| e.id.clone()).unwrap_or_default();
                ValueView::Stream {
                    len,
                    last_id,
                    entries,
                }
            }
        };
        Ok(view)
    }

    async fn stats(&mut self) -> anyhow::Result<super::ServerStats> {
        let mut conn = self.manager()?;
        let text: String = redis::cmd("INFO").query_async(&mut conn).await?;
        Ok(info::parse_info(&text))
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized Redis: `just test-int`, or
    //! `cargo test --features integration` with Redis reachable on
    //! `127.0.0.1:$BROKERTUI_TEST_REDIS_PORT` (default 6380). Each test seeds its
    //! own uniquely-namespaced keys, so the suite is deterministic and
    //! parallel-safe (no reliance on external seeding or TTLs).
    use super::*;
    use crate::config::RedisProfile;
    use redis::aio::MultiplexedConnection;

    fn test_port() -> u16 {
        std::env::var("BROKERTUI_TEST_REDIS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(6380)
    }

    fn test_profile() -> RedisProfile {
        RedisProfile {
            name: "test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            db: 0,
            username: None,
            password: None,
            tls: false,
        }
    }

    async fn connected() -> RedisConnection {
        let mut conn = RedisConnection::new(test_profile(), None, 64 * 1024);
        let caps = conn.connect().await.expect("connect to test redis");
        assert!(caps.databases >= 1);
        conn
    }

    /// A raw connection for seeding (bypasses the read-only browse layer).
    async fn raw() -> MultiplexedConnection {
        redis::Client::open(format!("redis://127.0.0.1:{}/0", test_port()))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .expect("raw seed connection")
    }

    #[tokio::test]
    async fn browse_scans_namespace_with_match() {
        let mut raw = raw().await;
        for key in ["it:browse:a", "it:browse:b", "it:browse:c"] {
            let _: redis::Value = redis::cmd("SET")
                .arg(key)
                .arg("v")
                .query_async(&mut raw)
                .await
                .unwrap();
        }

        let mut conn = connected().await;
        let mut cursor = 0u64;
        let mut found: Vec<(String, ValueType)> = Vec::new();
        loop {
            let page = conn
                .browse(BrowseReq {
                    db: 0,
                    pattern: "it:browse:*".into(),
                    cursor,
                    page_size: 100,
                })
                .await
                .expect("browse");
            found.extend(page.entries.into_iter().map(|e| (e.key, e.vtype)));
            cursor = page.next_cursor;
            if cursor == 0 {
                break;
            }
        }

        for key in ["it:browse:a", "it:browse:b", "it:browse:c"] {
            assert!(found.iter().any(|(k, _)| k == key), "MATCH missed {key}");
        }
        // MATCH must not leak keys outside the namespace, and type metadata is set.
        assert!(found.iter().all(|(k, _)| k.starts_with("it:browse:")));
        assert!(found.iter().all(|(_, t)| *t == ValueType::String));
    }

    #[tokio::test]
    async fn inspects_each_type() {
        let mut raw = raw().await;
        let _: () = redis::pipe()
            .cmd("DEL")
            .arg("it:ins:list")
            .ignore()
            .cmd("RPUSH")
            .arg("it:ins:list")
            .arg("a")
            .arg("b")
            .arg("c")
            .arg("d")
            .arg("e")
            .ignore()
            .cmd("DEL")
            .arg("it:ins:hash")
            .ignore()
            .cmd("HSET")
            .arg("it:ins:hash")
            .arg("name")
            .arg("Alice")
            .arg("age")
            .arg("30")
            .arg("city")
            .arg("Athens")
            .ignore()
            .cmd("DEL")
            .arg("it:ins:zset")
            .ignore()
            .cmd("ZADD")
            .arg("it:ins:zset")
            .arg(100)
            .arg("alice")
            .arg(95)
            .arg("bob")
            .arg(87)
            .arg("carol")
            .ignore()
            .cmd("DEL")
            .arg("it:ins:stream")
            .ignore()
            .cmd("SET")
            .arg("it:ins:json")
            .arg(r#"{"a":1,"b":[2,3]}"#)
            .ignore()
            .cmd("SET")
            .arg("it:ins:bin")
            .arg(&b"\x00\x01\xff"[..])
            .ignore()
            .query_async(&mut raw)
            .await
            .unwrap();
        let _: String = redis::cmd("XADD")
            .arg("it:ins:stream")
            .arg("*")
            .arg("k")
            .arg("v1")
            .query_async(&mut raw)
            .await
            .unwrap();
        let _: String = redis::cmd("XADD")
            .arg("it:ins:stream")
            .arg("*")
            .arg("k")
            .arg("v2")
            .query_async(&mut raw)
            .await
            .unwrap();

        let mut conn = connected().await;
        let req = |key: &str| InspectReq {
            db: 0,
            key: key.into(),
            offset: 0,
            limit: 100,
        };

        match conn.inspect(req("it:ins:list")).await.unwrap() {
            ValueView::List { len, items, .. } => {
                assert_eq!(len, 5);
                assert_eq!(items, vec!["a", "b", "c", "d", "e"]);
            }
            other => panic!("expected list, got {other:?}"),
        }
        match conn.inspect(req("it:ins:hash")).await.unwrap() {
            ValueView::Hash { len, fields, .. } => {
                assert_eq!(len, 3);
                assert!(fields.iter().any(|(k, v)| k == "name" && v == "Alice"));
            }
            other => panic!("expected hash, got {other:?}"),
        }
        match conn.inspect(req("it:ins:zset")).await.unwrap() {
            ValueView::ZSet { len, items, .. } => {
                assert_eq!(len, 3);
                assert!(items
                    .iter()
                    .any(|(m, s)| m == "alice" && (*s - 100.0).abs() < 1e-9));
            }
            other => panic!("expected zset, got {other:?}"),
        }
        match conn.inspect(req("it:ins:stream")).await.unwrap() {
            ValueView::Stream { len, entries, .. } => {
                assert_eq!(len, 2);
                assert_eq!(entries.len(), 2);
            }
            other => panic!("expected stream, got {other:?}"),
        }
        match conn.inspect(req("it:ins:json")).await.unwrap() {
            ValueView::Str { encoding, .. } => {
                assert_eq!(encoding, crate::broker::PayloadEncoding::Json);
            }
            other => panic!("expected json string, got {other:?}"),
        }
        match conn.inspect(req("it:ins:bin")).await.unwrap() {
            ValueView::Str { encoding, .. } => {
                assert_eq!(encoding, crate::broker::PayloadEncoding::Base64);
            }
            other => panic!("expected base64 string, got {other:?}"),
        }
        // A missing key inspects as Missing, not an error.
        assert!(matches!(
            conn.inspect(req("it:ins:absent")).await.unwrap(),
            ValueView::Missing
        ));
    }

    #[tokio::test]
    async fn stats_reports_version_and_keyspace() {
        let mut raw = raw().await;
        let _: redis::Value = redis::cmd("SET")
            .arg("it:stats:k")
            .arg("v")
            .query_async(&mut raw)
            .await
            .unwrap();

        let mut conn = connected().await;
        let stats = conn.stats().await.unwrap();
        assert!(stats.redis_version.is_some());
        assert!(stats.uptime_seconds.is_some());
        assert!(!stats.db_keys.is_empty());
    }
}
