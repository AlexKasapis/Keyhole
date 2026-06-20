//! Redis implementation of [`BrokerConnection`].
//!
//! Phase 1 uses a single [`ConnectionManager`] (multiplexed, auto-reconnecting)
//! for browse / inspect / `INFO`, driven serially by the connection actor.
//! `SELECT` is issued before each browse/inspect so the right database is used
//! even across reconnects. Blocking tail connections (pub/sub, streams) get
//! their own dedicated sockets in Phase 2.

mod command;
mod info;
mod tail;
mod value;

use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use redis::aio::ConnectionManager;

use super::{
    BrokerConnection, BrokerEventStream, BrowsePage, BrowseReq, Capabilities, EntryMeta,
    InspectReq, StreamEntry, SubSpec, Ttl, ValueType, ValueView,
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
        Ok(Capabilities::redis(databases))
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
        // Saturating so a pathological offset+limit can't overflow or sign-flip
        // (which LRANGE/ZRANGE would misread as a from-the-end index).
        let stop = req.offset.saturating_add(req.limit).saturating_sub(1) as isize;

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
                let members = scan_collect(&mut conn, "SSCAN", &req.key, req.limit).await?;
                ValueView::Set { len, members }
            }
            ValueType::Hash => {
                let len: usize = redis::cmd("HLEN")
                    .arg(&req.key)
                    .query_async(&mut conn)
                    .await?;
                // HSCAN returns a flat [field, value, …]; collect 2 elements per
                // field, then pair them up.
                let want = req.limit.saturating_mul(2);
                let flat = scan_collect(&mut conn, "HSCAN", &req.key, want).await?;
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

    async fn subscribe(&mut self, spec: SubSpec) -> anyhow::Result<BrokerEventStream> {
        if self.profile.tls {
            anyhow::bail!("TLS connections require building brokertui with --features tls");
        }
        // A fresh client/socket per tail — blocking ops must not share the
        // actor's multiplexed manager.
        let client = redis::Client::open(self.connection_url())?;
        match spec {
            SubSpec::Channel(_) | SubSpec::Pattern(_) => tail::open_pubsub(client, spec).await,
            SubSpec::Stream { key, db } => tail::open_stream(client, key, db).await,
            SubSpec::Keyspace { db } => tail::open_keyspace(client, db).await,
            SubSpec::Monitor => tail::open_monitor(client).await,
            SubSpec::Topic(_) | SubSpec::Queue(_) => {
                anyhow::bail!("topic/queue specs require an AMQP connection")
            }
        }
    }

    /// Flag when a keyspace tail is opened but the server isn't publishing
    /// notifications (`notify-keyspace-events` is empty), so the user knows why
    /// the tail stays silent. brokertui never changes the setting itself — that
    /// is a server-side write, deferred past v1.
    async fn tail_notice(&mut self, spec: &SubSpec) -> Option<String> {
        if !matches!(spec, SubSpec::Keyspace { .. }) {
            return None;
        }
        let mut conn = self.manager().ok()?;
        let reply: redis::RedisResult<Vec<String>> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("notify-keyspace-events")
            .query_async(&mut conn)
            .await;
        let flags = reply
            .ok()
            .and_then(|kv| kv.get(1).cloned())
            .unwrap_or_default();
        if flags.trim().is_empty() {
            Some(
                "keyspace notifications are disabled (notify-keyspace-events is empty); \
                 no events will appear. Enable server-side with e.g. \
                 `CONFIG SET notify-keyspace-events KEA`."
                    .to_string(),
            )
        } else {
            None
        }
    }

    /// Execute a single read-only command. The text is validated against a
    /// deny-by-default allowlist, then double-checked against the server's own
    /// `COMMAND INFO` flags (rejecting anything flagged `write`/`admin`) before
    /// it runs — so the console can never mutate data, upholding the v1
    /// read-only guarantee.
    async fn exec_readonly(&mut self, command: &str) -> anyhow::Result<String> {
        let parts = command::validate_readonly(command)?;
        let mut conn = self.manager()?;
        command::ensure_server_readonly(&mut conn, &parts).await?;

        let mut cmd = redis::cmd(&parts[0]);
        for arg in &parts[1..] {
            cmd.arg(arg);
        }
        let value: redis::Value = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(command::render_reply(&value))
    }
}

/// Run a cursor-based scan (`SSCAN`/`HSCAN`) on `key`, accumulating reply
/// elements until at least `want` are collected or the scan completes, then
/// truncating to `want`.
///
/// A single `SSCAN`/`HSCAN` returns only a `COUNT`-*hint*-sized batch plus a
/// cursor, so one call can yield far fewer elements than a large collection
/// holds — issuing it once (and discarding the cursor) would show an arbitrary
/// partial slice while the header reports the true, larger cardinality. Looping
/// gives the viewer deterministic "first N" paging.
async fn scan_collect(
    conn: &mut ConnectionManager,
    cmd: &str,
    key: &str,
    want: usize,
) -> anyhow::Result<Vec<String>> {
    /// `COUNT` hint per scan iteration — large enough to fill a viewer page in
    /// one or two round-trips without fetching an unbounded amount.
    const SCAN_HINT: usize = 512;

    if want == 0 {
        return Ok(Vec::new());
    }
    let mut cursor = 0u64;
    let mut out: Vec<String> = Vec::new();
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd(cmd)
            .arg(key)
            .arg(cursor)
            .arg("COUNT")
            .arg(SCAN_HINT)
            .query_async(conn)
            .await?;
        out.extend(batch);
        cursor = next;
        // Cursor 0 marks a completed scan; otherwise stop once the page is full.
        if cursor == 0 || out.len() >= want {
            break;
        }
    }
    out.truncate(want);
    Ok(out)
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized Redis: `just test-int`, or
    //! `cargo test --features integration` with Redis reachable on
    //! `127.0.0.1:$BROKERTUI_TEST_REDIS_PORT` (default 6380). Each test seeds its
    //! own uniquely-namespaced keys, so the suite is deterministic and
    //! parallel-safe (no reliance on external seeding or TTLs).
    use super::*;
    use crate::broker::Payload;
    use crate::config::RedisProfile;
    use crate::recording::{RecordSink, Recorder};
    use futures_util::StreamExt;
    use redis::aio::MultiplexedConnection;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use time::OffsetDateTime;
    use tokio::time::timeout;

    /// A unique, namespaced key/channel so concurrent tests never collide.
    fn unique(prefix: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}:{}:{}", std::process::id(), n)
    }

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
    async fn inspect_large_set_and_hash_fill_the_page() {
        let setk = unique("it:big:set");
        let hashk = unique("it:big:hash");
        let mut raw = raw().await;
        // 1000 members/fields — well above the per-scan COUNT hint, so a single
        // SSCAN/HSCAN would return only a partial batch (the bug this guards).
        let mut set_cmd = redis::cmd("SADD");
        set_cmd.arg(&setk);
        for i in 0..1000 {
            set_cmd.arg(format!("m{i}"));
        }
        let _: i64 = set_cmd.query_async(&mut raw).await.unwrap();
        let mut hash_cmd = redis::cmd("HSET");
        hash_cmd.arg(&hashk);
        for i in 0..1000 {
            hash_cmd.arg(format!("f{i}")).arg(format!("v{i}"));
        }
        let _: i64 = hash_cmd.query_async(&mut raw).await.unwrap();

        let mut conn = connected().await;
        let req = |key: &str| InspectReq {
            db: 0,
            key: key.into(),
            offset: 0,
            limit: 200,
        };
        match conn.inspect(req(&setk)).await.unwrap() {
            ValueView::Set { len, members } => {
                assert_eq!(len, 1000, "SCARD reports the true cardinality");
                assert_eq!(members.len(), 200, "the viewer page is filled to the limit");
            }
            other => panic!("expected set, got {other:?}"),
        }
        match conn.inspect(req(&hashk)).await.unwrap() {
            ValueView::Hash { len, fields } => {
                assert_eq!(len, 1000);
                assert_eq!(fields.len(), 200, "200 field/value pairs returned");
            }
            other => panic!("expected hash, got {other:?}"),
        }
        let _: () = redis::cmd("DEL")
            .arg(&setk)
            .arg(&hashk)
            .query_async(&mut raw)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn pubsub_tail_receives_published_message() {
        let channel = unique("it:ps");
        // `subscribe` awaits SUBSCRIBE confirmation before returning, so a publish
        // issued afterwards is guaranteed to be delivered to the tail.
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Channel(channel.clone()))
            .await
            .expect("subscribe");

        let mut pubconn = raw().await;
        let _: redis::Value = redis::cmd("PUBLISH")
            .arg(&channel)
            .arg("hello-pubsub")
            .query_async(&mut pubconn)
            .await
            .unwrap();

        let ev = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("tail timed out")
            .expect("stream ended");
        assert_eq!(ev.source, channel);
        assert_eq!(ev.payload, Payload::Utf8("hello-pubsub".into()));
    }

    #[tokio::test]
    async fn pattern_tail_records_matched_pattern() {
        let base = unique("it.pat"); // dots so a glob pattern matches
        let pattern = format!("{base}.*");
        let channel = format!("{base}.sports");

        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Pattern(pattern.clone()))
            .await
            .expect("psubscribe");

        let mut pubconn = raw().await;
        let _: redis::Value = redis::cmd("PUBLISH")
            .arg(&channel)
            .arg("p")
            .query_async(&mut pubconn)
            .await
            .unwrap();

        let ev = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("tail timed out")
            .expect("stream ended");
        assert_eq!(ev.source, channel);
        assert_eq!(ev.meta("pattern"), Some(pattern.as_str()));
    }

    #[tokio::test]
    async fn stream_tail_receives_new_entries() {
        let key = unique("it:stream");
        // Seed one entry so `$` resolves to a concrete id; the tail must NOT see it.
        let mut seed = raw().await;
        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg("*")
            .arg("seed")
            .arg("0")
            .query_async(&mut seed)
            .await
            .unwrap();

        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Stream {
                key: key.clone(),
                db: 0,
            })
            .await
            .expect("stream subscribe");

        // XADD after the first XREAD (`$`) is in flight; the tail blocks for it.
        // The delay deliberately exceeds the client's 500ms default response
        // timeout, so this also guards against that timeout aborting the block.
        let key2 = key.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(900)).await;
            let mut raw = raw().await;
            let _: String = redis::cmd("XADD")
                .arg(&key2)
                .arg("*")
                .arg("field")
                .arg("value1")
                .query_async(&mut raw)
                .await
                .unwrap();
        });

        let ev = timeout(Duration::from_secs(6), stream.next())
            .await
            .expect("tail timed out")
            .expect("stream ended");
        assert_eq!(ev.source, key);
        assert!(ev.meta("id").is_some(), "stream entry id present");
        // Only the post-subscribe entry, never the seed.
        assert_eq!(ev.payload, Payload::Json(r#"{"field":"value1"}"#.into()));
    }

    #[tokio::test]
    async fn monitor_tail_observes_other_connections_commands() {
        let token = unique("it:mon");
        let mut conn = connected().await;
        let mut stream = conn.subscribe(SubSpec::Monitor).await.expect("monitor");

        // Issue a recognizable command from a different connection.
        let mut other = raw().await;
        let _: redis::Value = redis::cmd("GET")
            .arg(&token)
            .query_async(&mut other)
            .await
            .unwrap();

        // MONITOR is a firehose; scan a few events for our command.
        let mut seen = false;
        for _ in 0..200 {
            match timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(ev)) => {
                    if ev.payload.as_text().contains(&token) {
                        assert_eq!(ev.meta("db"), Some("0"));
                        assert!(ev.meta("client").is_some(), "client addr captured");
                        seen = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(seen, "MONITOR should observe the GET command");
    }

    #[tokio::test]
    async fn keyspace_tail_observes_set_event() {
        // The test server runs with --notify-keyspace-events KEA.
        let key = unique("it:ks");
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Keyspace { db: 0 })
            .await
            .expect("keyspace tail");

        let mut other = raw().await;
        let _: redis::Value = redis::cmd("SET")
            .arg(&key)
            .arg("v")
            .query_async(&mut other)
            .await
            .unwrap();

        // Look for the `set` event carrying our key as the payload.
        let mut found = false;
        for _ in 0..200 {
            match timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(ev)) => {
                    if ev.source == "set" && ev.payload == Payload::Utf8(key.clone()) {
                        assert_eq!(ev.meta("db"), Some("0"));
                        found = true;
                        break;
                    }
                }
                _ => break,
            }
        }
        assert!(found, "keyspace tail should observe the SET event");
    }

    #[tokio::test]
    async fn keyspace_tail_notice_is_silent_when_enabled() {
        // With notifications enabled, opening a keyspace tail flags no advisory.
        let mut conn = connected().await;
        let notice = conn.tail_notice(&SubSpec::Keyspace { db: 0 }).await;
        assert!(notice.is_none(), "no notice when KEA is configured");
        // Non-keyspace specs never carry a notice.
        assert!(conn
            .tail_notice(&SubSpec::Channel("c".into()))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn exec_readonly_runs_reads_and_rejects_writes() {
        let key = unique("it:exec");
        let mut seed = raw().await;
        let _: redis::Value = redis::cmd("SET")
            .arg(&key)
            .arg("hello")
            .query_async(&mut seed)
            .await
            .unwrap();

        let mut conn = connected().await;
        // A read returns the value.
        let out = conn.exec_readonly(&format!("GET {key}")).await.unwrap();
        assert_eq!(out, "hello");
        // CONFIG GET is allowed (subcommand allowlist).
        assert!(conn.exec_readonly("CONFIG GET maxmemory").await.is_ok());
        // Writes and admin commands are refused before hitting the server.
        for bad in ["SET other v", "DEL x", "FLUSHALL", "CONFIG SET maxmemory 0"] {
            assert!(
                conn.exec_readonly(bad).await.is_err(),
                "must refuse `{bad}`"
            );
        }
        // The refused SET must not have created the key.
        let mut check = raw().await;
        let exists: i64 = redis::cmd("EXISTS")
            .arg("other")
            .query_async(&mut check)
            .await
            .unwrap();
        assert_eq!(exists, 0, "the read-only console never wrote `other`");
    }

    #[tokio::test]
    async fn records_tail_to_valid_jsonl() {
        let channel = unique("it:rec");
        let dir = std::env::temp_dir().join(unique("brokertui-it-rec").replace(':', "-"));

        let mut conn = connected().await;
        let spec = SubSpec::Channel(channel.clone());
        let mut stream = conn.subscribe(spec.clone()).await.expect("subscribe");

        let sink = RecordSink::create(&dir, "test", &spec, OffsetDateTime::now_utc())
            .expect("create sink");
        let path = sink.path().to_path_buf();
        let mut recorder = Recorder::new(sink, "test", &spec);

        let mut pubconn = raw().await;
        for i in 0..3 {
            let _: redis::Value = redis::cmd("PUBLISH")
                .arg(&channel)
                .arg(format!("msg-{i}"))
                .query_async(&mut pubconn)
                .await
                .unwrap();
        }
        for _ in 0..3 {
            let ev = timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("tail timed out")
                .expect("stream ended");
            recorder.record(&ev).unwrap();
        }
        recorder.flush().unwrap();
        assert_eq!(recorder.records(), 3);

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        let r0: crate::recording::Record = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(r0.source, channel);
        assert_eq!(r0.source_type, "pubsub");
        assert_eq!(r0.payload, "msg-0");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
