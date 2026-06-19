//! Redis live tails on dedicated sockets.
//!
//! Blocking tail operations can't share the actor's multiplexed
//! [`ConnectionManager`](redis::aio::ConnectionManager), so each tail opens its
//! own connection:
//! - **Pub/Sub** via [`redis::aio::PubSub`] + `SUBSCRIBE`/`PSUBSCRIBE`. The
//!   returned `PubSubStream` owns the backing task, so the subscription stays
//!   alive for as long as the stream is held.
//! - **Streams** via a `XREAD BLOCK <ms> COUNT <n> STREAMS <key> <id>` loop that
//!   advances `last_id`, starting from `$` (new entries only).
//!
//! Phase 3 adds two more dedicated tails:
//! - **Keyspace notifications** via `PSUBSCRIBE __keyevent@<db>__:*` — the event
//!   name is the channel suffix and the affected key is the message body.
//! - **MONITOR** via [`redis::aio::Monitor`] — a server-wide firehose of every
//!   processed command, one text line per command.
//!
//! Both map replies to [`BrokerEvent`]s. The byte→[`Payload`] decisions and the
//! line parsing live in pure helpers ([`channel_event`]/[`stream_event`]/
//! [`keyspace_event`]/[`monitor_event`]) so they can be unit tested without a
//! broker.

use futures_util::{stream, StreamExt};
use redis::aio::MultiplexedConnection;
use time::OffsetDateTime;

use crate::broker::{BrokerEvent, BrokerEventStream, Payload, SubSpec};

/// How long each `XREAD` blocks before looping (keeps cancellation responsive
/// and survives any connection-level read timeout).
const XREAD_BLOCK_MS: u64 = 5_000;
/// Max stream entries fetched per `XREAD`.
const XREAD_COUNT: usize = 100;

/// One `XREAD` reply: `[(stream_key, [(entry_id, [field, value, …])])]`, or
/// `None` on a `BLOCK` timeout.
type XReadReply = Option<Vec<(String, Vec<(String, Vec<String>)>)>>;

/// Build a [`BrokerEvent`] from a pub/sub message's parts.
fn channel_event(channel: String, pattern: Option<String>, bytes: Vec<u8>) -> BrokerEvent {
    let meta = match pattern {
        Some(p) => vec![("pattern".to_string(), p)],
        None => Vec::new(),
    };
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: channel,
        payload: Payload::classify(bytes),
        meta,
    }
}

/// Build a [`BrokerEvent`] from one stream entry. Field/value pairs become a
/// JSON object payload (order preserved); the entry id goes in `meta`.
fn stream_event(key: &str, id: String, fields: Vec<(String, String)>) -> BrokerEvent {
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: key.to_string(),
        payload: Payload::Json(fields_to_json(&fields)),
        meta: vec![("id".to_string(), id)],
    }
}

/// Serialize ordered field/value pairs into a JSON object string.
///
/// `serde_json::Map` would sort the keys, so the object is assembled by hand
/// (each key and value escaped via `serde_json`) to preserve stream field order.
/// Stream field values must be UTF-8 (the `XREAD` reply is decoded as strings),
/// matching the inspect path's behaviour.
fn fields_to_json(fields: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(k).unwrap_or_else(|_| "\"?\"".to_string()));
        out.push(':');
        out.push_str(&serde_json::to_string(v).unwrap_or_else(|_| "\"?\"".to_string()));
    }
    out.push('}');
    out
}

/// Build a [`BrokerEvent`] from a keyspace-notification message.
///
/// We subscribe to `__keyevent@<db>__:*`, so the channel suffix is the event
/// name (`set`, `expired`, `lpush`, …) and the message body is the affected
/// key. The event name becomes the `source` (so the tab reads like a log of
/// operations); the key is the binary-safe payload.
fn keyspace_event(channel: String, key: Vec<u8>) -> BrokerEvent {
    let (db, event) = parse_keyevent_channel(&channel);
    let mut meta = Vec::new();
    if let Some(db) = db {
        meta.push(("db".to_string(), db));
    }
    meta.push(("channel".to_string(), channel.clone()));
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: event.unwrap_or(channel),
        payload: Payload::classify(key),
        meta,
    }
}

/// Pull the db index and event name out of a `__keyevent@<db>__:<event>`
/// channel. Returns `(None, None)` for anything that doesn't match the shape.
fn parse_keyevent_channel(channel: &str) -> (Option<String>, Option<String>) {
    let Some(body) = channel.strip_prefix("__keyevent@") else {
        return (None, None);
    };
    // body == `<db>__:<event>`
    match body.split_once("__:") {
        Some((db, event)) if !event.is_empty() => (Some(db.to_string()), Some(event.to_string())),
        _ => (None, None),
    }
}

/// Build a [`BrokerEvent`] from a single `MONITOR` line.
///
/// A line looks like `<unix_ts> [<db> <client_addr>] "CMD" "arg" …`. The db and
/// client come from the bracketed section; the quoted command text (always
/// valid UTF-8 — Redis escapes non-printable bytes) is the payload. The client
/// address is the `source` so tabs/logs group by caller.
fn monitor_event(line: String) -> BrokerEvent {
    let (db, client, command) = parse_monitor_line(&line);
    let mut meta = Vec::new();
    if let Some(db) = db {
        meta.push(("db".to_string(), db));
    }
    if let Some(client) = &client {
        meta.push(("client".to_string(), client.clone()));
    }
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: client.unwrap_or_else(|| "monitor".to_string()),
        payload: Payload::Utf8(command),
        meta,
    }
}

/// Split a `MONITOR` line into `(db, client, command_text)`. Falls back to the
/// whole trimmed line as the command if the bracketed prefix is absent.
fn parse_monitor_line(line: &str) -> (Option<String>, Option<String>, String) {
    if let (Some(lb), Some(rb)) = (line.find('['), line.find(']')) {
        if lb < rb {
            let inside = &line[lb + 1..rb];
            let (db, client) = match inside.split_once(' ') {
                Some((db, client)) => (Some(db.to_string()), Some(client.trim().to_string())),
                None => (Some(inside.to_string()), None),
            };
            return (db, client, line[rb + 1..].trim().to_string());
        }
    }
    (None, None, line.trim().to_string())
}

/// Open a pub/sub tail (`SUBSCRIBE`/`PSUBSCRIBE`) and return its event stream.
pub async fn open_pubsub(
    client: redis::Client,
    spec: SubSpec,
) -> anyhow::Result<BrokerEventStream> {
    let mut pubsub = client.get_async_pubsub().await?;
    match &spec {
        SubSpec::Channel(c) => pubsub.subscribe(c).await?,
        SubSpec::Pattern(p) => pubsub.psubscribe(p).await?,
        other => anyhow::bail!("{} is not a pub/sub spec", other.label()),
    }
    let stream = pubsub.into_on_message().map(|msg| {
        let channel = msg.get_channel_name().to_string();
        let pattern = if msg.from_pattern() {
            msg.get_pattern::<String>().ok()
        } else {
            None
        };
        channel_event(channel, pattern, msg.get_payload_bytes().to_vec())
    });
    Ok(Box::pin(stream))
}

/// Per-step state for the stream tail loop.
struct StreamState {
    conn: MultiplexedConnection,
    key: String,
    last_id: String,
}

/// Open a stream tail (`XREAD BLOCK … $`) on database `db` and return its event
/// stream. Entries are emitted in order; the loop ends if the read errors (e.g.
/// the connection drops).
pub async fn open_stream(
    client: redis::Client,
    key: String,
    db: u32,
) -> anyhow::Result<BrokerEventStream> {
    // Disable the per-request response timeout (default 500ms): `XREAD BLOCK`
    // legitimately holds the connection idle for seconds while waiting for new
    // entries, which would otherwise be reported as a timeout error.
    let config = redis::AsyncConnectionConfig::new().set_response_timeout(None);
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&config)
        .await?;
    // The tail's db may differ from the profile db baked into the URL.
    let _: redis::Value = redis::cmd("SELECT").arg(db).query_async(&mut conn).await?;

    let state = StreamState {
        conn,
        key,
        last_id: "$".to_string(),
    };
    let stream = stream::unfold(state, |mut st| async move {
        loop {
            let reply: redis::RedisResult<XReadReply> = redis::cmd("XREAD")
                .arg("BLOCK")
                .arg(XREAD_BLOCK_MS)
                .arg("COUNT")
                .arg(XREAD_COUNT)
                .arg("STREAMS")
                .arg(&st.key)
                .arg(&st.last_id)
                .query_async(&mut st.conn)
                .await;
            match reply {
                Ok(Some(keys)) => {
                    let mut events = Vec::new();
                    for (_stream_key, entries) in keys {
                        for (id, flat) in entries {
                            st.last_id = id.clone();
                            let fields = flat
                                .chunks_exact(2)
                                .map(|c| (c[0].clone(), c[1].clone()))
                                .collect();
                            events.push(stream_event(&st.key, id, fields));
                        }
                    }
                    if events.is_empty() {
                        continue; // nothing usable; keep blocking
                    }
                    return Some((stream::iter(events), st));
                }
                Ok(None) => continue, // BLOCK timeout — re-issue
                Err(e) => {
                    tracing::debug!(error = %e, key = %st.key, "stream tail ended");
                    return None;
                }
            }
        }
    })
    .flatten();
    Ok(Box::pin(stream))
}

/// Open a keyspace-notification tail for database `db`
/// (`PSUBSCRIBE __keyevent@<db>__:*`) and return its event stream. The server's
/// `notify-keyspace-events` must be enabled for events to arrive; brokertui does
/// not change that setting (see the advisory in `RedisConnection::tail_notice`).
pub async fn open_keyspace(client: redis::Client, db: u32) -> anyhow::Result<BrokerEventStream> {
    let mut pubsub = client.get_async_pubsub().await?;
    let pattern = format!("__keyevent@{db}__:*");
    pubsub.psubscribe(&pattern).await?;
    let stream = pubsub.into_on_message().map(|msg| {
        let channel = msg.get_channel_name().to_string();
        keyspace_event(channel, msg.get_payload_bytes().to_vec())
    });
    Ok(Box::pin(stream))
}

/// Open a `MONITOR` tail and return its event stream: one [`BrokerEvent`] per
/// command the server processes (server-wide). High-rate by nature — the UI
/// forward stays lossy and the recorder lossless, as for every other tail.
pub async fn open_monitor(client: redis::Client) -> anyhow::Result<BrokerEventStream> {
    let monitor = client.get_async_monitor().await?;
    let stream = monitor.into_on_message::<String>().map(monitor_event);
    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_event_plain_text() {
        let ev = channel_event("news".into(), None, b"hello".to_vec());
        assert_eq!(ev.source, "news");
        assert_eq!(ev.payload, Payload::Utf8("hello".into()));
        assert!(ev.meta.is_empty());
    }

    #[test]
    fn channel_event_json_payload() {
        let ev = channel_event("news".into(), None, br#"{"a":1}"#.to_vec());
        assert_eq!(ev.payload, Payload::Json(r#"{"a":1}"#.into()));
    }

    #[test]
    fn channel_event_pattern_recorded_in_meta() {
        let ev = channel_event("news.sports".into(), Some("news.*".into()), b"x".to_vec());
        assert_eq!(ev.meta("pattern"), Some("news.*"));
        assert_eq!(ev.source, "news.sports");
    }

    #[test]
    fn channel_event_binary_payload() {
        let ev = channel_event("c".into(), None, vec![0x00, 0xff]);
        assert_eq!(ev.payload, Payload::Binary(vec![0x00, 0xff]));
    }

    #[test]
    fn stream_event_builds_json_object_and_id() {
        let ev = stream_event(
            "orders",
            "1-0".into(),
            vec![("k".into(), "v".into()), ("n".into(), "2".into())],
        );
        assert_eq!(ev.source, "orders");
        assert_eq!(ev.meta("id"), Some("1-0"));
        // Order preserved, valid JSON.
        assert_eq!(ev.payload, Payload::Json(r#"{"k":"v","n":"2"}"#.into()));
    }

    #[test]
    fn fields_to_json_escapes_and_handles_empty() {
        assert_eq!(fields_to_json(&[]), "{}");
        assert_eq!(
            fields_to_json(&[("a\"b".into(), "c\nd".into())]),
            r#"{"a\"b":"c\nd"}"#
        );
    }

    #[test]
    fn parses_keyevent_channels() {
        assert_eq!(
            parse_keyevent_channel("__keyevent@0__:set"),
            (Some("0".into()), Some("set".into()))
        );
        assert_eq!(
            parse_keyevent_channel("__keyevent@15__:expired"),
            (Some("15".into()), Some("expired".into()))
        );
        // Unexpected shapes degrade gracefully.
        assert_eq!(parse_keyevent_channel("__keyspace@0__:mykey"), (None, None));
        assert_eq!(parse_keyevent_channel("random"), (None, None));
        assert_eq!(parse_keyevent_channel("__keyevent@0__:"), (None, None));
    }

    #[test]
    fn keyspace_event_uses_event_as_source_and_key_as_payload() {
        let ev = keyspace_event("__keyevent@0__:set".into(), b"user:42".to_vec());
        assert_eq!(ev.source, "set", "the event name is the source");
        assert_eq!(ev.payload, Payload::Utf8("user:42".into()));
        assert_eq!(ev.meta("db"), Some("0"));
        assert_eq!(ev.meta("channel"), Some("__keyevent@0__:set"));
    }

    #[test]
    fn keyspace_event_binary_key_is_base64_safe() {
        let ev = keyspace_event("__keyevent@0__:del".into(), vec![0x00, 0xff]);
        assert_eq!(ev.source, "del");
        assert_eq!(ev.payload, Payload::Binary(vec![0x00, 0xff]));
    }

    #[test]
    fn keyspace_event_falls_back_to_raw_channel() {
        let ev = keyspace_event("weird-channel".into(), b"k".to_vec());
        assert_eq!(ev.source, "weird-channel");
        assert_eq!(ev.meta("db"), None);
    }

    #[test]
    fn parses_monitor_lines() {
        let (db, client, command) =
            parse_monitor_line(r#"1700000000.123456 [0 127.0.0.1:12345] "SET" "k" "v""#);
        assert_eq!(db.as_deref(), Some("0"));
        assert_eq!(client.as_deref(), Some("127.0.0.1:12345"));
        assert_eq!(command, r#""SET" "k" "v""#);
    }

    #[test]
    fn monitor_event_builds_from_line() {
        let ev = monitor_event(r#"1700000000.5 [3 10.0.0.1:5555] "GET" "key""#.into());
        assert_eq!(ev.source, "10.0.0.1:5555");
        assert_eq!(ev.payload, Payload::Utf8(r#""GET" "key""#.into()));
        assert_eq!(ev.meta("db"), Some("3"));
        assert_eq!(ev.meta("client"), Some("10.0.0.1:5555"));
    }

    #[test]
    fn monitor_line_without_brackets_degrades() {
        let (db, client, command) = parse_monitor_line("OK");
        assert_eq!(db, None);
        assert_eq!(client, None);
        assert_eq!(command, "OK");
        // And the event still builds, sourced as "monitor".
        let ev = monitor_event("OK".into());
        assert_eq!(ev.source, "monitor");
    }
}
