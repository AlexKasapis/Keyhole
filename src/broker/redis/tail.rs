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
//! Both map replies to [`BrokerEvent`]s. The byte→[`Payload`] decisions live in
//! the pure helpers [`channel_event`]/[`stream_event`] so they can be unit
//! tested without a broker.

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

/// Open a pub/sub tail (`SUBSCRIBE`/`PSUBSCRIBE`) and return its event stream.
pub async fn open_pubsub(
    client: redis::Client,
    spec: SubSpec,
) -> anyhow::Result<BrokerEventStream> {
    let mut pubsub = client.get_async_pubsub().await?;
    match &spec {
        SubSpec::Channel(c) => pubsub.subscribe(c).await?,
        SubSpec::Pattern(p) => pubsub.psubscribe(p).await?,
        SubSpec::Stream { .. } => anyhow::bail!("stream specs are tailed via open_stream"),
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
}
