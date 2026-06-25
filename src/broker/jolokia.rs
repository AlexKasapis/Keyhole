//! ActiveMQ "Classic" destination discovery via the Jolokia (JMX-over-HTTP)
//! management API.
//!
//! AMQP 1.0 cannot enumerate destinations, so the browser's list is normally
//! user-curated. When an `amqp` profile points at the broker's web console
//! (default `:8161`), keyhole queries Jolokia to enumerate the broker's topics
//! and queues and merge them into the browser — the same enrichment the console
//! itself shows. This is a wholly separate channel from the AMQP 1.0 wire used
//! for tailing/peeking.
//!
//! Two quirks of ActiveMQ's Jolokia agent shape the request:
//! - It guards cross-origin requests; a request with no `Origin` is rejected
//!   (`"Origin null is not allowed to call this agent"`). We send
//!   `Origin: <base_url>` — the console's own origin, which the default
//!   `jolokia-access.xml` allows.
//! - It answers with HTTP 200 even for its own errors, carrying the real outcome
//!   in each response's `status` field, so we inspect that rather than the HTTP
//!   status code.
//!
//! The HTTP call is blocking (`ureq`); callers drive it from a blocking context
//! (`tokio::task::spawn_blocking`) so it never stalls the async runtime.

use std::collections::HashSet;
use std::time::Duration;

use base64::Engine as _;
use serde_json::{json, Value};

use super::SubSpec;

/// How long to wait to connect to / read from the management API before giving
/// up. Discovery is best-effort enrichment, so it fails fast rather than hanging
/// the browser when the console is unreachable.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Internal ActiveMQ topics (broker advisories) are noise in a destination
/// browser, so they are filtered out of the discovered set.
const ADVISORY_PREFIX: &str = "ActiveMQ.Advisory.";

/// Enumerate the broker's topics and queues via Jolokia. `base_url` is the web
/// console root (e.g. `http://127.0.0.1:8161`); `username`/`password` are the
/// console's HTTP Basic credentials (ActiveMQ's default is `admin`/`admin`).
///
/// Returns the destinations as [`SubSpec`]s (topics first, then queues, each
/// de-duplicated), with broker advisory topics filtered out. Blocking.
pub fn discover(
    base_url: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> anyhow::Result<Vec<SubSpec>> {
    let origin = base_url.trim_end_matches('/').to_string();
    let url = format!("{origin}/api/jolokia/");
    // One batched request (an array) enumerates both destination types in a
    // single round trip; Jolokia preserves request order in the reply.
    let body = json!([search_request("Topic"), search_request("Queue")]).to_string();

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(CONNECT_TIMEOUT)
        .timeout_read(READ_TIMEOUT)
        .build();
    let mut req = agent
        .post(&url)
        .set("Origin", &origin)
        .set("Content-Type", "application/json");
    if let Some(user) = username {
        let token = base64::engine::general_purpose::STANDARD
            .encode(format!("{user}:{}", password.unwrap_or("")));
        req = req.set("Authorization", &format!("Basic {token}"));
    }

    let text = match req.send_string(&body) {
        Ok(resp) => resp.into_string()?,
        Err(e) => return Err(http_error(e)),
    };
    let value: Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("management API returned a non-JSON response: {e}"))?;
    parse_search_batch(&value)
}

/// A Jolokia `search` request matching every topic or queue MBean across all
/// brokers (`brokerName=*` avoids hardcoding the broker name).
fn search_request(destination_type: &str) -> Value {
    json!({
        "type": "search",
        "mbean": format!(
            "org.apache.activemq:type=Broker,brokerName=*,destinationType={destination_type},destinationName=*"
        ),
    })
}

/// Turn a `ureq` error into a user-facing message. Jolokia's own errors come
/// back as HTTP 200 (handled in [`parse_search_batch`]); this covers the
/// transport-level failures: an unreachable console, or Jetty's BASIC-auth
/// rejection (HTTP 401) when the credentials are missing or wrong.
fn http_error(e: ureq::Error) -> anyhow::Error {
    match e {
        ureq::Error::Status(401, _) => anyhow::anyhow!(
            "the management API rejected the credentials (HTTP 401) — set \
             management_username/management_password"
        ),
        ureq::Error::Status(code, resp) => {
            let body = resp.into_string().unwrap_or_default();
            let body = body.trim();
            if body.is_empty() {
                anyhow::anyhow!("the management API returned HTTP {code}")
            } else {
                anyhow::anyhow!("the management API returned HTTP {code}: {body}")
            }
        }
        ureq::Error::Transport(t) => {
            anyhow::anyhow!("could not reach the management API: {t}")
        }
    }
}

/// Parse a Jolokia batch reply (an array of per-request result objects) into the
/// discovered destinations. Each result is classified topic/queue from its
/// request's MBean, its `value` (an array of JMX ObjectName strings) mined for
/// `destinationName`s, advisories dropped, and the whole set de-duplicated while
/// preserving order.
fn parse_search_batch(value: &Value) -> anyhow::Result<Vec<SubSpec>> {
    let entries = value.as_array().ok_or_else(|| {
        anyhow::anyhow!("expected a Jolokia batch reply (a JSON array), got something else")
    })?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for entry in entries {
        let status = entry.get("status").and_then(Value::as_u64).unwrap_or(0);
        if status != 200 {
            let msg = entry
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown Jolokia error");
            anyhow::bail!("management API error (status {status}): {msg}");
        }
        // Classify from the echoed request so a reordered reply is still correct.
        let mbean = entry
            .pointer("/request/mbean")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_topic = mbean.contains("destinationType=Topic");
        let Some(names) = entry.get("value").and_then(Value::as_array) else {
            continue;
        };
        for object_name in names.iter().filter_map(Value::as_str) {
            let Some(name) = destination_name(object_name) else {
                continue;
            };
            if name.starts_with(ADVISORY_PREFIX) {
                continue;
            }
            let spec = if is_topic {
                SubSpec::Topic(name.to_string())
            } else {
                SubSpec::Queue(name.to_string())
            };
            // De-dupe on the canonical `topic:`/`queue:` label.
            if seen.insert(spec.label()) {
                out.push(spec);
            }
        }
    }
    Ok(out)
}

/// Extract the `destinationName=...` value from an ActiveMQ JMX ObjectName like
/// `org.apache.activemq:brokerName=localhost,destinationName=foo,destinationType=Queue,type=Broker`.
/// ObjectName values are comma-separated `key=value` pairs; a value containing a
/// reserved character is quoted, so surrounding quotes are stripped.
fn destination_name(object_name: &str) -> Option<&str> {
    const KEY: &str = "destinationName=";
    let start = object_name.find(KEY)? + KEY.len();
    let rest = &object_name[start..];
    let raw = if let Some(stripped) = rest.strip_prefix('"') {
        // Quoted: take up to the closing quote.
        &stripped[..stripped.find('"').unwrap_or(stripped.len())]
    } else {
        // Unquoted: take up to the next pair separator.
        &rest[..rest.find(',').unwrap_or(rest.len())]
    };
    (!raw.is_empty()).then_some(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reply mirroring a real ActiveMQ Classic Jolokia batch search: a topic
    /// result (including broker advisories that must be filtered) and a queue
    /// result, both with `status: 200`.
    fn ok_batch() -> Value {
        json!([
            {
                "request": {
                    "type": "search",
                    "mbean": "org.apache.activemq:type=Broker,brokerName=*,destinationType=Topic,destinationName=*"
                },
                "value": [
                    "org.apache.activemq:brokerName=localhost,destinationName=ActiveMQ.Advisory.MasterBroker,destinationType=Topic,type=Broker",
                    "org.apache.activemq:brokerName=localhost,destinationName=keyhole.demo.events,destinationType=Topic,type=Broker",
                    "org.apache.activemq:brokerName=localhost,destinationName=ActiveMQ.Advisory.Connection,destinationType=Topic,type=Broker"
                ],
                "status": 200
            },
            {
                "request": {
                    "type": "search",
                    "mbean": "org.apache.activemq:type=Broker,brokerName=*,destinationType=Queue,destinationName=*"
                },
                "value": [
                    "org.apache.activemq:brokerName=localhost,destinationName=keyhole.demo.orders,destinationType=Queue,type=Broker",
                    "org.apache.activemq:brokerName=localhost,destinationName=ActiveMQ.DLQ,destinationType=Queue,type=Broker"
                ],
                "status": 200
            }
        ])
    }

    #[test]
    fn parses_topics_and_queues_filtering_advisories() {
        let specs = parse_search_batch(&ok_batch()).expect("parses the batch");
        assert_eq!(
            specs,
            vec![
                SubSpec::Topic("keyhole.demo.events".into()),
                SubSpec::Queue("keyhole.demo.orders".into()),
                // A real queue named with the ActiveMQ prefix but not an advisory
                // is kept; only `ActiveMQ.Advisory.*` is dropped.
                SubSpec::Queue("ActiveMQ.DLQ".into()),
            ],
            "advisory topics are filtered; real topics and queues are kept in order"
        );
    }

    #[test]
    fn de_dupes_repeated_destinations() {
        // The same destination across multiple brokers appears once.
        let batch = json!([{
            "request": { "mbean": "...destinationType=Topic,destinationName=*" },
            "value": [
                "org.apache.activemq:brokerName=a,destinationName=shared,destinationType=Topic,type=Broker",
                "org.apache.activemq:brokerName=b,destinationName=shared,destinationType=Topic,type=Broker"
            ],
            "status": 200
        }]);
        let specs = parse_search_batch(&batch).expect("parses");
        assert_eq!(specs, vec![SubSpec::Topic("shared".into())]);
    }

    #[test]
    fn surfaces_a_jolokia_error_status() {
        // The CORS/origin guard rejects with status 403 inside an HTTP 200 reply.
        let batch = json!([{
            "error_type": "java.lang.Exception",
            "error": "java.lang.Exception : Origin null is not allowed to call this agent",
            "status": 403
        }]);
        let err = parse_search_batch(&batch).expect_err("a non-200 status is an error");
        let msg = format!("{err}");
        assert!(msg.contains("403"), "message names the status: {msg}");
        assert!(
            msg.contains("Origin null"),
            "message carries the cause: {msg}"
        );
    }

    #[test]
    fn rejects_a_non_array_reply() {
        let err = parse_search_batch(&json!({"status": 200})).expect_err("not a batch array");
        assert!(format!("{err}").contains("JSON array"));
    }

    #[test]
    fn destination_name_extracts_unquoted_and_quoted() {
        assert_eq!(
            destination_name(
                "org.apache.activemq:brokerName=localhost,destinationName=foo.bar,destinationType=Queue,type=Broker"
            ),
            Some("foo.bar")
        );
        // Quoted value (would contain a reserved char in practice).
        assert_eq!(
            destination_name("org.apache.activemq:destinationName=\"a,b\",destinationType=Topic"),
            Some("a,b")
        );
        // No destinationName segment.
        assert_eq!(destination_name("org.apache.activemq:type=Broker"), None);
        // Empty value.
        assert_eq!(destination_name("destinationName=,type=Broker"), None);
    }

    #[test]
    fn search_request_targets_all_brokers() {
        let req = search_request("Topic");
        let mbean = req["mbean"].as_str().unwrap();
        assert!(mbean.contains("brokerName=*"));
        assert!(mbean.contains("destinationType=Topic"));
    }
}
