//! RabbitMQ's HTTP management API (the `rabbitmq_management` plugin).
//!
//! AMQP 0.9.1 can't list queues or exchanges and has no stats plane, so those
//! come from the management API instead: [`MgmtClient`] is a small blocking
//! client for one vhost, and [`Management`] pairs it with what the connect-time
//! probe learned (the user's tags and the broker version) or why the API is
//! unusable.
//!
//! Wire facts this module relies on, checked against RabbitMQ 3.13 and 4.4 (the
//! replies in `testdata/` are real):
//! - ureq sends paths verbatim and encodes nothing, so the vhost and names are
//!   percent-encoded here as single segments. A raw `#` would silently cut the
//!   path.
//! - `columns=` trims a reply, and a key the broker doesn't have is omitted, not
//!   `null`. With the 4.x metrics collector off, message counts disappear along
//!   with rates. Every stats field is therefore optional.
//! - A paged list comes back in an envelope, and asking past `page_count` is a
//!   400.
//! - Every auth failure is a 401 (never a 403), told apart by its `reason`.

use anyhow::Context as _;
use base64::Engine as _;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::de::{DeserializeOwned, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::broker::{http, QueueType, DESTINATION_LIST_CAP};

/// The most queue or exchange rows one listing collects. A vhost with more
/// reports the listing as truncated.
pub const LIST_CAP: usize = DESTINATION_LIST_CAP;

/// Rows per page: the server's maximum (`page_size=501` is a 400).
const PAGE_SIZE: usize = 500;

/// The columns each endpoint is asked for. Unfiltered rows carry far more than
/// keyhole shows (a node is ~12 KB).
const QUEUE_COLUMNS: &str = "name,type,state,durable,auto_delete,exclusive,arguments,policy,\
    effective_policy_definition,delivery_limit,messages,messages_ready,messages_unacknowledged,\
    consumers,message_stats.publish_details.rate,message_stats.deliver_get_details.rate,\
    message_stats.ack_details.rate,message_stats.redeliver_details.rate";
const EXCHANGE_COLUMNS: &str = "name,type,durable,auto_delete,internal,arguments,policy,\
    message_stats.publish_in_details.rate,message_stats.publish_out_details.rate";
const NODE_COLUMNS: &str = "name,running,uptime,mem_used,mem_limit,mem_alarm,disk_free,\
    disk_free_limit,disk_free_alarm,fd_used,fd_total,partitions";
const CONNECTION_COLUMNS: &str = "name,user_provided_name,user,peer_host,peer_port,channels,\
    state,connected_at,protocol,recv_oct_details,send_oct_details";
const CONSUMER_COLUMNS: &str =
    "consumer_tag,queue,channel_details,ack_required,prefetch_count,active,activity_status,exclusive";

/// Percent-encode `s` as a single path segment (`/` → `%2F`, `#` → `%23`).
fn segment(s: &str) -> String {
    utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
}

/// Whether a request names the vhost, which decides what a 404 means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    Cluster,
    Vhost,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A blocking client for one vhost of the management API. Each method makes
/// its HTTP requests on the calling thread, so drive it from
/// `tokio::task::spawn_blocking`. Clones share one connection pool.
#[derive(Clone)]
pub struct MgmtClient {
    base_url: String,
    vhost: String,
    username: String,
    password: String,
    agent: ureq::Agent,
}

impl MgmtClient {
    /// A client for `vhost` at `base_url` (e.g. `http://127.0.0.1:15672`),
    /// logging in with HTTP Basic credentials. Opens nothing yet.
    pub fn new(base_url: String, vhost: String, username: String, password: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            vhost,
            username,
            password,
            agent: http::agent(),
        }
    }

    /// The API's base URL, without a trailing slash.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Check that the API answers and accepts the credentials, and learn what
    /// the connection needs up front: the user's tags (`/api/whoami`), the
    /// broker version and whether its metrics are off (`/api/overview`).
    pub fn probe(&self) -> anyhow::Result<Probe> {
        let me = self.whoami()?;
        let overview = self.probe_overview()?;
        Ok(Probe {
            user: me.name,
            tags: me.tags,
            version: overview.rabbitmq_version,
            stats_disabled: overview.disable_stats,
        })
    }

    /// `GET /api/whoami`: the authenticated user and its tags.
    pub fn whoami(&self) -> anyhow::Result<Whoami> {
        self.get_json("/api/whoami", Scope::Cluster)
    }

    /// The probe's two `/api/overview` fields, `rabbitmq_version` and
    /// `disable_stats`, trimmed to those columns. The metrics setting needs a
    /// node restart to change, so the probe's answer holds for the connection.
    fn probe_overview(&self) -> anyhow::Result<Overview> {
        self.get_json(
            "/api/overview?columns=rabbitmq_version,disable_stats",
            Scope::Cluster,
        )
    }

    /// `GET /api/overview`: cluster-wide versions, totals and rates.
    pub fn overview(&self) -> anyhow::Result<Overview> {
        self.get_json("/api/overview", Scope::Cluster)
    }

    /// `GET /api/nodes`, trimmed to the health figures. Needs the `monitoring`
    /// (or `administrator`) tag; see [`Probe::can_monitor`].
    pub fn nodes(&self) -> anyhow::Result<Vec<NodeInfo>> {
        self.get_json(
            &format!("/api/nodes?columns={NODE_COLUMNS}"),
            Scope::Cluster,
        )
    }

    /// The vhost's queues, sorted by name, up to [`LIST_CAP`] rows.
    pub fn queues(&self) -> anyhow::Result<Listing<QueueInfo>> {
        self.list("queues", QUEUE_COLUMNS)
    }

    /// The vhost's exchanges (the default `""` included), sorted by name, up
    /// to [`LIST_CAP`] rows.
    pub fn exchanges(&self) -> anyhow::Result<Listing<ExchangeInfo>> {
        self.list("exchanges", EXCHANGE_COLUMNS)
    }

    /// Collect `/api/{kind}/{vhost}` page by page: pages `1..=page_count` and
    /// never past it (that's a 400), stopping at [`LIST_CAP`] rows.
    fn list<T: DeserializeOwned>(&self, kind: &str, columns: &str) -> anyhow::Result<Listing<T>> {
        let vhost = segment(&self.vhost);
        collect_pages(LIST_CAP, |page| {
            self.get_json(
                &format!("/api/{kind}/{vhost}?columns={columns}&page={page}&page_size={PAGE_SIZE}"),
                Scope::Vhost,
            )
        })
    }

    /// `POST /api/queues/{vhost}/{queue}/get`: fetch up to `count` messages,
    /// settling each per `ackmode`. Bodies come back base64-encoded so binary
    /// payloads survive; `truncate` caps each body's bytes.
    pub fn get(
        &self,
        queue: &str,
        count: usize,
        ackmode: AckMode,
        truncate: Option<usize>,
    ) -> anyhow::Result<Vec<GetMessage>> {
        let path = format!(
            "/api/queues/{}/{}/get",
            segment(&self.vhost),
            segment(queue)
        );
        let mut body = json!({ "count": count, "ackmode": ackmode, "encoding": "base64" });
        if let Some(limit) = truncate {
            body["truncate"] = json!(limit);
        }
        let sent = self
            .agent
            .post(self.url(&path))
            .header("Authorization", self.auth())
            .header("Content-Type", "application/json")
            // RabbitMQ 3.13 closes the connection after a 404 here without
            // saying so, and the next request on the pooled socket fails with
            // "Peer disconnected". Asking for the close avoids reusing it.
            .header("Connection", "close")
            .send(body.to_string().as_str());
        self.reply(sent, Scope::Vhost)
            .and_then(|text| parse(&text))
            .with_context(|| format!("reading queue `{queue}`"))
    }

    /// `GET /api/vhosts/{vhost}/connections`. A user without `monitoring` sees
    /// only its own connections.
    pub fn connections(&self) -> anyhow::Result<Vec<ConnectionInfo>> {
        self.get_json(
            &format!(
                "/api/vhosts/{}/connections?columns={CONNECTION_COLUMNS}",
                segment(&self.vhost)
            ),
            Scope::Vhost,
        )
    }

    /// `GET /api/consumers/{vhost}`. A user without `monitoring` sees only the
    /// consumers on its own channels.
    pub fn consumers(&self) -> anyhow::Result<Vec<ConsumerInfo>> {
        self.get_json(
            &format!(
                "/api/consumers/{}?columns={CONSUMER_COLUMNS}",
                segment(&self.vhost)
            ),
            Scope::Vhost,
        )
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn auth(&self) -> String {
        http::basic_auth(&self.username, Some(&self.password))
    }

    fn get_json<T: DeserializeOwned>(&self, path: &str, scope: Scope) -> anyhow::Result<T> {
        let sent = self
            .agent
            .get(self.url(path))
            .header("Authorization", self.auth())
            .call();
        parse(&self.reply(sent, scope)?)
    }

    /// A 2xx reply's body, or the reply as an [`ApiError`].
    fn reply(
        &self,
        sent: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
        scope: Scope,
    ) -> anyhow::Result<String> {
        let (status, text) = http::read_reply(sent)?;
        if (200..300).contains(&status) {
            Ok(text)
        } else {
            Err(self.api_error(status, &text, scope).into())
        }
    }

    /// Explain a non-2xx reply, naming the fix where there is one. RabbitMQ
    /// sends `{"error": …, "reason": …}`, and `reason` tells the auth failures
    /// apart: they are all 401s, though a 403 is treated the same way.
    fn api_error(&self, status: u16, body: &str, scope: Scope) -> ApiError {
        let reason =
            serde_json::from_str::<Value>(body)
                .ok()
                .and_then(|v| match v.get("reason")? {
                    Value::String(s) => Some(s.clone()),
                    Value::Null => None,
                    other => Some(other.to_string()),
                });
        let user = &self.username;
        let vhost = &self.vhost;
        let message = match (status, reason.as_deref()) {
            (401 | 403, Some("Not_Authorized")) => format!(
                "the management API rejected the credentials for user `{user}` (HTTP {status}) \
                 — set management_username/management_password"
            ),
            (401 | 403, Some("Not management user")) => format!(
                "user `{user}` lacks the `management` tag the management API requires \
                 (HTTP {status}) — add the tag, or set management_username/management_password \
                 to a user that has it"
            ),
            (401 | 403, Some("Not monitor user")) => format!(
                "user `{user}` lacks the `monitoring` tag (HTTP {status}) — add `monitoring` or \
                 `administrator` to see node stats"
            ),
            (401 | 403, Some("Not administrator user")) => {
                format!("user `{user}` lacks the `administrator` tag (HTTP {status})")
            }
            (401 | 403, Some(r)) if r.starts_with("ACCESS_REFUSED") => {
                format!("permission denied (HTTP {status}): {r}")
            }
            // 3.x; 4.x answers the same case with a 404 (below).
            (401 | 403, Some(r)) if r.contains("virtual host") => format!(
                "user `{user}` has no permissions on vhost `{vhost}` (HTTP {status}) — grant \
                 some, or set management_username/management_password"
            ),
            (401 | 403, Some(r)) => {
                format!("the management API refused the request (HTTP {status}): {r}")
            }
            (404, Some("queue_not_found")) => "queue not found (HTTP 404)".to_string(),
            (404, _) if scope == Scope::Vhost => format!(
                "vhost `{vhost}` doesn't exist, or user `{user}` has no permissions on it \
                 (HTTP 404)"
            ),
            (404, _) => format!(
                "no management API at {} (HTTP 404) — check management_url",
                self.base_url
            ),
            (_, Some(r)) => format!("the management API returned HTTP {status}: {r}"),
            (_, None) => http::status_error(status, body).to_string(),
        };
        ApiError {
            status,
            reason,
            message,
        }
    }
}

/// Parse a 2xx reply body.
fn parse<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    serde_json::from_str(text)
        .map_err(|e| anyhow::anyhow!("unexpected management API response: {e}"))
}

/// A non-2xx reply from the management API. `Display` is the user-facing
/// message; `status` and `reason` let a caller react to one specific refusal
/// (a stream queue's `/get`, or stats disabled on the node).
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct ApiError {
    pub status: u16,
    /// The body's `reason`, when the body was RabbitMQ's JSON error.
    pub reason: Option<String>,
    message: String,
}

/// How `/get` settles each message it fetches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Put the message back (flagged `redelivered`): a non-destructive read.
    AckRequeueTrue,
    /// Remove the message: a destructive read.
    AckRequeueFalse,
}

/// The rows of a listing, and whether [`LIST_CAP`] cut it short.
#[derive(Debug, Clone)]
pub struct Listing<T> {
    pub items: Vec<T>,
    pub truncated: bool,
}

/// One page of a paged list reply. The envelope also carries counts and the
/// page size, which collection doesn't need.
#[derive(Debug, Deserialize)]
struct Page<T> {
    items: Vec<T>,
    page_count: u32,
}

/// Fetch pages `1..=page_count` (`page_count` read from each reply; an empty
/// vhost reports 0) until `cap` rows are in, never asking past the last page.
fn collect_pages<T>(
    cap: usize,
    mut fetch: impl FnMut(u32) -> anyhow::Result<Page<T>>,
) -> anyhow::Result<Listing<T>> {
    let mut items = Vec::new();
    let mut truncated = false;
    let mut page = 1;
    loop {
        let reply = fetch(page)?;
        // An empty page also ends it, so a confused `page_count` can't spin.
        let last = page >= reply.page_count || reply.items.is_empty();
        items.extend(reply.items);
        if items.len() >= cap {
            truncated = items.len() > cap || !last;
            items.truncate(cap);
            break;
        }
        if last {
            break;
        }
        page += 1;
    }
    Ok(Listing { items, truncated })
}

// ---------------------------------------------------------------------------
// Probe state
// ---------------------------------------------------------------------------

/// What a successful probe learned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// The user the API authenticated.
    pub user: String,
    /// The user's tags (`management`, `monitoring`, `administrator`, …).
    pub tags: Vec<String>,
    /// The broker's `rabbitmq_version`, e.g. `3.13.7`. Peek semantics differ
    /// between 3.x and 4.x.
    pub version: Option<String>,
    /// Whether the node's metrics collector is off (`disable_stats`), so
    /// listings carry no counts or rates.
    pub stats_disabled: bool,
}

impl Probe {
    /// Whether the user can read monitoring data: `/api/nodes` (a 401
    /// otherwise), and every user's connections and consumers rather than only
    /// its own. `administrator` includes `monitoring`.
    pub fn can_monitor(&self) -> bool {
        self.tags
            .iter()
            .any(|t| t == "monitoring" || t == "administrator")
    }

    /// The broker's major version (`3` for `3.13.7`).
    pub fn major_version(&self) -> Option<u32> {
        self.version.as_deref()?.split('.').next()?.parse().ok()
    }
}

/// The management API as one connection sees it: the client and the outcome of
/// the last probe. A failed probe is kept as the connection's `mgmt_error`, and
/// [`Self::client`] returns that error instead of the client, so a dead
/// endpoint costs one probe rather than a timeout per call on the connection's
/// serial actor.
pub struct Management {
    client: MgmtClient,
    probe: Result<Probe, String>,
}

impl Management {
    /// Wrap `client`; nothing is usable until [`Self::run_probe`] succeeds.
    pub fn new(client: MgmtClient) -> Self {
        Self {
            client,
            probe: Err("the management API has not been probed yet".to_string()),
        }
    }

    /// The API's base URL, for messages about it.
    pub fn base_url(&self) -> &str {
        self.client.base_url()
    }

    /// What the last probe learned, if it succeeded.
    pub fn probe(&self) -> Option<&Probe> {
        self.probe.as_ref().ok()
    }

    /// Why the API is unusable (the `mgmt_error`), if the last probe failed.
    pub fn error(&self) -> Option<&str> {
        self.probe.as_ref().err().map(String::as_str)
    }

    /// The client, or the last probe's error without touching the network.
    pub fn client(&self) -> anyhow::Result<&MgmtClient> {
        match &self.probe {
            Ok(_) => Ok(&self.client),
            Err(e) => Err(anyhow::Error::msg(e.clone())),
        }
    }

    /// Probe the API (again). Success records the tags and version and clears
    /// an earlier error; failure records the error. Runs at connect, and again
    /// when the user refreshes after a failure.
    pub async fn run_probe(&mut self) {
        let client = self.client.clone();
        let outcome = tokio::task::spawn_blocking(move || client.probe())
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("the management probe task failed: {e}")));
        self.record(outcome);
    }

    /// Mark the API unusable after a call that never got a reply (unreachable,
    /// timed out, or not the API at all), so later calls fail fast with this
    /// error instead of waiting out the timeouts again. The next
    /// [`Self::run_probe`] clears it.
    pub fn mark_unreachable(&mut self, error: &anyhow::Error) {
        self.record(Err(anyhow::anyhow!("{error:#}")));
    }

    fn record(&mut self, outcome: anyhow::Result<Probe>) {
        match &outcome {
            Ok(p) => tracing::info!(
                url = %self.base_url(),
                user = %p.user,
                tags = ?p.tags,
                version = ?p.version,
                "rabbitmq management API probe succeeded"
            ),
            Err(e) => tracing::warn!(
                url = %self.base_url(),
                error = %format!("{e:#}"),
                "rabbitmq management API unavailable"
            ),
        }
        self.probe = outcome.map_err(|e| format!("{e:#}"));
    }
}

// ---------------------------------------------------------------------------
// Reply types
// ---------------------------------------------------------------------------

/// `/api/whoami`.
#[derive(Debug, Clone, Deserialize)]
pub struct Whoami {
    pub name: String,
    #[serde(default, deserialize_with = "tag_list")]
    pub tags: Vec<String>,
}

/// `/api/overview`. A `management`-only user gets no `node`, and per-user
/// `object_totals` without `consumers`; with metrics off there are no
/// `message_stats` or `queue_totals`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Overview {
    pub rabbitmq_version: Option<String>,
    pub erlang_version: Option<String>,
    pub cluster_name: Option<String>,
    pub node: Option<String>,
    /// True when the node's metrics collector is off: counts and rates are
    /// missing everywhere.
    #[serde(default)]
    pub disable_stats: bool,
    #[serde(default)]
    pub message_stats: MessageStats,
    #[serde(default)]
    pub queue_totals: QueueTotals,
    #[serde(default)]
    pub object_totals: ObjectTotals,
}

/// Message rates, per second. Floats, and they can be negative.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MessageStats {
    #[serde(default, rename = "publish_details", deserialize_with = "rate")]
    pub publish_rate: Option<f64>,
    #[serde(default, rename = "deliver_get_details", deserialize_with = "rate")]
    pub deliver_get_rate: Option<f64>,
    #[serde(default, rename = "ack_details", deserialize_with = "rate")]
    pub ack_rate: Option<f64>,
    #[serde(default, rename = "redeliver_details", deserialize_with = "rate")]
    pub redeliver_rate: Option<f64>,
    /// Exchanges only: messages published into it.
    #[serde(default, rename = "publish_in_details", deserialize_with = "rate")]
    pub publish_in_rate: Option<f64>,
    /// Exchanges only: messages it routed onward.
    #[serde(default, rename = "publish_out_details", deserialize_with = "rate")]
    pub publish_out_rate: Option<f64>,
}

/// `overview.queue_totals`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueueTotals {
    pub messages: Option<u64>,
    pub messages_ready: Option<u64>,
    pub messages_unacknowledged: Option<u64>,
}

/// `overview.object_totals`. Cluster-wide only for `monitoring` users.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ObjectTotals {
    pub queues: Option<u64>,
    pub exchanges: Option<u64>,
    pub connections: Option<u64>,
    pub channels: Option<u64>,
    pub consumers: Option<u64>,
}

/// One `/api/nodes` row. With metrics off only `name` and `running` remain.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub running: Option<bool>,
    /// Milliseconds.
    pub uptime: Option<u64>,
    pub mem_used: Option<u64>,
    pub mem_limit: Option<u64>,
    pub mem_alarm: Option<bool>,
    pub disk_free: Option<u64>,
    pub disk_free_limit: Option<u64>,
    pub disk_free_alarm: Option<bool>,
    pub fd_used: Option<u64>,
    pub fd_total: Option<u64>,
    /// Nodes this one is partitioned from.
    #[serde(default)]
    pub partitions: Vec<String>,
}

/// One queue row. Counts and rates are missing when metrics are off.
#[derive(Debug, Clone, Deserialize)]
pub struct QueueInfo {
    pub name: String,
    #[serde(default, rename = "type")]
    pub queue_type: QueueType,
    pub state: Option<String>,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub arguments: Map<String, Value>,
    pub policy: Option<String>,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub effective_policy_definition: Map<String, Value>,
    /// 4.x only; 3.x has it in `arguments` or the policy, if anywhere.
    pub delivery_limit: Option<i64>,
    pub messages: Option<u64>,
    pub messages_ready: Option<u64>,
    pub messages_unacknowledged: Option<u64>,
    pub consumers: Option<u64>,
    #[serde(default)]
    pub message_stats: MessageStats,
}

/// One exchange row. The default exchange is named `""`.
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeInfo {
    pub name: String,
    /// `direct`, `fanout`, `topic`, `headers`, or a plugin type (`x-…`).
    #[serde(rename = "type")]
    pub exchange_type: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    /// Internal exchanges refuse client publishes.
    #[serde(default)]
    pub internal: bool,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub arguments: Map<String, Value>,
    pub policy: Option<String>,
    #[serde(default)]
    pub message_stats: MessageStats,
}

/// One message fetched by `/get`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetMessage {
    /// The body, encoded per `payload_encoding`.
    pub payload: String,
    /// `base64` (what keyhole asks for) or `string`.
    #[serde(default)]
    pub payload_encoding: String,
    /// The full body size; `payload` holds less when `/get` truncated it.
    pub payload_bytes: Option<u64>,
    #[serde(default)]
    pub redelivered: bool,
    #[serde(default)]
    pub exchange: String,
    #[serde(default)]
    pub routing_key: String,
    /// Messages left in the queue after this one.
    pub message_count: Option<u64>,
    /// `[]` rather than an object when the message has none.
    #[serde(default, deserialize_with = "object_or_empty")]
    pub properties: MessageProperties,
}

impl GetMessage {
    /// The body bytes, decoded from `payload`.
    pub fn body(&self) -> anyhow::Result<Vec<u8>> {
        if self.payload_encoding == "base64" {
            base64::engine::general_purpose::STANDARD
                .decode(&self.payload)
                .context("decoding a base64 message body")
        } else {
            Ok(self.payload.clone().into_bytes())
        }
    }
}

/// A fetched message's AMQP properties.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MessageProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    /// Seconds since the epoch.
    pub timestamp: Option<u64>,
    #[serde(rename = "type")]
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    /// Arbitrary JSON values: numbers, bools, arrays, nested objects (`x-death`).
    #[serde(default, deserialize_with = "object_or_empty")]
    pub headers: Map<String, Value>,
}

/// One `/api/vhosts/{vhost}/connections` row. `[]` when metrics are off.
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionInfo {
    /// `peer -> broker` addresses.
    pub name: String,
    /// The client's own connection name, when it set one.
    pub user_provided_name: Option<String>,
    pub user: Option<String>,
    pub peer_host: Option<String>,
    #[serde(default, deserialize_with = "lenient")]
    pub peer_port: Option<u16>,
    pub channels: Option<u64>,
    pub state: Option<String>,
    /// Milliseconds since the epoch.
    pub connected_at: Option<u64>,
    pub protocol: Option<String>,
    /// Bytes per second.
    #[serde(default, rename = "recv_oct_details", deserialize_with = "rate")]
    pub recv_rate: Option<f64>,
    /// Bytes per second.
    #[serde(default, rename = "send_oct_details", deserialize_with = "rate")]
    pub send_rate: Option<f64>,
}

/// One `/api/consumers/{vhost}` row.
#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerInfo {
    pub consumer_tag: String,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub queue: QueueRef,
    #[serde(default, deserialize_with = "object_or_empty")]
    pub channel_details: ChannelDetails,
    /// False for auto-ack (`no_ack`) consumers.
    pub ack_required: Option<bool>,
    pub prefetch_count: Option<u64>,
    pub active: Option<bool>,
    pub activity_status: Option<String>,
    pub exclusive: Option<bool>,
}

/// The queue a consumer reads.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueueRef {
    #[serde(default)]
    pub name: String,
}

/// The channel a consumer is on.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChannelDetails {
    /// `peer -> broker (n)`.
    pub name: Option<String>,
    pub number: Option<u64>,
    pub connection_name: Option<String>,
    pub user: Option<String>,
}

// ---------------------------------------------------------------------------
// Lenient field parsers
// ---------------------------------------------------------------------------

/// A `{"rate": x}` details object as its rate.
fn rate<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    #[derive(Deserialize)]
    struct Details {
        rate: Option<f64>,
    }
    Ok(Option::<Details>::deserialize(d)?.and_then(|d| d.rate))
}

/// An object field that the broker renders as `[]` (or `null`) when it's empty
/// — an Erlang empty proplist.
fn object_or_empty<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned + Default,
{
    match Value::deserialize(d)? {
        Value::Null => Ok(T::default()),
        Value::Array(a) if a.is_empty() => Ok(T::default()),
        v => T::deserialize(v).map_err(serde::de::Error::custom),
    }
}

/// A value some connection kinds fill with a non-number (`"unknown"`), read as
/// `None` when it isn't a `T`.
fn lenient<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    Ok(Option::<Value>::deserialize(d)?.and_then(|v| serde_json::from_value(v).ok()))
}

/// User tags: a JSON array, or a comma-separated string on RabbitMQ before 3.9.
fn tag_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Tags {
        List(Vec<String>),
        Csv(String),
    }
    Ok(match Option::<Tags>::deserialize(d)? {
        Some(Tags::List(tags)) => tags,
        Some(Tags::Csv(s)) => s
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .collect(),
        None => Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::http::stub::{header, stub_server};

    /// A Phase 0 fixture from `testdata/` (real broker replies, unedited).
    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!("testdata/", $name, ".json"))
        };
    }

    fn client(base_url: &str, vhost: &str) -> MgmtClient {
        MgmtClient::new(
            base_url.to_string(),
            vhost.to_string(),
            "keyhole".to_string(),
            "s3cret".to_string(),
        )
    }

    fn ok(body: &str) -> (u16, String) {
        (200, body.to_string())
    }

    /// `METHOD target` from a raw request.
    fn request_line(raw: &str) -> &str {
        raw.lines()
            .next()
            .and_then(|l| l.rsplit_once(' '))
            .map_or("", |(target, _)| target)
    }

    fn page<T: DeserializeOwned>(json: &str) -> Page<T> {
        serde_json::from_str(json).expect("paged envelope parses")
    }

    fn queue<'a>(page: &'a Page<QueueInfo>, name: &str) -> &'a QueueInfo {
        page.items
            .iter()
            .find(|q| q.name == name)
            .unwrap_or_else(|| panic!("queue {name} in fixture"))
    }

    // -- parsing -------------------------------------------------------------

    #[test]
    fn whoami_parses_the_tags_on_both_versions() {
        for (json, tag, monitors) in [
            (fixture!("whoami.administrator.v3"), "administrator", true),
            (fixture!("whoami.administrator.v4"), "administrator", true),
            (fixture!("whoami.monitoring.v3"), "monitoring", true),
            (fixture!("whoami.monitoring.v4"), "monitoring", true),
            (fixture!("whoami.management.v3"), "management", false),
            (fixture!("whoami.management.v4"), "management", false),
        ] {
            let me: Whoami = serde_json::from_str(json).expect("whoami parses");
            assert_eq!(me.tags, vec![tag.to_string()]);
            let probe = Probe {
                user: me.name,
                tags: me.tags,
                version: None,
                stats_disabled: false,
            };
            assert_eq!(probe.can_monitor(), monitors, "{tag}");
        }
    }

    #[test]
    fn whoami_accepts_the_older_comma_separated_tags() {
        let me: Whoami =
            serde_json::from_str(r#"{"name":"u","tags":"administrator, monitoring"}"#).unwrap();
        assert_eq!(me.tags, vec!["administrator", "monitoring"]);
        let none: Whoami = serde_json::from_str(r#"{"name":"u","tags":""}"#).unwrap();
        assert!(none.tags.is_empty());
    }

    #[test]
    fn probe_reads_the_major_version() {
        let probe = |v: Option<&str>| Probe {
            user: "u".into(),
            tags: Vec::new(),
            version: v.map(str::to_string),
            stats_disabled: false,
        };
        assert_eq!(probe(Some("3.13.7")).major_version(), Some(3));
        assert_eq!(probe(Some("4.4.0")).major_version(), Some(4));
        assert_eq!(probe(Some("garbage")).major_version(), None);
        assert_eq!(probe(None).major_version(), None);
    }

    #[test]
    fn overview_parses_versions_totals_and_rates_on_both_versions() {
        let v3: Overview = serde_json::from_str(fixture!("overview.v3")).unwrap();
        assert_eq!(v3.rabbitmq_version.as_deref(), Some("3.13.7"));
        assert_eq!(v3.erlang_version.as_deref(), Some("26.2.5.16"));
        assert_eq!(v3.cluster_name.as_deref(), Some("rabbit@19942ffdc832"));
        assert!(v3.node.is_some() && !v3.disable_stats);
        assert_eq!(v3.message_stats.publish_rate, Some(9.6));
        assert_eq!(v3.message_stats.deliver_get_rate, Some(14.4));
        assert_eq!(v3.message_stats.ack_rate, Some(4.8));
        assert_eq!(v3.queue_totals.messages, Some(1998));
        assert_eq!(v3.queue_totals.messages_unacknowledged, Some(21));
        assert_eq!(v3.object_totals.queues, Some(18));
        assert_eq!(v3.object_totals.exchanges, Some(19));
        assert_eq!(v3.object_totals.channels, Some(6));

        let v4: Overview = serde_json::from_str(fixture!("overview.v4")).unwrap();
        assert_eq!(v4.rabbitmq_version.as_deref(), Some("4.4.0"));
        assert_eq!(v4.message_stats.publish_rate, Some(9.8));
        assert_eq!(v4.queue_totals.messages_ready, Some(1893));
        assert_eq!(v4.object_totals.consumers, Some(4));
    }

    #[test]
    fn overview_for_a_management_user_has_no_node_or_consumer_total() {
        for json in [
            fixture!("overview.management.v3"),
            fixture!("overview.management.v4"),
        ] {
            let ov: Overview = serde_json::from_str(json).unwrap();
            assert!(ov.rabbitmq_version.is_some());
            assert_eq!(ov.node, None);
            assert_eq!(ov.object_totals.consumers, None);
            assert!(ov.object_totals.connections.is_some());
        }
    }

    #[test]
    fn overview_with_metrics_off_leaves_stats_none() {
        let ov: Overview = serde_json::from_str(fixture!("overview.no-metrics.v4")).unwrap();
        assert!(ov.disable_stats);
        assert_eq!(ov.message_stats.publish_rate, None);
        assert_eq!(ov.queue_totals.messages, None);
        assert_eq!(ov.object_totals.consumers, None);
        assert_eq!(ov.object_totals.queues, Some(17));
    }

    #[test]
    fn nodes_parse_the_health_figures_on_both_versions() {
        for json in [fixture!("nodes.v3"), fixture!("nodes.v4")] {
            let nodes: Vec<NodeInfo> = serde_json::from_str(json).unwrap();
            let node = &nodes[0];
            assert!(node.name.starts_with("rabbit@"));
            assert_eq!(node.running, Some(true));
            assert!(node.mem_used.is_some() && node.mem_limit.is_some());
            assert!(node.disk_free.is_some() && node.disk_free_limit.is_some());
            assert!(node.fd_used.is_some() && node.fd_total.is_some());
            assert_eq!(node.mem_alarm, Some(false));
            assert_eq!(node.disk_free_alarm, Some(false));
            assert!(node.partitions.is_empty());
            assert!(node.uptime.is_some());
        }
        let nodes: Vec<NodeInfo> = serde_json::from_str(fixture!("nodes.no-metrics.v4")).unwrap();
        assert_eq!(nodes[0].running, Some(true));
        assert_eq!(nodes[0].mem_used, None);
        assert_eq!(nodes[0].fd_used, None);
        assert_eq!(nodes[0].disk_free, None);
    }

    #[test]
    fn queues_parse_kinds_counts_and_rates_on_both_versions() {
        for (json, consumed_rate) in [(fixture!("queues.v3"), 9.6), (fixture!("queues.v4"), 9.8)] {
            let page: Page<QueueInfo> = page(json);
            assert_eq!(page.page_count, 1);
            assert_eq!(queue(&page, "spike.classic").queue_type, QueueType::Classic);
            assert_eq!(queue(&page, "spike.quorum").queue_type, QueueType::Quorum);
            assert_eq!(queue(&page, "spike.stream").queue_type, QueueType::Stream);
            // Names that need percent-encoding in a path arrive intact.
            for name in ["spike#hash", "spike/slash", "spike space", "spike.café ✓"] {
                queue(&page, name);
            }
            let consumed = queue(&page, "spike.consumed");
            assert_eq!(consumed.consumers, Some(2));
            assert_eq!(consumed.messages_unacknowledged, Some(20));
            assert_eq!(consumed.message_stats.publish_rate, Some(consumed_rate));
            assert_eq!(consumed.message_stats.redeliver_rate, Some(0.0));
            assert!(consumed.durable && !consumed.exclusive && !consumed.auto_delete);
            assert_eq!(consumed.state.as_deref(), Some("running"));
            let limited = queue(&page, "spike.quorum.limit");
            assert_eq!(limited.arguments.get("x-delivery-limit"), Some(&json!(3)));
            let policy = queue(&page, "spike.quorum.policy");
            assert_eq!(policy.policy.as_deref(), Some("spike-dl"));
            assert_eq!(
                policy.effective_policy_definition.get("delivery-limit"),
                Some(&json!(5))
            );
        }
        // Only 4.x reports `delivery_limit`, and it tags every queue with
        // `x-queue-type`, which is why the kind is read from `type`.
        let v3: Page<QueueInfo> = page(fixture!("queues.v3"));
        let v4: Page<QueueInfo> = page(fixture!("queues.v4"));
        assert_eq!(queue(&v3, "spike.quorum.limit").delivery_limit, None);
        assert_eq!(queue(&v4, "spike.quorum.limit").delivery_limit, Some(3));
        assert!(queue(&v3, "spike.consumed").arguments.is_empty());
        assert_eq!(
            queue(&v4, "spike.consumed").arguments.get("x-queue-type"),
            Some(&json!("classic"))
        );
    }

    #[test]
    fn queues_with_metrics_off_parse_with_counts_and_rates_none() {
        let page: Page<QueueInfo> = page(fixture!("queues.no-metrics.v4"));
        let consumed = queue(&page, "spike.consumed");
        assert_eq!(consumed.messages, None);
        assert_eq!(consumed.messages_ready, None);
        assert_eq!(consumed.consumers, None);
        assert_eq!(consumed.message_stats.publish_rate, None);
        assert_eq!(consumed.queue_type, QueueType::Classic);
    }

    #[test]
    fn queue_type_keeps_an_unknown_kind_and_defaults_to_classic() {
        let q: QueueInfo = serde_json::from_str(r#"{"name":"q","type":"delayed"}"#).unwrap();
        assert_eq!(q.queue_type, QueueType::Other("delayed".into()));
        // No `type` at all (an old broker): classic, the only kind it had.
        let q: QueueInfo = serde_json::from_str(r#"{"name":"q"}"#).unwrap();
        assert_eq!(q.queue_type, QueueType::Classic);
        // Erlang's empty proplist renders as `[]`.
        let q: QueueInfo =
            serde_json::from_str(r#"{"name":"q","arguments":[],"effective_policy_definition":[]}"#)
                .unwrap();
        assert!(q.arguments.is_empty() && q.effective_policy_definition.is_empty());
    }

    #[test]
    fn rates_can_be_negative() {
        let q: QueueInfo = serde_json::from_str(
            r#"{"name":"q","message_stats":{"publish_details":{"rate":-0.2}}}"#,
        )
        .unwrap();
        assert_eq!(q.message_stats.publish_rate, Some(-0.2));
        assert_eq!(q.message_stats.ack_rate, None);
    }

    #[test]
    fn exchanges_parse_types_and_rates_on_both_versions() {
        for (json, topic_in) in [
            (fixture!("exchanges.v3"), 9.6),
            (fixture!("exchanges.v4"), 9.8),
        ] {
            let page: Page<ExchangeInfo> = page(json);
            let find = |name: &str| page.items.iter().find(|e| e.name == name).unwrap();
            assert_eq!(find("").exchange_type, "direct");
            let fanout = find("amq.fanout");
            assert!(fanout.durable && !fanout.auto_delete);
            assert!(fanout.arguments.is_empty() && fanout.policy.is_none());
            assert_eq!(find("amq.fanout").exchange_type, "fanout");
            assert_eq!(find("amq.headers").exchange_type, "headers");
            assert!(find("amq.rabbitmq.trace").internal);
            let topic = find("spike.topic");
            assert_eq!(topic.exchange_type, "topic");
            assert_eq!(topic.message_stats.publish_in_rate, Some(topic_in));
            assert_eq!(topic.message_stats.publish_out_rate, Some(topic_in * 2.0));
            // An exchange that never saw traffic has no `message_stats` at all.
            assert_eq!(find("amq.direct").message_stats.publish_in_rate, None);
        }
        let no_metrics: Page<ExchangeInfo> = page(fixture!("exchanges.no-metrics.v4"));
        assert!(no_metrics
            .items
            .iter()
            .all(|e| e.message_stats.publish_in_rate.is_none()));
    }

    #[test]
    fn connections_parse_on_both_versions() {
        for json in [fixture!("connections.v3"), fixture!("connections.v4")] {
            let conns: Vec<ConnectionInfo> = serde_json::from_str(json).unwrap();
            assert_eq!(conns.len(), 2);
            let admin = conns
                .iter()
                .find(|c| c.user_provided_name.as_deref() == Some("spike-load-admin"))
                .expect("the admin's connection");
            assert_eq!(admin.user.as_deref(), Some("keyhole"));
            assert!(admin.name.contains(" -> "));
            assert!(admin.peer_host.is_some() && admin.peer_port.is_some());
            assert_eq!(admin.channels, Some(3));
            assert_eq!(admin.state.as_deref(), Some("running"));
            assert!(admin.connected_at.is_some());
            assert_eq!(admin.protocol.as_deref(), Some("AMQP 0-9-1"));
            assert!(admin.recv_rate.is_some() && admin.send_rate.is_some());
        }
        // A management-only user sees just its own connection.
        for json in [
            fixture!("connections.management.v3"),
            fixture!("connections.management.v4"),
        ] {
            let conns: Vec<ConnectionInfo> = serde_json::from_str(json).unwrap();
            assert_eq!(conns.len(), 1);
            assert_eq!(conns[0].user.as_deref(), Some("spike-mgmt"));
        }
        let none: Vec<ConnectionInfo> =
            serde_json::from_str(fixture!("connections.no-metrics.v4")).unwrap();
        assert!(none.is_empty());
        // A port that isn't a number reads as missing rather than failing the list.
        let odd: ConnectionInfo =
            serde_json::from_str(r#"{"name":"direct","peer_port":"unknown"}"#).unwrap();
        assert_eq!(odd.peer_port, None);
    }

    #[test]
    fn consumers_parse_on_both_versions() {
        for json in [fixture!("consumers.v3"), fixture!("consumers.v4")] {
            let consumers: Vec<ConsumerInfo> = serde_json::from_str(json).unwrap();
            assert_eq!(consumers.len(), 4);
            let manual = consumers
                .iter()
                .find(|c| c.consumer_tag == "spike-consumer-classic")
                .unwrap();
            assert_eq!(manual.queue.name, "spike.consumed");
            assert_eq!(manual.ack_required, Some(true));
            assert_eq!(manual.prefetch_count, Some(10));
            assert_eq!(manual.active, Some(true));
            assert_eq!(manual.activity_status.as_deref(), Some("up"));
            assert_eq!(manual.exclusive, Some(false));
            let channel = &manual.channel_details;
            assert_eq!(channel.number, Some(1));
            assert!(channel.name.as_deref().unwrap().ends_with(" (1)"));
            assert!(channel.connection_name.is_some());
            assert!(channel.user.is_some());
            let auto = consumers
                .iter()
                .find(|c| c.consumer_tag == "spike-consumer-quorum")
                .unwrap();
            assert_eq!(auto.ack_required, Some(false));
        }
        // A consumer whose channel is gone renders it as `[]`.
        let c: ConsumerInfo = serde_json::from_str(
            r#"{"consumer_tag":"t","queue":{"name":"q"},"channel_details":[]}"#,
        )
        .unwrap();
        assert_eq!(c.channel_details.name, None);
    }

    #[test]
    fn get_parses_bodies_properties_and_headers_on_both_versions() {
        for json in [fixture!("get.classic.v3"), fixture!("get.classic.v4")] {
            let msgs: Vec<GetMessage> = serde_json::from_str(json).unwrap();
            assert_eq!(msgs.len(), 3);

            let full = &msgs[0];
            assert!(full.redelivered);
            assert_eq!(full.exchange, "");
            assert_eq!(full.routing_key, "spike.classic");
            assert_eq!(full.message_count, Some(2));
            let body = full.body().unwrap();
            assert_eq!(body.len() as u64, full.payload_bytes.unwrap());
            assert!(String::from_utf8(body).unwrap().contains("\"order_id\":42"));
            let p = &full.properties;
            assert_eq!(p.content_type.as_deref(), Some("application/json"));
            assert_eq!(p.message_id.as_deref(), Some("msg-0001"));
            assert_eq!(p.correlation_id.as_deref(), Some("corr-abc"));
            assert_eq!(p.message_type.as_deref(), Some("order.created"));
            assert_eq!(p.delivery_mode, Some(2));
            assert_eq!(p.priority, Some(5));
            assert_eq!(p.timestamp, Some(1_762_500_000));
            assert_eq!(p.content_encoding.as_deref(), Some("utf-8"));
            assert_eq!(p.reply_to.as_deref(), Some("spike.replies"));
            assert_eq!(p.expiration.as_deref(), Some("3600000"));
            assert_eq!(p.user_id.as_deref(), Some("keyhole"));
            assert_eq!(p.app_id.as_deref(), Some("keyhole-spike"));
            // Header values keep their JSON types.
            assert_eq!(p.headers.get("x-attempt"), Some(&json!(1)));
            assert_eq!(p.headers.get("x-flag"), Some(&json!(true)));
            assert_eq!(p.headers.get("x-ratio"), Some(&json!(0.5)));
            assert_eq!(p.headers.get("x-tags"), Some(&json!(["a", "b"])));
            assert_eq!(p.headers.get("x-nested"), Some(&json!({"k": "v"})));

            // A binary body survives byte for byte.
            let binary = msgs[1].body().unwrap();
            assert_eq!(&binary[..6], &[0x00, 0x01, 0x02, 0xff, 0xfe, 0x80]);
            assert!(String::from_utf8(binary).is_err());

            // `properties: []` (no properties) parses as empty.
            let plain = &msgs[2];
            assert_eq!(plain.properties.content_type, None);
            assert!(plain.properties.headers.is_empty());
            assert_eq!(plain.body().unwrap(), b"plain text body, no properties");
        }
    }

    #[test]
    fn get_parses_truncated_dead_lettered_and_requeued_messages() {
        for json in [fixture!("get.truncated.v3"), fixture!("get.truncated.v4")] {
            let msgs: Vec<GetMessage> = serde_json::from_str(json).unwrap();
            assert_eq!(msgs[0].body().unwrap().len(), 10);
            assert_eq!(msgs[0].payload_bytes, Some(81));
        }
        for json in [
            fixture!("get.dead-lettered.v3"),
            fixture!("get.dead-lettered.v4"),
        ] {
            let msgs: Vec<GetMessage> = serde_json::from_str(json).unwrap();
            let death = &msgs[0].properties.headers["x-death"][0];
            assert_eq!(death["reason"], json!("delivery_limit"));
            assert_eq!(msgs[0].exchange, "spike.dlx");
        }
        // A requeued quorum message counts the requeue: 3.x as a delivery, 4.x as
        // an acquisition.
        let v3: Vec<GetMessage> = serde_json::from_str(fixture!("get.quorum.requeued.v3")).unwrap();
        let v4: Vec<GetMessage> = serde_json::from_str(fixture!("get.quorum.requeued.v4")).unwrap();
        assert_eq!(
            v3[0].properties.headers.get("x-delivery-count"),
            Some(&json!(1))
        );
        assert_eq!(
            v4[0].properties.headers.get("x-acquired-count"),
            Some(&json!(1))
        );
        for json in [
            fixture!("get.quorum.v3"),
            fixture!("get.quorum.v4"),
            fixture!("get.quorum.reject-requeued.v4"),
        ] {
            let msgs: Vec<GetMessage> = serde_json::from_str(json).unwrap();
            assert!(!msgs.is_empty());
        }
        for json in [fixture!("get.empty.v3"), fixture!("get.empty.v4")] {
            let msgs: Vec<GetMessage> = serde_json::from_str(json).unwrap();
            assert!(msgs.is_empty());
        }
    }

    // -- paging --------------------------------------------------------------

    /// A page of `n` numbered rows claiming `page_count` pages.
    fn numbered(start: usize, n: usize, page_count: u32) -> Page<usize> {
        Page {
            items: (start..start + n).collect(),
            page_count,
        }
    }

    #[test]
    fn collect_pages_requests_up_to_page_count_and_never_past_it() {
        let mut asked = Vec::new();
        let listing = collect_pages(100, |page| {
            asked.push(page);
            Ok(numbered((page as usize - 1) * 10, 10, 3))
        })
        .unwrap();
        assert_eq!(asked, vec![1, 2, 3]);
        assert_eq!(listing.items.len(), 30);
        assert!(!listing.truncated);
    }

    #[test]
    fn collect_pages_stops_at_the_cap_and_reports_truncation() {
        let mut asked = Vec::new();
        let listing = collect_pages(25, |page| {
            asked.push(page);
            Ok(numbered((page as usize - 1) * 10, 10, 9))
        })
        .unwrap();
        assert_eq!(
            asked,
            vec![1, 2, 3],
            "no page past the one that hit the cap"
        );
        assert_eq!(listing.items, (0..25).collect::<Vec<_>>());
        assert!(listing.truncated);

        // Exactly the cap on the last page is complete, not truncated.
        let exact =
            collect_pages(20, |page| Ok(numbered((page as usize - 1) * 10, 10, 2))).unwrap();
        assert_eq!(exact.items.len(), 20);
        assert!(!exact.truncated);
    }

    #[test]
    fn collect_pages_handles_an_empty_vhost_and_an_empty_page() {
        let empty: Page<QueueInfo> = page(fixture!("queues.empty-vhost.v4"));
        assert_eq!(empty.page_count, 0);
        let mut calls = 0;
        let listing = collect_pages(LIST_CAP, |_| {
            calls += 1;
            Ok(page::<QueueInfo>(fixture!("queues.empty-vhost.v4")))
        })
        .unwrap();
        assert_eq!(calls, 1, "page_count 0 means no page 2");
        assert!(listing.items.is_empty() && !listing.truncated);

        // An empty page ends collection even if page_count claims more.
        let mut calls = 0;
        let listing = collect_pages(LIST_CAP, |_| {
            calls += 1;
            Ok(numbered(0, 0, 99))
        })
        .unwrap();
        assert_eq!(calls, 1);
        assert!(listing.items.is_empty());
    }

    #[test]
    fn collect_pages_propagates_an_error() {
        let err = collect_pages::<usize>(LIST_CAP, |_| anyhow::bail!("boom")).unwrap_err();
        assert_eq!(err.to_string(), "boom");
    }

    // -- requests on the wire ----------------------------------------------------

    #[test]
    fn probe_asks_whoami_then_the_version_with_basic_auth() {
        let (base_url, server) = stub_server(vec![
            ok(fixture!("whoami.monitoring.v4")),
            ok(r#"{"rabbitmq_version":"4.4.0","disable_stats":false}"#),
        ]);
        let probe = client(&base_url, "/").probe().expect("probe succeeds");
        assert_eq!(
            probe,
            Probe {
                user: "spike-mon".into(),
                tags: vec!["monitoring".into()],
                version: Some("4.4.0".into()),
                stats_disabled: false,
            }
        );
        let requests = server.join().unwrap();
        assert_eq!(request_line(&requests[0]), "GET /api/whoami");
        assert_eq!(
            request_line(&requests[1]),
            "GET /api/overview?columns=rabbitmq_version,disable_stats"
        );
        for raw in &requests {
            assert_eq!(
                header(raw, "authorization"),
                Some(http::basic_auth("keyhole", Some("s3cret")).as_str())
            );
        }
    }

    #[test]
    fn queues_are_collected_page_by_page_until_page_count() {
        let (base_url, server) = stub_server(vec![
            ok(fixture!("queues.p1-of-2.v4")),
            ok(fixture!("queues.p2-of-2.v4")),
        ]);
        let listing = client(&base_url, "/").queues().expect("lists queues");
        assert_eq!(listing.items.len(), 17);
        assert!(!listing.truncated);
        assert_eq!(listing.items[0].name, "spike space");
        assert_eq!(listing.items[16].name, "spike/slash");

        // Exactly two requests (a third would find the port closed), for pages
        // 1 and 2 of the default vhost, trimmed to the columns.
        let requests = server.join().unwrap();
        for (raw, page) in requests.iter().zip(1..) {
            assert_eq!(
                request_line(raw),
                format!("GET /api/queues/%2F?columns={QUEUE_COLUMNS}&page={page}&page_size=500")
            );
        }
    }

    #[test]
    fn the_other_endpoints_request_their_paths() {
        let (base_url, server) = stub_server(vec![
            ok(fixture!("overview.v3")),
            ok(fixture!("exchanges.v3")),
            ok(fixture!("nodes.v3")),
            ok(fixture!("connections.v3")),
            ok(fixture!("consumers.v3")),
        ]);
        let c = client(&base_url, "my vhost/x");
        assert!(c.overview().unwrap().rabbitmq_version.is_some());
        assert_eq!(c.exchanges().unwrap().items.len(), 12);
        assert_eq!(c.nodes().unwrap().len(), 1);
        assert_eq!(c.connections().unwrap().len(), 2);
        assert_eq!(c.consumers().unwrap().len(), 4);
        let requests = server.join().unwrap();
        let lines: Vec<&str> = requests.iter().map(|r| request_line(r)).collect();
        assert_eq!(
            lines,
            vec![
                "GET /api/overview".to_string(),
                format!(
                    "GET /api/exchanges/my%20vhost%2Fx?columns={EXCHANGE_COLUMNS}&page=1&page_size=500"
                ),
                format!("GET /api/nodes?columns={NODE_COLUMNS}"),
                format!("GET /api/vhosts/my%20vhost%2Fx/connections?columns={CONNECTION_COLUMNS}"),
                format!("GET /api/consumers/my%20vhost%2Fx?columns={CONSUMER_COLUMNS}"),
            ]
        );
    }

    #[test]
    fn get_posts_a_base64_request_and_closes_the_connection() {
        let (base_url, server) = stub_server(vec![ok(fixture!("get.classic.v4"))]);
        let msgs = client(&base_url, "/")
            .get("spike#hash", 3, AckMode::AckRequeueTrue, Some(4096))
            .expect("gets messages");
        assert_eq!(msgs.len(), 3);

        let raw = server.join().unwrap().pop().unwrap();
        // `#` and the vhost's `/` are encoded, so neither cuts the path.
        assert_eq!(request_line(&raw), "POST /api/queues/%2F/spike%23hash/get");
        assert_eq!(header(&raw, "connection"), Some("close"));
        assert_eq!(header(&raw, "content-type"), Some("application/json"));
        assert!(header(&raw, "authorization").is_some());
        let body: Value = serde_json::from_str(raw.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"count": 3, "ackmode": "ack_requeue_true", "encoding": "base64", "truncate": 4096})
        );
    }

    #[test]
    fn get_omits_truncate_when_unset_and_names_the_queue_on_error() {
        let (base_url, server) =
            stub_server(vec![(404, fixture!("error.404.queue-get.v3").into())]);
        let err = client(&base_url, "/")
            .get("gone", 1, AckMode::AckRequeueFalse, None)
            .unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "reading queue `gone`: queue not found (HTTP 404)"
        );
        let raw = server.join().unwrap().pop().unwrap();
        let body: Value = serde_json::from_str(raw.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"count": 1, "ackmode": "ack_requeue_false", "encoding": "base64"})
        );
    }

    // -- errors --------------------------------------------------------------

    /// The message a reply with `status` and fixture `body` maps to.
    fn message(status: u16, body: &str, scope: Scope) -> String {
        client("http://rmq:15672", "/")
            .api_error(status, body, scope)
            .to_string()
    }

    #[test]
    fn auth_failures_name_the_fix() {
        for v in [
            fixture!("error.401.bad-credentials.v3"),
            fixture!("error.401.bad-credentials.v4"),
        ] {
            let msg = message(401, v, Scope::Cluster);
            assert!(
                msg.contains("rejected the credentials for user `keyhole`"),
                "{msg}"
            );
            assert!(
                msg.contains("management_username/management_password"),
                "{msg}"
            );
        }
        for v in [
            fixture!("error.401.not-management.v3"),
            fixture!("error.401.not-management.v4"),
        ] {
            let msg = message(401, v, Scope::Cluster);
            assert!(msg.contains("lacks the `management` tag"), "{msg}");
        }
        for v in [
            fixture!("error.401.not-monitoring.v3"),
            fixture!("error.401.not-monitoring.v4"),
        ] {
            let msg = message(401, v, Scope::Cluster);
            assert!(msg.contains("lacks the `monitoring` tag"), "{msg}");
        }
        for v in [
            fixture!("error.401.not-administrator.v3"),
            fixture!("error.401.not-administrator.v4"),
        ] {
            let msg = message(401, v, Scope::Cluster);
            assert!(msg.contains("lacks the `administrator` tag"), "{msg}");
        }
        for v in [
            fixture!("error.401.queue-read-refused.v3"),
            fixture!("error.401.queue-read-refused.v4"),
        ] {
            let msg = message(401, v, Scope::Vhost);
            assert!(msg.starts_with("permission denied (HTTP 401): ACCESS_REFUSED - read access to queue 'spike.classic'"), "{msg}");
        }
        // A vhost without permissions: a 401 on 3.x, a 404 on 4.x.
        let v3 = message(401, fixture!("error.401.vhost-access.v3"), Scope::Vhost);
        assert!(v3.contains("no permissions on vhost `/`"), "{v3}");
        let v4 = message(404, fixture!("error.404.vhost-access.v4"), Scope::Vhost);
        assert!(
            v4.contains("vhost `/` doesn't exist, or user `keyhole` has no permissions"),
            "{v4}"
        );
    }

    #[test]
    fn a_403_reads_like_a_401() {
        assert_eq!(
            message(403, fixture!("error.401.not-monitoring.v4"), Scope::Cluster),
            message(401, fixture!("error.401.not-monitoring.v4"), Scope::Cluster)
                .replace("HTTP 401", "HTTP 403")
        );
    }

    #[test]
    fn other_errors_carry_the_reason_and_stay_downcastable() {
        for v in [
            fixture!("error.404.queue.v3"),
            fixture!("error.404.queue.v4"),
        ] {
            let msg = message(404, v, Scope::Vhost);
            assert!(msg.contains("vhost `/` doesn't exist"), "{msg}");
        }
        for v in [
            fixture!("error.404.queue-get.v3"),
            fixture!("error.404.queue-get.v4"),
        ] {
            assert_eq!(message(404, v, Scope::Vhost), "queue not found (HTTP 404)");
        }
        // A cluster-wide path that 404s means the URL isn't the management API.
        let msg = message(404, fixture!("error.404.queue.v3"), Scope::Cluster);
        assert_eq!(
            msg,
            "no management API at http://rmq:15672 (HTTP 404) — check management_url"
        );
        for (v, reason) in [
            (
                fixture!("error.400.stream-get.v3"),
                "540 basic.get not supported",
            ),
            (
                fixture!("error.400.stream-get.v4"),
                "540 basic.get not supported",
            ),
            (
                fixture!("error.400.stats-disabled.v4"),
                "Stats in management UI are disabled",
            ),
            (
                fixture!("error.400.page-out-of-range.v3"),
                "Page out of range",
            ),
            (
                fixture!("error.400.page-out-of-range.v4"),
                "Page out of range",
            ),
            (
                fixture!("error.400.page-size.v3"),
                "Invalid pagination parameters",
            ),
            (
                fixture!("error.400.page-size.v4"),
                "Invalid pagination parameters",
            ),
        ] {
            let err = client("http://rmq:15672", "/").api_error(400, v, Scope::Vhost);
            assert!(
                err.to_string()
                    .starts_with(&format!("the management API returned HTTP 400: {reason}")),
                "{err}"
            );
            assert_eq!(err.status, 400);
            assert!(err.reason.as_deref().unwrap().starts_with(reason));
            // The typed error survives the trip through `anyhow`.
            let any: anyhow::Error = err.into();
            assert_eq!(any.downcast_ref::<ApiError>().unwrap().status, 400);
        }
        // A non-JSON body falls back to the shared wording.
        assert_eq!(
            message(502, "<html>bad gateway</html>", Scope::Cluster),
            "the management API returned HTTP 502: <html>bad gateway</html>"
        );
        assert!(message(401, "", Scope::Cluster).contains("rejected the credentials (HTTP 401)"));
    }

    #[test]
    fn errors_come_back_over_the_wire() {
        let (base_url, server) =
            stub_server(vec![(401, fixture!("error.401.bad-credentials.v4").into())]);
        let err = client(&base_url, "/").whoami().unwrap_err();
        assert!(
            err.to_string().contains("rejected the credentials"),
            "{err}"
        );
        assert_eq!(err.downcast_ref::<ApiError>().unwrap().status, 401);
        server.join().unwrap();

        // A 200 that isn't the expected JSON.
        let (base_url, server) = stub_server(vec![ok("<html>not rabbit</html>")]);
        let err = client(&base_url, "/").whoami().unwrap_err();
        assert!(
            err.to_string()
                .starts_with("unexpected management API response"),
            "{err}"
        );
        server.join().unwrap();
    }

    // -- probe state -------------------------------------------------------------

    #[tokio::test]
    async fn a_failed_probe_is_cached_and_a_reprobe_clears_it() {
        // One failed probe (whoami refused), then a successful one.
        let (base_url, server) = stub_server(vec![
            (401, fixture!("error.401.not-management.v3").into()),
            ok(fixture!("whoami.administrator.v3")),
            ok(r#"{"rabbitmq_version":"3.13.7"}"#),
        ]);
        let mut mgmt = Management::new(client(&base_url, "/"));
        assert!(mgmt.client().is_err(), "unusable until probed");

        mgmt.run_probe().await;
        let error = mgmt.error().expect("the failure is recorded").to_string();
        assert!(error.contains("lacks the `management` tag"), "{error}");
        assert!(mgmt.probe().is_none());
        // Later calls get the recorded error back without a request.
        let err = mgmt.client().err().expect("fails fast");
        assert_eq!(err.to_string(), error);

        mgmt.run_probe().await;
        assert_eq!(mgmt.error(), None, "a successful re-probe clears the error");
        let probe = mgmt.probe().expect("probe recorded");
        assert_eq!(probe.tags, vec!["administrator"]);
        assert_eq!(probe.version.as_deref(), Some("3.13.7"));
        assert!(probe.can_monitor());
        assert!(mgmt.client().is_ok());
        assert_eq!(server.join().unwrap().len(), 3);
    }

    #[test]
    fn probe_learns_that_metrics_are_disabled() {
        // The overview trimmed to the probe's columns, from a 4.x node with the
        // metrics collector off.
        let full: Value = serde_json::from_str(fixture!("overview.no-metrics.v4")).unwrap();
        let trimmed = json!({
            "rabbitmq_version": full["rabbitmq_version"],
            "disable_stats": full["disable_stats"],
        })
        .to_string();
        let (base_url, server) = stub_server(vec![
            ok(fixture!("whoami.administrator.v4")),
            (200, trimmed),
        ]);
        let probe = client(&base_url, "/").probe().expect("probe succeeds");
        assert!(probe.stats_disabled);
        assert_eq!(probe.major_version(), Some(4));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn an_unreachable_listing_makes_later_calls_fail_fast() {
        let (base_url, server) = stub_server(vec![
            ok(fixture!("whoami.administrator.v3")),
            ok(r#"{"rabbitmq_version":"3.13.7"}"#),
        ]);
        let mut mgmt = Management::new(client(&base_url, "/"));
        mgmt.run_probe().await;
        assert!(mgmt.client().is_ok());
        server.join().unwrap();

        mgmt.mark_unreachable(&anyhow::anyhow!(
            "could not reach the management API: refused"
        ));
        let err = mgmt.client().err().expect("fails fast");
        assert!(err.to_string().contains("refused"), "{err}");
        assert!(mgmt.probe().is_none(), "a re-probe is needed");
    }

    #[tokio::test]
    async fn an_unreachable_api_records_why() {
        // Bind then drop, so the port is (almost certainly) closed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut mgmt = Management::new(client(&base_url, "/"));
        mgmt.run_probe().await;
        let error = mgmt.error().expect("recorded");
        assert!(
            error.contains("could not reach the management API"),
            "{error}"
        );
        assert_eq!(mgmt.base_url(), base_url);
    }
}
