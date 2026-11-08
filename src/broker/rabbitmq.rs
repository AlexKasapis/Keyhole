//! RabbitMQ (AMQP 0.9.1) implementation of [`BrokerConnection`], built on
//! `lapin`. Works against every RabbitMQ version (3.x and 4.x).
//!
//! Read + record, plus a deliberate one-message publish, like the AMQP 1.0
//! broker (see [`Capabilities::rabbitmq`]). The live view is a
//! **non-destructive exchange tap**:
//!  1. declare a temporary, server-named, `exclusive` + `auto_delete` queue;
//!  2. bind it to the target exchange with the requested binding key;
//!  3. consume the copies routed to it (auto-ack).
//!
//! Because the spy queue is brand-new and bound *in addition* to whatever real
//! queues exist, the broker routes independent copies to it — the real queues
//! and their consumers never lose a message. Auto-acking only discards our own
//! copy, and the spy queue auto-deletes when the tail's connection drops. This
//! is a stronger non-destructive guarantee than AMQP 1.0's queue browse: we
//! never read from an existing queue at all. The destination list leaves the
//! spy queues out ([`taps`]).
//!
//! Each tail owns a dedicated connection + channel + consumer (mirroring the
//! Redis dedicated-socket model) so the returned stream is `'static` and the
//! actor's main connection stays free for liveness checks.
//!
//! Listing queues and exchanges, peeking, stats and the connection and consumer
//! rosters need the HTTP management API ([`mgmt`]), which `connect` probes after
//! the AMQP login. A failed probe leaves the connection usable for tails,
//! without the stats band or the roster tabs.
//!
//! A **queue peek** ([`RabbitmqConnection::peek`]) is the one read that touches
//! a real queue: the management API's `/get` takes the messages off the queue
//! and, in browse mode, puts them straight back (`ack_requeue_true`) flagged
//! `redelivered`. It only runs when the user opens a queue.
//!
//! A **publish** ([`RabbitmqConnection::publish`]) goes over AMQP on a channel
//! of the main connection, with publisher confirms on and `mandatory` set, so
//! it reports whether the broker confirmed, NACKed or returned (couldn't
//! route) the message. It doesn't need the management API.

mod mgmt;
mod taps;

pub use self::taps::TapQueues;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use lapin::message::Delivery;
use lapin::options::{
    BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{FieldTable, ShortString, MAX_SHORT_STRING_LENGTH};
use lapin::uri::{AMQPAuthority, AMQPScheme, AMQPUri, AMQPUserInfo};
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, Consumer,
    ExchangeKind,
};

use self::mgmt::{
    AckMode, ApiError, ConnectionInfo, ConsumerInfo, ExchangeInfo, GetMessage, Management,
    MessageProperties, MgmtClient, NodeInfo, Overview, QueueInfo,
};
use self::taps::TapQueueGuard;
use super::{
    BrokerConnection, BrokerEvent, BrokerEventStream, BrokerStats, Capabilities, ClientConnection,
    DestKind, DestinationInfo, DestinationList, ExchangeType, NodeHealth, Payload, PeekReq,
    PublishReq, QueueConsumer, QueueType, RabbitmqStats, Roster, RosterKind, RosterRows, SubSpec,
    STREAM_PEEK_HINT,
};
use crate::config::{PeekMode, RabbitmqProfile};

/// The username and password the AMQP login uses when the profile has none (the
/// AMQP URI spec's default). The management login defaults the same way, so both
/// channels authenticate as the same user.
const DEFAULT_CREDENTIAL: &str = "guest";

/// How long a publish waits for the broker's confirm. A broker under a memory
/// or disk alarm blocks publishing connections without a word, and the actor
/// runs one command at a time, so the wait must end.
const PUBLISH_CONFIRM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// AMQP 0.9.1's `delivery_mode` for a persistent message.
const PERSISTENT: u8 = 2;

/// `value` as an AMQP short string, or an error naming `what` it is (e.g.
/// "routing key") and the 255-byte limit, short enough for the footer's
/// status column. lapin's `From<&str>` panics past the limit, so every name,
/// key or vhost from the user, the config or the broker reaches lapin through
/// here.
pub(crate) fn short_string(what: &str, value: &str) -> anyhow::Result<ShortString> {
    ShortString::try_new(value)
        .map_err(|_| anyhow::anyhow!("{what} too long, max {MAX_SHORT_STRING_LENGTH} bytes"))
}

/// Where lapin connects for `profile`, logging in with `password`. Built as
/// lapin's URI struct rather than a URL string it parses back, so the host and
/// vhost reach it as configured: in a URL a `/` in the host starts the vhost,
/// an IPv6 host comes back as `localhost`, and a `.` or `..` vhost is dropped
/// as a dot segment. A missing or empty username or password is `guest`, as
/// in a URL without one. Fails on a vhost over AMQP's 255 bytes.
pub(crate) fn amqp_uri(
    profile: &RabbitmqProfile,
    password: Option<&str>,
) -> anyhow::Result<AMQPUri> {
    // lapin makes the vhost a short string as it connects.
    short_string("vhost", &profile.vhost)?;
    let credential = |s: Option<&str>| {
        s.filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_CREDENTIAL)
            .to_string()
    };
    // A bare address: only a URL needs an IPv6 host in brackets.
    let host = profile
        .host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(&profile.host);
    Ok(AMQPUri {
        // `amqps` is the :5671 TLS listener.
        scheme: if profile.tls {
            AMQPScheme::AMQPS
        } else {
            AMQPScheme::AMQP
        },
        authority: AMQPAuthority {
            userinfo: AMQPUserInfo {
                username: credential(profile.username.as_deref()),
                password: credential(password),
            },
            host: host.to_string(),
            port: profile.port,
        },
        vhost: profile.vhost.clone(),
        ..AMQPUri::default()
    })
}

/// A live (or not-yet-connected) RabbitMQ (AMQP 0.9.1) connection.
pub struct RabbitmqConnection {
    profile: RabbitmqProfile,
    password: Option<String>,
    /// The main connection, kept open for liveness checks and publishing.
    conn: Option<Connection>,
    /// The channel publishes go out on, with publisher confirms on. Opened by
    /// the first publish, and again by the next one after a broker error (a
    /// 403 on an internal exchange, a missing exchange) closed it. Taps have
    /// their own connections, so this channel closing never touches them.
    publish_channel: Option<Channel>,
    /// The HTTP management API and what its last probe found (tags, version,
    /// or the `mgmt_error` that disables it).
    mgmt: Management,
    /// Each exchange's type from the last listing, so a tap's advisory can
    /// tell whether the default `#` binding key fits the exchange.
    exchange_types: HashMap<String, ExchangeType>,
    /// The most body bytes a peek fetches per message (the
    /// `value_preview_bytes` setting); longer bodies come back truncated.
    preview_bytes: usize,
    /// The spy queues of this session's taps, which the listing leaves out.
    /// Shared with the session's other RabbitMQ connections.
    tap_queues: Arc<TapQueues>,
}

impl RabbitmqConnection {
    /// Build a connection from a profile, its resolved AMQP password, the
    /// resolved management API password (`management_password`, else the AMQP
    /// password), the most body bytes a peek shows per message, and the
    /// session's tap queues. Call [`BrokerConnection::connect`] to actually
    /// establish it.
    pub fn new(
        profile: RabbitmqProfile,
        password: Option<String>,
        management_password: Option<String>,
        preview_bytes: usize,
        tap_queues: Arc<TapQueues>,
    ) -> Self {
        let mgmt = Management::new(management_client(&profile, management_password));
        Self {
            profile,
            password,
            conn: None,
            publish_channel: None,
            mgmt,
            exchange_types: HashMap::new(),
            preview_bytes: preview_bytes.max(1),
            tap_queues,
        }
    }

    /// Why the management API can't be used, naming where it was looked for.
    fn unavailable(&self) -> String {
        format!("management API unavailable at {}", self.mgmt.base_url())
    }

    /// What the connection offers, given the management probe's outcome: the
    /// stats band needs the API, so it's on only when the probe succeeded.
    /// It stays as connect found it for the session; a later re-probe doesn't
    /// change it (reconnect instead).
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            can_dashboard: self.mgmt.probe().is_some(),
            ..Capabilities::rabbitmq()
        }
    }

    /// Where lapin connects: see [`amqp_uri`].
    fn uri(&self) -> anyhow::Result<AMQPUri> {
        amqp_uri(&self.profile, self.password.as_deref())
    }

    /// Connection properties carrying a recognisable name for the broker's
    /// management UI. lapin's default features bind it to the ambient tokio
    /// runtime, so no executor/reactor wiring is needed here.
    fn conn_props(&self) -> ConnectionProperties {
        let name = format!("keyhole-{}-{}", self.profile.name, super::next_conn_seq());
        ConnectionProperties::default().with_connection_name(name.into())
    }

    /// The open publish channel, or a new one (confirms on) when there is none
    /// or a broker error closed the last.
    async fn publish_channel(&mut self) -> anyhow::Result<Channel> {
        if let Some(channel) = self
            .publish_channel
            .as_ref()
            .filter(|c| c.status().connected())
        {
            return Ok(channel.clone());
        }
        self.publish_channel = None;
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("connection is not established"))?;
        let channel = conn
            .create_channel()
            .await
            .context("opening the publish channel")?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .context("turning on publisher confirms")?;
        self.publish_channel = Some(channel.clone());
        Ok(channel)
    }
}

#[async_trait]
impl BrokerConnection for RabbitmqConnection {
    async fn connect(&mut self) -> anyhow::Result<Capabilities> {
        // A `tls` profile builds a rustls config.
        super::install_tls_provider();
        let conn = Connection::connect_uri(self.uri()?, self.conn_props())
            .await
            // `.context` keeps the lapin error as the source (so its reply
            // code/text survives, shown via the `{:#}` chain) rather than
            // flattening it into the message here.
            .context("connecting to RabbitMQ")?;
        self.conn = Some(conn);
        self.publish_channel = None;
        // Find a dead or misconfigured management API once, up front; a failure
        // is recorded and the connection stays usable for tails.
        self.mgmt.run_probe().await;
        Ok(self.capabilities())
    }

    async fn ping(&mut self) -> anyhow::Result<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("connection is not established"))?;
        // Opening a channel round-trips to the broker (channel.open / open-ok),
        // so a dead peer surfaces here rather than lingering silently. The
        // channel closes when it drops at the end of this scope.
        conn.create_channel()
            .await
            .context("liveness check failed")?;
        Ok(())
    }

    async fn subscribe(&mut self, spec: SubSpec) -> anyhow::Result<BrokerEventStream> {
        let (exchange, binding_key) = match &spec {
            SubSpec::Exchange {
                exchange,
                binding_key,
            } => (exchange.clone(), binding_key.clone()),
            other => anyhow::bail!("{} is not a RabbitMQ destination", other.label()),
        };
        // Each tail is its own dedicated connection (and thus its own spy queue).
        open_exchange_tap(
            self.uri()?,
            self.conn_props(),
            &self.tap_queues,
            exchange,
            binding_key,
        )
        .await
    }

    async fn list_destinations(&mut self, reprobe: bool) -> anyhow::Result<DestinationList> {
        if reprobe && self.mgmt.error().is_some() {
            self.mgmt.run_probe().await;
        }
        let unavailable = self.unavailable();
        // A failed probe answers here without touching the network.
        let client = self.mgmt.client().context(unavailable.clone())?.clone();
        let probe = self.mgmt.probe();
        let stats_disabled = probe.is_some_and(|p| p.stats_disabled);
        // Only 3.x counts a requeue toward a quorum queue's delivery limit. An
        // unknown version is assumed to count, so the warning errs on the side
        // of showing.
        let requeue_counts = probe
            .and_then(mgmt::Probe::major_version)
            .is_none_or(|major| major < 4);
        let fetched = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            Ok((client.queues()?, client.exchanges()?))
        })
        .await
        .context("the listing task failed")?;
        let (queues, exchanges) = match fetched {
            Ok(lists) => lists,
            Err(e) => {
                // A refusal (an HTTP error reply) is cheap to repeat; a call that
                // got no reply would cost the timeouts again on every refresh.
                if e.downcast_ref::<ApiError>().is_none() {
                    self.mgmt.mark_unreachable(&e);
                }
                return Err(e.context(unavailable));
            }
        };
        let mut list = destination_list(queues, exchanges, stats_disabled, requeue_counts);
        self.tap_queues.hide(&mut list);
        self.exchange_types = list
            .items
            .iter()
            .filter_map(|d| Some((d.name.clone(), d.exchange_type.clone()?)))
            .collect();
        Ok(list)
    }

    /// The overview's versions, rates and totals, plus each node's health when
    /// the user may read `/api/nodes` (the `monitoring` or `administrator`
    /// tag). Runs on the stats clock, so it never re-probes: a failed probe or
    /// an API marked unreachable answers at once with the cached error.
    async fn stats(&mut self) -> anyhow::Result<BrokerStats> {
        let unavailable = self.unavailable();
        // A failed probe answers here without touching the network. The error
        // is flattened into its message, which the stats reply carries.
        let client = self
            .mgmt
            .client()
            .map_err(|e| anyhow::anyhow!("{:#}", e.context(unavailable.clone())))?
            .clone();
        let monitor = self.mgmt.probe().is_some_and(mgmt::Probe::can_monitor);
        let fetched = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let overview = client.overview()?;
            let nodes = if monitor {
                match client.nodes() {
                    Ok(nodes) => Some(nodes),
                    // Refused (the user's tags changed since the probe): show
                    // the overview without node health.
                    Err(e) if e.downcast_ref::<ApiError>().is_some() => None,
                    Err(e) => return Err(e),
                }
            } else {
                None
            };
            Ok((overview, nodes))
        })
        .await
        .map_err(|e| anyhow::anyhow!("the stats task failed: {e}"))?;
        match fetched {
            Ok((overview, nodes)) => Ok(BrokerStats::Rabbitmq(rabbitmq_stats(
                overview, nodes, monitor,
            ))),
            Err(e) => {
                // No reply at all: later refreshes fail fast until a re-probe.
                if e.downcast_ref::<ApiError>().is_none() {
                    self.mgmt.mark_unreachable(&e);
                }
                Err(anyhow::anyhow!("{:#}", e.context(unavailable)))
            }
        }
    }

    /// The vhost's connections or consumers. Like the stats, this runs on the
    /// stats clock (while its tab is focused), so it never re-probes. Without
    /// the `monitoring` tag the API lists only the user's own; with the
    /// metrics collector off it lists nothing, so nothing is asked.
    async fn roster(&mut self, kind: RosterKind) -> anyhow::Result<Roster> {
        let unavailable = self.unavailable();
        // A failed probe answers here without touching the network.
        let client = self
            .mgmt
            .client()
            .map_err(|e| anyhow::anyhow!("{:#}", e.context(unavailable.clone())))?
            .clone();
        let probe = self.mgmt.probe();
        let own_only = !probe.is_some_and(mgmt::Probe::can_monitor);
        // With metrics off, connections come back `[]` and consumers as a
        // 400, so an empty list would only mislead.
        if probe.is_some_and(|p| p.stats_disabled) {
            return Ok(Roster::empty(kind, own_only, true));
        }
        let fetched = tokio::task::spawn_blocking(move || -> anyhow::Result<RosterRows> {
            Ok(match kind {
                RosterKind::Connections => connection_rows(client.connections()?),
                RosterKind::Consumers => consumer_rows(client.consumers()?),
            })
        })
        .await
        .map_err(|e| anyhow::anyhow!("the {} task failed: {e}", kind.noun()))?;
        match fetched {
            Ok(rows) => Ok(Roster {
                rows,
                own_only,
                stats_disabled: false,
            }),
            // The node's metrics went off after the probe.
            Err(e) if e.downcast_ref::<ApiError>().is_some_and(is_stats_refusal) => {
                Ok(Roster::empty(kind, own_only, true))
            }
            Err(e) => {
                // No reply at all: later refreshes fail fast until a re-probe.
                if e.downcast_ref::<ApiError>().is_none() {
                    self.mgmt.mark_unreachable(&e);
                }
                Err(anyhow::anyhow!("{:#}", e.context(unavailable)))
            }
        }
    }

    /// Peek a queue through the management API's `/get`: browse mode puts the
    /// messages back (`ack_requeue_true`, which RabbitMQ flags `redelivered`),
    /// destructive mode removes them (`ack_requeue_false`). Bodies are capped at
    /// `preview_bytes`. A stream queue refuses `/get`; that comes back as the
    /// [`STREAM_PEEK_HINT`].
    async fn peek(&mut self, req: PeekReq) -> anyhow::Result<Vec<BrokerEvent>> {
        let queue = match &req.spec {
            SubSpec::Queue(q) => q.clone(),
            other => anyhow::bail!(
                "{} can't be peeked: only a queue holds messages",
                other.label()
            ),
        };
        let Some(ackmode) = ack_mode(req.mode) else {
            return Ok(Vec::new());
        };
        let unavailable = self.unavailable();
        // A failed probe answers here without touching the network.
        let client = self.mgmt.client().context(unavailable.clone())?.clone();
        let (limit, truncate) = (req.limit, self.preview_bytes);
        let name = queue.clone();
        let fetched =
            tokio::task::spawn_blocking(move || client.get(&name, limit, ackmode, Some(truncate)))
                .await
                .context("the peek task failed")?;
        match fetched {
            Ok(messages) => Ok(messages
                .into_iter()
                .map(|m| peek_event(&queue, m))
                .collect()),
            Err(e) => Err(match e.downcast_ref::<ApiError>() {
                // The listing didn't say it's a stream, but the broker did.
                Some(api) if is_stream_refusal(api) => anyhow::anyhow!(STREAM_PEEK_HINT),
                Some(_) => e,
                // No reply at all: later calls fail fast until a re-probe.
                None => {
                    self.mgmt.mark_unreachable(&e);
                    e.context(unavailable)
                }
            }),
        }
    }

    /// Publish one persistent message: to a queue through the default
    /// exchange (its routing key is the queue's name), or to an exchange with
    /// `req.routing_key`. Waits for the broker's confirm. `mandatory` makes the
    /// broker return a message no queue takes, which is reported as
    /// unroutable. A broker error closes the channel; the next publish opens
    /// another.
    ///
    /// Every message is marked persistent (`delivery_mode` 2): keyhole can't
    /// tell which queues an exchange will route it to, a durable queue then
    /// keeps it across a restart like the messages its applications send, and
    /// the confirm comes once the broker has stored it. A transient queue
    /// ignores the flag. The body goes out as it was typed, with no content
    /// type, as AMQP 1.0's publish does.
    async fn publish(&mut self, req: PublishReq) -> anyhow::Result<()> {
        // A queue's routing key is its name.
        let (exchange, key_name, target) = match &req.spec {
            SubSpec::Queue(q) => (String::new(), "queue name", format!("queue `{q}`")),
            SubSpec::Exchange { exchange, .. } => (
                exchange.clone(),
                "routing key",
                format!("exchange `{exchange}`"),
            ),
            other => anyhow::bail!("{} is not a RabbitMQ destination", other.label()),
        };
        // Checked before the channel is touched, so an over-long name or key
        // leaves it as it was.
        let exchange_name = short_string("exchange name", &exchange)?;
        let routing_key = short_string(key_name, &req.routing_key)?;
        let channel = self.publish_channel().await?;
        let sent = tokio::time::timeout(
            PUBLISH_CONFIRM_TIMEOUT,
            publish_confirmed(&channel, exchange_name, routing_key, &req.body),
        )
        .await;
        let confirmation = match sent {
            Ok(Ok(confirmation)) => confirmation,
            Ok(Err(e)) => {
                // A broker refusal closes the channel, and any other failure
                // leaves it suspect: the next publish opens a fresh one.
                self.publish_channel = None;
                return Err(publish_error(e, &target));
            }
            Err(_) => {
                // A confirm may still arrive on this channel; start afresh.
                self.publish_channel = None;
                anyhow::bail!(
                    "no confirm from the broker within {}s (is an alarm blocking publishers?)",
                    PUBLISH_CONFIRM_TIMEOUT.as_secs()
                );
            }
        };
        let exchange_type = self.exchange_types.get(&exchange);
        publish_result(
            PublishOutcome::from(confirmation),
            &req.spec,
            exchange_type,
            &req.routing_key,
        )
    }

    async fn tail_notice(&mut self, spec: &SubSpec) -> Option<String> {
        // The default `#` binding key matches every routing key on a *topic*
        // exchange and is ignored by a *fanout* exchange, but a *direct* exchange
        // matches it only against the literal key `#` — so the tap could attach
        // yet stay silent. A *headers* exchange ignores the key, and a binding
        // without header arguments matches every message, so the tap gets
        // everything (checked on 3.13) like a fanout. The last listing knows each
        // exchange's type; without it (no management API, or an exchange it
        // hasn't listed) flag the ambiguity whenever the catch-all default is
        // in use (mirroring the Redis keyspace notice).
        let SubSpec::Exchange {
            exchange,
            binding_key,
        } = spec
        else {
            return None;
        };
        if binding_key != "#" {
            return None;
        }
        match self.exchange_types.get(exchange) {
            Some(ExchangeType::Topic | ExchangeType::Fanout | ExchangeType::Headers) => None,
            Some(ExchangeType::Direct) => Some(format!(
                "`{exchange}` is a direct exchange: binding key `#` only matches messages \
                 routed with the key `#` — tap exchange:{exchange}/<routing-key> instead"
            )),
            Some(ExchangeType::Other(_)) | None => Some(
                "binding key `#` matches every key on a topic exchange and is ignored by \
                 fanout and headers exchanges, but on a direct exchange it matches only the \
                 key `#` — give an explicit key (exchange:name/key) if this tail stays \
                 silent"
                    .to_string(),
            ),
        }
    }
}

/// The stats band's figures from the management API's overview and, for a
/// `monitoring` user (`cluster_wide`), its nodes. Without that tag the overview
/// counts only the vhosts the user can access and only its own connections
/// (checked on 3.13 and 4.4), so the connection count is left out rather than
/// shown as a server total.
fn rabbitmq_stats(
    overview: Overview,
    nodes: Option<Vec<NodeInfo>>,
    cluster_wide: bool,
) -> RabbitmqStats {
    let totals = overview.object_totals;
    RabbitmqStats {
        rabbitmq_version: overview.rabbitmq_version,
        erlang_version: overview.erlang_version,
        cluster_name: overview.cluster_name,
        stats_disabled: overview.disable_stats,
        cluster_wide,
        publish_rate: overview.message_stats.publish_rate,
        deliver_rate: overview.message_stats.deliver_get_rate,
        ack_rate: overview.message_stats.ack_rate,
        messages: overview.queue_totals.messages,
        messages_ready: overview.queue_totals.messages_ready,
        messages_unacked: overview.queue_totals.messages_unacknowledged,
        queues: totals.queues,
        connections: totals.connections.filter(|_| cluster_wide),
        consumers: totals.consumers,
        nodes: nodes
            .unwrap_or_default()
            .into_iter()
            .map(node_health)
            .collect(),
    }
}

fn node_health(n: NodeInfo) -> NodeHealth {
    NodeHealth {
        name: n.name,
        running: n.running,
        uptime: n.uptime.map(|ms| ms / 1000),
        mem_used: n.mem_used,
        mem_limit: n.mem_limit,
        mem_alarm: n.mem_alarm,
        fd_used: n.fd_used,
        fd_total: n.fd_total,
        disk_free: n.disk_free,
        disk_free_limit: n.disk_free_limit,
        disk_free_alarm: n.disk_free_alarm,
        partitions: n.partitions,
    }
}

/// The `/get` settlement for a peek `mode`; `None` for [`PeekMode::Skip`],
/// which reads nothing.
fn ack_mode(mode: PeekMode) -> Option<AckMode> {
    match mode {
        // Never worse than `reject_requeue_true`, and on 4.x it doesn't raise
        // a quorum queue's delivery count (checked on 3.13 and 4.4).
        PeekMode::Browse => Some(AckMode::AckRequeueTrue),
        PeekMode::Destructive => Some(AckMode::AckRequeueFalse),
        PeekMode::Skip => None,
    }
}

/// Whether a `/get` refusal is a stream queue's: a 400 whose reason is `540
/// basic.get not supported by stream queues …` (3.13 and 4.4 alike).
fn is_stream_refusal(error: &ApiError) -> bool {
    error.status == 400
        && error
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("basic.get not supported"))
}

/// Whether a refusal says the node's metrics collector is off: `/api/consumers`
/// answers a 400 "Stats in management UI are disabled on this node" (4.4).
fn is_stats_refusal(error: &ApiError) -> bool {
    error.status == 400
        && error
            .reason
            .as_deref()
            .is_some_and(|r| r.contains("Stats in management UI are disabled"))
}

/// The Connections tab's rows, by name so they hold still between refreshes
/// (the API lists them in no set order).
fn connection_rows(rows: Vec<ConnectionInfo>) -> RosterRows {
    let mut rows: Vec<ClientConnection> = rows.into_iter().map(client_connection).collect();
    rows.sort_by(|a, b| {
        a.display_name()
            .cmp(b.display_name())
            .then_with(|| a.name.cmp(&b.name))
    });
    RosterRows::Connections(rows)
}

/// The Consumers tab's rows, by queue and then consumer tag.
fn consumer_rows(rows: Vec<ConsumerInfo>) -> RosterRows {
    let mut rows: Vec<QueueConsumer> = rows.into_iter().map(queue_consumer).collect();
    rows.sort_by(|a, b| (&a.queue, &a.tag, &a.channel).cmp(&(&b.queue, &b.tag, &b.channel)));
    RosterRows::Consumers(rows)
}

/// A `/api/vhosts/{vhost}/connections` row for the Connections tab.
fn client_connection(c: ConnectionInfo) -> ClientConnection {
    ClientConnection {
        client_name: c.user_provided_name.filter(|n| !n.is_empty()),
        user: c.user,
        peer: c.peer_host.map(|host| peer_address(&host, c.peer_port)),
        channels: c.channels,
        state: c.state,
        connected_at: c.connected_at.and_then(|ms| {
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).ok()
        }),
        recv_rate: c.recv_rate,
        send_rate: c.send_rate,
        name: c.name,
    }
}

/// `host:port`, bracketing an IPv6 host; just the host without a port.
fn peer_address(host: &str, port: Option<u16>) -> String {
    match port {
        Some(port) if host.contains(':') => format!("[{host}]:{port}"),
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

/// A `/api/consumers/{vhost}` row for the Consumers tab. The channel's name is
/// `peer -> broker (n)`; the broker side is the same for every row, so it's
/// shortened to `peer (n)`.
fn queue_consumer(c: ConsumerInfo) -> QueueConsumer {
    let details = c.channel_details;
    let peer = details
        .connection_name
        .as_deref()
        .map(|name| name.split(" -> ").next().unwrap_or(name).to_string());
    let channel = match (peer, details.number) {
        (Some(peer), Some(n)) => Some(format!("{peer} ({n})")),
        _ => details.name,
    };
    QueueConsumer {
        queue: c.queue.name,
        tag: c.consumer_tag,
        channel,
        ack_required: c.ack_required,
        prefetch: c.prefetch_count,
        active: c.active,
        activity_status: c.activity_status,
    }
}

/// A message `/get` fetched from `queue`, as an event: the queue is the source,
/// and how the message arrived (exchange, routing key, redelivery) plus its
/// properties and headers ride along as metadata for the inspector's preview
/// and filter.
fn peek_event(queue: &str, msg: GetMessage) -> BrokerEvent {
    let mut body = msg.body().unwrap_or_else(|e| {
        // RabbitMQ sends valid base64. Should a body still not decode, show it
        // as sent rather than fail the whole batch: a destructive peek has
        // already taken it off the queue.
        tracing::debug!(error = %e, queue, "undecodable peeked body");
        msg.payload.clone().into_bytes()
    });
    // `truncate` cuts the body before encoding; `payload_bytes` is its full size.
    let full = msg.payload_bytes.unwrap_or(body.len() as u64);
    let truncated = (body.len() as u64) < full;
    if truncated {
        drop_cut_character(&mut body);
    }
    let exchange = if msg.exchange.is_empty() {
        "(default)".to_string()
    } else {
        msg.exchange
    };
    let mut meta = vec![
        ("exchange".to_string(), exchange),
        ("routing_key".to_string(), msg.routing_key),
        ("redelivered".to_string(), msg.redelivered.to_string()),
    ];
    if truncated {
        meta.push((
            "truncated".to_string(),
            format!("showing {} of {full} bytes", body.len()),
        ));
    }
    meta.extend(property_meta(msg.properties));
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: queue.to_string(),
        payload: Payload::classify(body),
        meta,
    }
}

/// Drop a multi-byte character that truncation cut in half, so a truncated
/// text body still reads as text rather than binary.
fn drop_cut_character(body: &mut Vec<u8>) {
    if let Err(e) = std::str::from_utf8(body) {
        // `error_len() == None`: the bytes only stop mid-character.
        if e.error_len().is_none() {
            body.truncate(e.valid_up_to());
        }
    }
}

/// The properties a message carries, as metadata (only those it set), then
/// each header as `headers.<name>`.
fn property_meta(p: MessageProperties) -> Vec<(String, String)> {
    let timestamp = p.timestamp.map(|secs| {
        i64::try_from(secs)
            .ok()
            .and_then(|s| OffsetDateTime::from_unix_timestamp(s).ok())
            .and_then(|t| t.format(&Rfc3339).ok())
            .unwrap_or_else(|| secs.to_string())
    });
    let delivery_mode = p.delivery_mode.map(|m| match m {
        1 => "1 (transient)".to_string(),
        2 => "2 (persistent)".to_string(),
        other => other.to_string(),
    });
    let fields = [
        ("content_type", p.content_type),
        ("message_id", p.message_id),
        ("correlation_id", p.correlation_id),
        ("delivery_mode", delivery_mode),
        ("content_encoding", p.content_encoding),
        ("priority", p.priority.map(|n| n.to_string())),
        ("timestamp", timestamp),
        ("type", p.message_type),
        ("app_id", p.app_id),
        ("user_id", p.user_id),
        ("reply_to", p.reply_to),
        ("expiration", p.expiration),
    ];
    let mut meta: Vec<(String, String)> = fields
        .into_iter()
        .filter_map(|(key, value)| Some((key.to_string(), value?)))
        .collect();
    meta.extend(
        p.headers
            .into_iter()
            .map(|(name, value)| (format!("headers.{name}"), header_text(value))),
    );
    meta
}

/// A header value as text: a string as itself, anything else (a number, a
/// bool, an array, a nested table such as `x-death`) as compact JSON.
fn header_text(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// The destination list from the management API's queue and exchange
/// listings: queues first, then exchanges, each in the API's name order. The
/// default exchange (`""`) is left out: it can't be bound, so it can't be
/// tapped. `requeue_counts` says whether the broker counts a requeue toward a
/// quorum queue's delivery limit (3.x).
fn destination_list(
    queues: mgmt::Listing<QueueInfo>,
    exchanges: mgmt::Listing<ExchangeInfo>,
    stats_disabled: bool,
    requeue_counts: bool,
) -> DestinationList {
    let truncated = queues.truncated || exchanges.truncated;
    let items = queues
        .items
        .into_iter()
        .map(|q| queue_info(q, requeue_counts))
        .chain(
            exchanges
                .items
                .into_iter()
                .filter(|e| !e.name.is_empty())
                .map(exchange_info),
        )
        .collect();
    DestinationList {
        items,
        truncated,
        stats_disabled,
    }
}

fn queue_info(q: QueueInfo, requeue_counts: bool) -> DestinationInfo {
    // Only 3.x lacks the `delivery_limit` field; a broker that reports it (even
    // as `unlimited`) is 4.x, where a requeue doesn't count either way.
    let counts = requeue_counts && q.queue_type == QueueType::Quorum && q.delivery_limit.is_none();
    let peek_delivery_limit = counts.then(|| configured_delivery_limit(&q)).flatten();
    DestinationInfo {
        peek_delivery_limit,
        name: q.name,
        kind: DestKind::Queue,
        messages: q.messages,
        ready: q.messages_ready,
        unacked: q.messages_unacknowledged,
        consumers: q.consumers,
        queue_type: Some(q.queue_type),
        exchange_type: None,
        publish_rate: q.message_stats.publish_rate,
        internal: false,
    }
}

/// A 3.x quorum queue's delivery limit: its `x-delivery-limit` argument or a
/// `delivery-limit` policy, the lower one when both are set (as the broker
/// applies it). 3.x has no default limit. It doesn't validate the argument, so
/// the limit can be negative, which (like 0) drops or dead-letters a message on
/// its first requeue.
fn configured_delivery_limit(q: &QueueInfo) -> Option<i64> {
    let argument = q.arguments.get("x-delivery-limit").and_then(|v| v.as_i64());
    let policy = q
        .effective_policy_definition
        .get("delivery-limit")
        .and_then(|v| v.as_i64());
    match (argument, policy) {
        (Some(a), Some(p)) => Some(a.min(p)),
        (a, p) => a.or(p),
    }
}

fn exchange_info(e: ExchangeInfo) -> DestinationInfo {
    DestinationInfo {
        name: e.name,
        kind: DestKind::Exchange,
        messages: None,
        ready: None,
        unacked: None,
        consumers: None,
        queue_type: None,
        exchange_type: Some(ExchangeType::from(e.exchange_type)),
        // Published into the exchange by clients (`publish_out` is what it
        // routed on to queues).
        publish_rate: e.message_stats.publish_in_rate,
        peek_delivery_limit: None,
        internal: e.internal,
    }
}

/// The management client for `profile`: its base URL and vhost, logging in as
/// `management_username` (else the AMQP `username`) with `password`. A missing
/// username or password defaults to lapin's `guest`, so the management login is
/// the same user the AMQP login was.
fn management_client(profile: &RabbitmqProfile, password: Option<String>) -> MgmtClient {
    let set = |s: &Option<String>| s.clone().filter(|s| !s.is_empty());
    let username = set(&profile.management_username)
        .or_else(|| set(&profile.username))
        .unwrap_or_else(|| DEFAULT_CREDENTIAL.to_string());
    MgmtClient::new(
        profile.management_base_url(),
        profile.vhost.clone(),
        username,
        password.unwrap_or_else(|| DEFAULT_CREDENTIAL.to_string()),
    )
}

/// Publish `body` on `channel` (confirms on) with `mandatory` set, and wait
/// for the broker's answer.
async fn publish_confirmed(
    channel: &Channel,
    exchange: ShortString,
    routing_key: ShortString,
    body: &[u8],
) -> lapin::Result<Confirmation> {
    channel
        .basic_publish(
            exchange,
            routing_key,
            BasicPublishOptions {
                mandatory: true,
                ..Default::default()
            },
            body,
            BasicProperties::default().with_delivery_mode(PERSISTENT),
        )
        .await?
        .await
}

/// Why a publish failed at the AMQP level. A broker refusal (e.g.
/// `ACCESS_REFUSED` on an internal exchange, `NOT_FOUND` for a deleted
/// exchange) comes back as its reply text.
fn publish_error(e: lapin::Error, target: &str) -> anyhow::Error {
    match e.kind() {
        lapin::ErrorKind::ProtocolError(refusal) => {
            anyhow::anyhow!("refused: {}", refusal.get_message().as_str())
        }
        _ => anyhow::Error::new(e).context(format!("publishing to {target}")),
    }
}

/// How the broker answered a confirmed publish.
#[derive(Debug, PartialEq, Eq)]
enum PublishOutcome {
    /// Acked, and some queue took it.
    Confirmed,
    /// Returned: no queue matched (`mandatory`).
    Unroutable,
    /// NACKed: the broker couldn't take it (a queue full with
    /// `reject-publish`, a quorum queue without a quorum).
    Nacked,
}

impl From<Confirmation> for PublishOutcome {
    fn from(confirmation: Confirmation) -> Self {
        match confirmation {
            // RabbitMQ returns an unroutable message, then acks it.
            Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_)) => Self::Unroutable,
            Confirmation::Nack(None) => Self::Nacked,
            // `NotRequested` can't happen: the channel turned confirms on.
            Confirmation::Ack(None) | Confirmation::NotRequested => Self::Confirmed,
        }
    }
}

/// The publish's result: `Ok` when confirmed, otherwise why not, starting with
/// the kind of failure (the footer shows the start). How an unroutable
/// message is explained follows the exchange's type from the last listing.
fn publish_result(
    outcome: PublishOutcome,
    spec: &SubSpec,
    exchange_type: Option<&ExchangeType>,
    routing_key: &str,
) -> anyhow::Result<()> {
    match outcome {
        PublishOutcome::Confirmed => Ok(()),
        PublishOutcome::Nacked => anyhow::bail!("NACKed: the broker didn't take the message"),
        PublishOutcome::Unroutable => match (spec, exchange_type) {
            (SubSpec::Queue(q), _) => anyhow::bail!("unroutable: no queue named `{q}`"),
            (_, Some(ExchangeType::Fanout)) => {
                anyhow::bail!("unroutable: no queue is bound to the exchange")
            }
            (_, Some(ExchangeType::Headers)) => {
                anyhow::bail!("unroutable: no binding matches a message without headers")
            }
            _ if routing_key.is_empty() => {
                anyhow::bail!("unroutable: no queue bound for the empty key")
            }
            _ => anyhow::bail!("unroutable: no queue bound for key `{routing_key}`"),
        },
    }
}

/// Owns a tap's dedicated connection/channel/consumer so the stream stays alive.
/// Dropping it closes the connection, which deletes the exclusive spy queue.
struct TapState {
    // Kept alive for the life of the stream (dropped → spy queue auto-deletes).
    _connection: Connection,
    _channel: Channel,
    /// Keeps the spy queue out of the destination list.
    _spy_queue: TapQueueGuard,
    consumer: Consumer,
    /// The exchange name reported as the event source.
    exchange: String,
}

/// Open a dedicated, non-destructive tap on `exchange` and return its event
/// stream. Declares a temporary spy queue, notes it in `spy_queues`, binds it
/// with `binding_key`, and consumes the copies routed to it.
async fn open_exchange_tap(
    uri: AMQPUri,
    props: ConnectionProperties,
    spy_queues: &Arc<TapQueues>,
    exchange: String,
    binding_key: String,
) -> anyhow::Result<BrokerEventStream> {
    // Each lapin call attaches human context with `.context`/`.with_context`,
    // keeping the lapin error as the source so its AMQP reply code (e.g.
    // NOT_FOUND vs ACCESS_REFUSED) survives in the `{:#}` chain rather than being
    // flattened — and the messages name what we were doing, not a guessed cause.
    //
    // Checked before the tap's connection opens, so an over-long name or key
    // fails the tap with nothing to clean up.
    let exchange_name = short_string("exchange name", &exchange)?;
    let binding_key = short_string("binding key", &binding_key)?;
    super::install_tls_provider();
    let connection = Connection::connect_uri(uri, props)
        .await
        .context("opening tap connection")?;
    let channel = connection
        .create_channel()
        .await
        .context("opening tap channel")?;

    // Passively declare the exchange first so a missing/inaccessible exchange
    // fails here (with the broker's reply code in the chain) instead of as an
    // opaque bind error. In passive mode the broker only checks existence and
    // ignores `kind`, so the `Topic` placeholder never conflicts with the
    // exchange's real type.
    channel
        .exchange_declare(
            exchange_name.clone(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("tapping exchange `{exchange}`"))?;

    // A temporary spy queue: server-named (empty name → broker generates one),
    // `exclusive` (only this connection may use it) and `auto_delete` (removed
    // when the connection drops). Never durable — it must not outlive the tail.
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("declaring the spy queue")?;
    // The broker's name for it, already a short string.
    let queue_name = queue.name().clone();
    // Noted before the bind or consume can fail, so the queue a failed tap
    // leaves behind is hidden too, for the same retention after the failure.
    let spy_queue = spy_queues.register(queue_name.as_str());

    // Bind the spy queue: the broker now routes a COPY of every matching message
    // to it, on top of whatever real queues are bound. Non-destructive by
    // construction — we never consume from a pre-existing queue.
    channel
        .queue_bind(
            queue_name.clone(),
            exchange_name,
            binding_key.clone(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| {
            format!("binding the spy queue to exchange `{exchange}` (key `{binding_key}`)")
        })?;

    // Consume with auto-ack: acking only discards our own copy from the spy
    // queue (which we drop on close anyway), so it never touches real queues.
    let consumer = channel
        .basic_consume(
            queue_name,
            // Ours, and well under the limit.
            format!("keyhole-{}", super::next_conn_seq())
                .as_str()
                .into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("consuming the spy queue")?;

    let state = TapState {
        _connection: connection,
        _channel: channel,
        _spy_queue: spy_queue,
        consumer,
        exchange,
    };
    let stream = stream::unfold(state, |mut st| async move {
        match st.consumer.next().await {
            Some(Ok(delivery)) => {
                let event = delivery_to_event(&st.exchange, delivery);
                Some((event, st))
            }
            // `None` = consumer cancelled / channel closed; `Some(Err)` = a
            // stream-level error. Either way the tap ends.
            ended => {
                if let Some(Err(e)) = ended {
                    tracing::debug!(error = %e, exchange = %st.exchange, "rabbitmq tap ended");
                }
                None
            }
        }
    });
    Ok(Box::pin(stream))
}

/// Build a [`BrokerEvent`] from a received delivery. Factored out of the lapin
/// [`Delivery`] (which is awkward to construct in tests) so the payload/meta
/// logic is unit-testable via [`build_event`].
fn delivery_to_event(exchange: &str, delivery: Delivery) -> BrokerEvent {
    build_event(
        exchange,
        delivery.routing_key.as_str(),
        delivery.redelivered,
        delivery.data,
    )
}

/// Assemble the event: the exchange is the observed source; the per-message
/// routing key (and a redelivery flag, when set) ride along as metadata, the
/// same way a Redis stream entry carries its id.
fn build_event(exchange: &str, routing_key: &str, redelivered: bool, data: Vec<u8>) -> BrokerEvent {
    let mut meta = vec![("routing_key".to_string(), routing_key.to_string())];
    if redelivered {
        meta.push(("redelivered".to_string(), "true".to_string()));
    }
    BrokerEvent {
        ts: OffsetDateTime::now_utc(),
        source: exchange.to_string(),
        payload: Payload::classify(data),
        meta,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The body bytes a test connection's peek fetches per message.
    const PREVIEW: usize = 4096;

    fn profile(tls: bool) -> RabbitmqProfile {
        RabbitmqProfile {
            name: "rmq".into(),
            host: "rabbit.example.com".into(),
            port: if tls { 5671 } else { 5672 },
            vhost: "/".into(),
            username: Some("user".into()),
            password: None,
            tls,
            management_url: None,
            management_username: None,
            management_password: None,
        }
    }

    #[test]
    fn the_uri_logs_in_as_the_profile_over_its_scheme() {
        let conn = RabbitmqConnection::new(
            profile(true),
            Some("p@ss/word".into()),
            None,
            PREVIEW,
            Arc::default(),
        );
        let uri = conn.uri().unwrap();
        assert_eq!(uri.scheme, AMQPScheme::AMQPS);
        assert_eq!(uri.authority.host, "rabbit.example.com");
        assert_eq!(uri.authority.port, 5671);
        assert_eq!(uri.authority.userinfo.username, "user");
        assert_eq!(uri.authority.userinfo.password, "p@ss/word");
        assert_eq!(uri.vhost, "/");

        // No login at all: guest/guest.
        let mut p = profile(false);
        p.username = None;
        p.vhost = "my/host".into();
        let uri = amqp_uri(&p, None).unwrap();
        assert_eq!(uri.scheme, AMQPScheme::AMQP);
        assert_eq!(uri.authority.userinfo.username, "guest");
        assert_eq!(uri.authority.userinfo.password, "guest");
        assert_eq!(uri.vhost, "my/host");
    }

    /// What lapin made of the URL string keyhole built for `p` before it
    /// built lapin's URI struct itself.
    fn parsed_from_url(p: &RabbitmqProfile, password: Option<&str>) -> AMQPUri {
        use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
        let mut url =
            crate::broker::amqp_base_url(p.tls, &p.host, p.port, p.username.as_deref(), password);
        url.push('/');
        url.push_str(&utf8_percent_encode(&p.vhost, NON_ALPHANUMERIC).to_string());
        url.parse().unwrap()
    }

    #[test]
    fn the_uri_connects_where_and_as_whom_the_url_did_for_ordinary_profiles() {
        let logins = [None, Some(""), Some("user"), Some("p@ss/wörd:%")];
        for tls in [false, true] {
            for host in [
                "rabbit.example.com",
                "Rabbit.Example.COM",
                "10.0.0.5",
                "localhost",
            ] {
                for vhost in ["/", "", "prod", "my/host", "café", "a b"] {
                    for username in logins {
                        for password in logins {
                            let mut p = profile(tls);
                            p.host = host.into();
                            p.vhost = vhost.into();
                            p.username = username.map(Into::into);
                            assert_eq!(
                                amqp_uri(&p, password).unwrap(),
                                parsed_from_url(&p, password),
                                "{host} {vhost:?} {username:?} {password:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn the_uri_keeps_an_ipv6_host_the_url_turned_into_localhost() {
        for (host, address) in [
            ("2001:db8::1", "2001:db8::1"),
            ("::1", "::1"),
            ("[::1]", "::1"),
        ] {
            let mut p = profile(false);
            p.host = host.into();
            assert_eq!(amqp_uri(&p, None).unwrap().authority.host, address);
            // So the login went to a local broker instead.
            assert_eq!(parsed_from_url(&p, None).authority.host, "localhost");
        }
    }

    #[test]
    fn the_uri_keeps_a_dot_vhost_the_url_dropped() {
        for vhost in [".", ".."] {
            let mut p = profile(false);
            p.vhost = vhost.into();
            assert_eq!(amqp_uri(&p, None).unwrap().vhost, vhost);
            // As a dot segment, which left the empty vhost.
            assert_eq!(parsed_from_url(&p, None).vhost, "");
        }
    }

    #[test]
    fn a_slash_in_the_host_stays_in_the_host() {
        // In the URL, everything after the `/` became the vhost: 307 bytes
        // here, which lapin panicked on.
        let mut p = profile(false);
        p.host = format!("127.0.0.1/{}", "v".repeat(300));
        let uri = amqp_uri(&p, None).unwrap();
        assert_eq!(uri.authority.host, p.host);
        assert_eq!(uri.vhost, "/");
    }

    #[test]
    fn short_string_takes_up_to_255_bytes() {
        let ok = |s: &str| short_string("routing key", s).map(|s| s.as_str().to_owned());
        let err = |s: &str| short_string("routing key", s).unwrap_err().to_string();
        let too_long = "routing key too long, max 255 bytes";
        assert_eq!(ok("").unwrap(), "");
        let max = "k".repeat(255);
        assert_eq!(ok(&max).unwrap(), max);
        assert_eq!(err(&"k".repeat(256)), too_long);

        // The limit counts bytes, not characters. 128 two-byte chars are
        // under 255 characters but 256 bytes.
        let wide = "é".repeat(128);
        assert!(wide.chars().count() < 255);
        assert_eq!(err(&wide), too_long);
        // A multibyte char that ends exactly on byte 255 fits; one more byte
        // doesn't.
        for last in ["é", "€", "🦀"] {
            let fits = format!("{}{last}", "k".repeat(255 - last.len()));
            assert_eq!(fits.len(), 255);
            assert_eq!(ok(&fits).unwrap(), fits, "{last}");
            let over = format!("k{fits}");
            assert_eq!(err(&over), too_long, "{last}");
        }

        // The message names what was too long.
        assert_eq!(
            short_string("vhost", &max.repeat(2))
                .unwrap_err()
                .to_string(),
            "vhost too long, max 255 bytes"
        );
    }

    #[tokio::test]
    async fn an_overlong_vhost_fails_the_connect_without_dialling() {
        let mut p = profile(false);
        p.vhost = "v".repeat(256);
        // The host doesn't resolve, so getting this error proves the check
        // came first.
        let mut conn = RabbitmqConnection::new(p, None, None, PREVIEW, Arc::default());
        let err = conn.connect().await.unwrap_err();
        assert_eq!(format!("{err:#}"), "vhost too long, max 255 bytes");
        let err = match conn.subscribe(tap_spec("ex", "#")).await {
            Ok(_) => panic!("a tap on the vhost can't open"),
            Err(e) => e,
        };
        assert_eq!(format!("{err:#}"), "vhost too long, max 255 bytes");
    }

    fn tap_spec(exchange: &str, binding_key: &str) -> SubSpec {
        SubSpec::Exchange {
            exchange: exchange.into(),
            binding_key: binding_key.into(),
        }
    }

    #[tokio::test]
    async fn an_overlong_tap_exchange_or_binding_key_fails_before_connecting() {
        // Not connected, and the host doesn't resolve: the tap fails on the
        // name or key before it dials.
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let long = "x".repeat(256);
        for (spec, expected) in [
            (
                tap_spec(&long, "#"),
                "exchange name too long, max 255 bytes",
            ),
            (tap_spec("ex", &long), "binding key too long, max 255 bytes"),
        ] {
            let err = match conn.subscribe(spec).await {
                Ok(_) => panic!("{expected}: the tap can't open"),
                Err(e) => e,
            };
            assert_eq!(format!("{err:#}"), expected);
        }
    }

    #[tokio::test]
    async fn an_overlong_publish_name_or_key_fails_before_the_channel() {
        // Not connected, so a check that came after the channel would fail
        // with "connection is not established" instead.
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let long = "x".repeat(256);
        let publish = |spec: SubSpec, routing_key: &str| PublishReq {
            spec,
            routing_key: routing_key.into(),
            body: b"m".to_vec(),
        };
        for (req, expected) in [
            (
                publish(tap_spec("ex", "#"), &long),
                "routing key too long, max 255 bytes",
            ),
            (
                publish(tap_spec(&long, "#"), "k"),
                "exchange name too long, max 255 bytes",
            ),
            (
                publish(SubSpec::Queue(long.clone()), &long),
                "queue name too long, max 255 bytes",
            ),
        ] {
            let err = conn.publish(req).await.unwrap_err();
            assert_eq!(format!("{err:#}"), expected);
        }
        assert!(conn.publish_channel.is_none());
    }

    #[test]
    fn build_event_classifies_payload_and_attaches_routing_key() {
        let ev = build_event("orders", "order.created", false, b"hello".to_vec());
        assert_eq!(ev.source, "orders");
        assert_eq!(ev.payload, Payload::Utf8("hello".into()));
        assert_eq!(ev.meta("routing_key"), Some("order.created"));
        // No redelivery flag on a first delivery.
        assert_eq!(ev.meta("redelivered"), None);

        // JSON bodies are recognised as JSON.
        let j = build_event("ex", "k", false, br#"{"a":1}"#.to_vec());
        assert!(matches!(j.payload, Payload::Json(_)));

        // Non-UTF-8 bodies survive as binary (base64 when displayed/recorded).
        let b = build_event("ex", "k", true, vec![0x00, 0xff]);
        assert!(matches!(b.payload, Payload::Binary(_)));
        // The redelivery flag is surfaced when set.
        assert_eq!(b.meta("redelivered"), Some("true"));
    }

    #[tokio::test]
    async fn default_binding_key_tap_warns_about_non_topic_exchanges() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        // The catch-all `#` default → advisory that direct exchanges
        // need an explicit key (else the tail is silently empty).
        let notice = conn
            .tail_notice(&SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "#".into(),
            })
            .await
            .expect("default `#` key should produce a notice");
        assert!(notice.contains("direct exchange"), "{notice}");
        // An explicit binding key → the user chose it, so no advisory.
        let none = conn
            .tail_notice(&SubSpec::Exchange {
                exchange: "ex".into(),
                binding_key: "orders.*".into(),
            })
            .await;
        assert!(none.is_none());
    }

    /// The `Authorization` header the management probe sends for `profile`,
    /// whose API is pointed at a stub that answers the probe.
    async fn management_login(
        mut profile: RabbitmqProfile,
        management_password: Option<&str>,
    ) -> String {
        use crate::broker::http::stub::{header, stub_server};
        let (base_url, server) = stub_server(vec![
            (200, r#"{"name":"u","tags":["management"]}"#.into()),
            (200, r#"{"rabbitmq_version":"4.4.0"}"#.into()),
        ]);
        profile.management_url = Some(base_url);
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("amqp-pw".into()),
            management_password.map(str::to_string),
            PREVIEW,
            Arc::default(),
        );
        conn.mgmt.run_probe().await;
        assert!(conn.mgmt.probe().is_some(), "{:?}", conn.mgmt.error());
        let requests = server.join().unwrap();
        header(&requests[0], "authorization").unwrap().to_string()
    }

    #[tokio::test]
    async fn management_logs_in_as_the_amqp_user_unless_overridden() {
        use crate::broker::http::basic_auth;
        // The AMQP username, with the password `start_connect` resolved for the
        // management API.
        assert_eq!(
            management_login(profile(false), Some("pw")).await,
            basic_auth("user", Some("pw"))
        );
        // `management_username` overrides the AMQP username.
        let mut p = profile(false);
        p.management_username = Some("monitor".into());
        assert_eq!(
            management_login(p, Some("pw")).await,
            basic_auth("monitor", Some("pw"))
        );
        // No credentials at all: lapin's `guest`/`guest`, like the AMQP login.
        let mut anon = profile(false);
        anon.username = None;
        assert_eq!(
            management_login(anon, None).await,
            basic_auth("guest", Some("guest"))
        );
    }

    /// A Phase 0 fixture from `rabbitmq/testdata/` (real broker replies).
    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!("rabbitmq/testdata/", $name, ".json")).to_string()
        };
    }

    /// A connection whose management API is a stub answering `replies` in
    /// order, after the probe's two (`stats_disabled` as the overview says).
    /// The join handle yields the raw requests.
    async fn listing_conn(
        stats_disabled: bool,
        replies: Vec<(u16, String)>,
    ) -> (RabbitmqConnection, std::thread::JoinHandle<Vec<String>>) {
        probed_conn("4.4.0", stats_disabled, replies).await
    }

    /// [`listing_conn`] for a broker reporting `version`.
    async fn probed_conn(
        version: &str,
        stats_disabled: bool,
        replies: Vec<(u16, String)>,
    ) -> (RabbitmqConnection, std::thread::JoinHandle<Vec<String>>) {
        probed_as(
            fixture!("whoami.administrator.v4"),
            version,
            stats_disabled,
            replies,
        )
        .await
    }

    /// [`probed_conn`] for the user `whoami` describes.
    async fn probed_as(
        whoami: String,
        version: &str,
        stats_disabled: bool,
        replies: Vec<(u16, String)>,
    ) -> (RabbitmqConnection, std::thread::JoinHandle<Vec<String>>) {
        use crate::broker::http::stub::stub_server;
        let mut all = vec![
            (200, whoami),
            (
                200,
                format!(r#"{{"rabbitmq_version":"{version}","disable_stats":{stats_disabled}}}"#),
            ),
        ];
        all.extend(replies);
        let (base_url, server) = stub_server(all);
        let mut p = profile(false);
        p.management_url = Some(base_url);
        let mut conn = RabbitmqConnection::new(p, None, Some("pw".into()), PREVIEW, Arc::default());
        conn.mgmt.run_probe().await;
        assert!(conn.mgmt.probe().is_some(), "{:?}", conn.mgmt.error());
        (conn, server)
    }

    fn find<'a>(list: &'a DestinationList, name: &str, kind: DestKind) -> &'a DestinationInfo {
        list.items
            .iter()
            .find(|d| d.name == name && d.kind == kind)
            .unwrap_or_else(|| panic!("{name} listed"))
    }

    #[tokio::test]
    async fn lists_queues_then_exchanges_without_the_default_exchange() {
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("queues.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();

        // 17 queues, then the 13 exchanges less the default one.
        assert_eq!(list.items.len(), 17 + 12);
        assert!(!list.truncated && !list.stats_disabled);
        let first_exchange = list
            .items
            .iter()
            .position(|d| d.kind == DestKind::Exchange)
            .unwrap();
        assert_eq!(first_exchange, 17, "queues come first");
        assert!(list.items[first_exchange..]
            .iter()
            .all(|d| d.kind == DestKind::Exchange));
        assert!(
            list.items.iter().all(|d| !d.name.is_empty()),
            "the default exchange can't be bound, so it isn't listed"
        );

        let busy = find(&list, "spike.consumed", DestKind::Queue);
        assert_eq!(busy.queue_type, Some(QueueType::Classic));
        assert_eq!(busy.exchange_type, None);
        assert_eq!(
            (busy.messages, busy.ready, busy.unacked, busy.consumers),
            (Some(1895), Some(1875), Some(20), Some(2))
        );
        assert_eq!(busy.publish_rate, Some(9.8));
        // `type` decides the kind (4.x puts `x-queue-type` in every queue's
        // arguments).
        let quorum = find(&list, "spike.quorum", DestKind::Queue);
        assert_eq!(quorum.queue_type, Some(QueueType::Quorum));
        let stream = find(&list, "spike.stream", DestKind::Queue);
        assert_eq!(stream.queue_type, Some(QueueType::Stream));

        let topic = find(&list, "spike.topic", DestKind::Exchange);
        assert_eq!(topic.exchange_type, Some(ExchangeType::Topic));
        assert_eq!(topic.publish_rate, Some(9.8), "publish_in, not publish_out");
        assert_eq!(topic.messages, None);
        let headers = find(&list, "amq.headers", DestKind::Exchange);
        assert_eq!(headers.exchange_type, Some(ExchangeType::Headers));
        // `amq.*` stays, internal ones marked.
        let trace = find(&list, "amq.rabbitmq.trace", DestKind::Exchange);
        assert!(trace.internal);
        assert!(!find(&list, "amq.topic", DestKind::Exchange).internal);
    }

    #[tokio::test]
    async fn the_listing_leaves_out_this_sessions_tap_queues() {
        let queues =
            r#"{"items":[{"name":"amq.gen-ours"},{"name":"amq.gen-theirs"}],"page_count":1}"#;
        let (mut conn, server) = listing_conn(
            false,
            vec![(200, queues.into()), (200, fixture!("exchanges.v4"))],
        )
        .await;
        let _tap = conn.tap_queues.register("amq.gen-ours");
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        let queues: Vec<_> = list
            .items
            .iter()
            .filter(|d| d.kind == DestKind::Queue)
            .map(|d| d.name.as_str())
            .collect();
        assert_eq!(queues, ["amq.gen-theirs"]);
    }

    #[tokio::test]
    async fn lists_with_metrics_disabled_carry_no_counts_or_rates() {
        let (mut conn, server) = listing_conn(
            true,
            vec![
                (200, fixture!("queues.no-metrics.v4")),
                (200, fixture!("exchanges.no-metrics.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert!(list.stats_disabled, "the probe's overview said why");
        assert_eq!(list.items.len(), 17 + 12);
        for d in &list.items {
            assert_eq!(
                (d.messages, d.ready, d.unacked, d.consumers, d.publish_rate),
                (None, None, None, None, None),
                "{}",
                d.name
            );
        }
        // The kinds still come through.
        let stream = find(&list, "spike.stream", DestKind::Queue);
        assert_eq!(stream.queue_type, Some(QueueType::Stream));
    }

    #[tokio::test]
    async fn listing_before_a_good_probe_fails_fast_and_names_the_url() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let err = conn.list_destinations(false).await.unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "management API unavailable at http://rabbit.example.com:15672: the management \
             API has not been probed yet"
        );
    }

    #[tokio::test]
    async fn a_refresh_reprobes_a_failed_api_and_the_periodic_one_does_not() {
        use crate::broker::http::stub::stub_server;
        let (base_url, server) = stub_server(vec![
            (200, fixture!("whoami.administrator.v3")),
            (200, r#"{"rabbitmq_version":"3.13.7"}"#.to_string()),
            (200, fixture!("queues.empty-vhost.v4")),
            (200, fixture!("queues.empty-vhost.v4")),
        ]);
        let mut p = profile(false);
        p.management_url = Some(base_url);
        let mut conn = RabbitmqConnection::new(p, None, None, PREVIEW, Arc::default());
        assert!(conn.mgmt.error().is_some(), "not probed yet");

        // Without `reprobe` the recorded error comes straight back.
        conn.list_destinations(false).await.unwrap_err();
        assert!(conn.mgmt.error().is_some());
        // With it, the API is probed again and then listed.
        let list = conn.list_destinations(true).await.expect("lists");
        assert!(list.items.is_empty() && !list.truncated);
        assert_eq!(server.join().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn an_api_that_stops_answering_fails_fast_until_a_reprobe() {
        // The stub answers the probe, then stops listening.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let err = format!("{:#}", conn.list_destinations(false).await.unwrap_err());
        assert!(err.contains("could not reach the management API"), "{err}");
        assert!(
            conn.mgmt.error().is_some(),
            "the next listing won't wait on the timeouts again"
        );
        let again = format!("{:#}", conn.list_destinations(false).await.unwrap_err());
        assert!(
            again.starts_with("management API unavailable at"),
            "{again}"
        );
    }

    #[tokio::test]
    async fn a_refused_listing_keeps_the_api_usable() {
        let (mut conn, server) =
            listing_conn(false, vec![(401, fixture!("error.401.vhost-access.v3"))]).await;
        let err = format!("{:#}", conn.list_destinations(false).await.unwrap_err());
        server.join().unwrap();
        assert!(err.contains("no permissions on vhost"), "{err}");
        assert!(conn.mgmt.client().is_ok(), "a refusal is cheap to retry");
    }

    #[tokio::test]
    async fn tap_notice_follows_the_listed_exchange_type() {
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("queues.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        let tap = |name: &str, key: &str| SubSpec::Exchange {
            exchange: name.into(),
            binding_key: key.into(),
        };
        // `#` fits a topic or fanout exchange, and a headers exchange ignores
        // it: the tap gets every message.
        assert_eq!(conn.tail_notice(&tap("spike.topic", "#")).await, None);
        assert_eq!(conn.tail_notice(&tap("spike.fanout", "#")).await, None);
        assert_eq!(conn.tail_notice(&tap("spike.headers", "#")).await, None);
        // A known direct exchange gets a specific advisory.
        let direct = conn.tail_notice(&tap("spike.direct", "#")).await.unwrap();
        assert!(direct.contains("direct exchange"), "{direct}");
        // An exchange the listing didn't include: the type is unknown.
        let unknown = conn.tail_notice(&tap("elsewhere", "#")).await.unwrap();
        assert!(
            unknown.contains("direct exchange it matches only"),
            "{unknown}"
        );
        // An explicit key is the user's choice: no advisory.
        assert_eq!(conn.tail_notice(&tap("spike.direct", "k")).await, None);
    }

    #[tokio::test]
    async fn only_3x_quorum_queues_with_a_limit_count_peeks_toward_it() {
        let limits = |list: &DestinationList| -> Vec<(String, Option<i64>)> {
            list.items
                .iter()
                .filter(|d| d.kind == DestKind::Queue)
                .map(|d| (d.name.clone(), d.peek_delivery_limit))
                .filter(|(_, limit)| limit.is_some())
                .collect()
        };

        // 3.x: a requeue raises `x-delivery-count`, so the queues with a limit
        // by argument or by policy are marked. The default quorum queue has no
        // limit on 3.x, and classic queues have none at all.
        let (mut conn, server) = probed_conn(
            "3.13.7",
            false,
            vec![
                (200, fixture!("queues.v3")),
                (200, fixture!("exchanges.v3")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert_eq!(
            limits(&list),
            [
                ("spike.quorum.limit".to_string(), Some(3)),
                ("spike.quorum.limit.dlx".to_string(), Some(3)),
                ("spike.quorum.policy".to_string(), Some(5)),
                ("spike.quorum.reject".to_string(), Some(3)),
            ]
        );

        // 4.x: `ack_requeue_true` raises only `x-acquired-count`, so no peek
        // counts toward a limit, the default 20 included.
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("queues.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert!(limits(&list).is_empty(), "{:?}", limits(&list));

        // A negative or zero limit on 3.x drops a message on its first
        // requeue, so it's marked too.
        let (mut conn, server) = probed_conn(
            "3.13.7",
            false,
            vec![
                (200, fixture!("queues.negative-delivery-limit.v3")),
                (200, fixture!("exchanges.v3")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert_eq!(
            limits(&list),
            [
                ("spike.quorum.unlimited".to_string(), Some(-1)),
                ("spike.quorum.zero".to_string(), Some(0)),
            ]
        );

        // On 4.x a negative limit is reported as `unlimited`. That still marks
        // the broker as 4.x when its version is unknown (which is otherwise
        // assumed to count).
        let (mut conn, server) = probed_conn(
            "",
            false,
            vec![
                (200, fixture!("queues.negative-delivery-limit.v4")),
                (200, fixture!("exchanges.v4")),
            ],
        )
        .await;
        let list = conn.list_destinations(false).await.expect("lists");
        server.join().unwrap();
        assert!(limits(&list).is_empty(), "{:?}", limits(&list));
    }

    #[test]
    fn a_3x_delivery_limit_is_the_lower_of_argument_and_policy() {
        let queue = |json: &str| -> QueueInfo { serde_json::from_str(json).unwrap() };
        let both = queue(
            r#"{"name":"q","type":"quorum","arguments":{"x-delivery-limit":7},
                "effective_policy_definition":{"delivery-limit":4}}"#,
        );
        assert_eq!(configured_delivery_limit(&both), Some(4));
        // 3.x accepts a negative argument and drops on the first requeue, as
        // it does at 0. An Erlang empty proplist (`[]`) is no policy.
        for limit in [0, -1] {
            let q = queue(&format!(
                r#"{{"name":"q","type":"quorum","arguments":{{"x-delivery-limit":{limit}}},
                    "effective_policy_definition":[]}}"#
            ));
            assert_eq!(configured_delivery_limit(&q), Some(limit));
        }
        // A negative argument beats a policy too.
        let negative = queue(
            r#"{"name":"q","type":"quorum","arguments":{"x-delivery-limit":-1},
                "effective_policy_definition":{"delivery-limit":5}}"#,
        );
        assert_eq!(configured_delivery_limit(&negative), Some(-1));
        let none = queue(r#"{"name":"q","type":"quorum"}"#);
        assert_eq!(configured_delivery_limit(&none), None);
        // A 3.x broker counts it; an unknown version is assumed to.
        assert_eq!(queue_info(both.clone(), true).peek_delivery_limit, Some(4));
        assert_eq!(queue_info(both, false).peek_delivery_limit, None);
    }

    // -- peek ------------------------------------------------------------------

    /// The events a `/get` fixture maps to, peeked from queue `q`.
    fn peeked(json: &str) -> Vec<BrokerEvent> {
        let messages: Vec<GetMessage> = serde_json::from_str(json).expect("get reply parses");
        messages.into_iter().map(|m| peek_event("q", m)).collect()
    }

    #[test]
    fn peeked_messages_carry_their_bodies_properties_and_headers() {
        for json in [fixture!("get.classic.v3"), fixture!("get.classic.v4")] {
            let events = peeked(&json);
            assert_eq!(events.len(), 3);
            let [json_body, binary, plain] = &events[..] else {
                unreachable!()
            };
            for ev in &events {
                assert_eq!(ev.source, "q", "the source is the queue");
                assert_eq!(ev.meta("exchange"), Some("(default)"));
                assert_eq!(ev.meta("routing_key"), Some("spike.classic"));
                assert_eq!(ev.meta("truncated"), None);
            }

            // A JSON body with every property and typed headers.
            assert_eq!(
                json_body.payload,
                Payload::Json(
                    r#"{"order_id":42,"status":"created","items":[{"sku":"A-1","qty":2}],"note":"café"}"#
                        .into()
                )
            );
            for (key, value) in [
                ("redelivered", "true"),
                ("content_type", "application/json"),
                ("message_id", "msg-0001"),
                ("correlation_id", "corr-abc"),
                ("delivery_mode", "2 (persistent)"),
                ("content_encoding", "utf-8"),
                ("priority", "5"),
                ("timestamp", "2025-11-07T07:20:00Z"),
                ("type", "order.created"),
                ("app_id", "keyhole-spike"),
                ("user_id", "keyhole"),
                ("reply_to", "spike.replies"),
                ("expiration", "3600000"),
                // Header values: a string as itself, the rest as compact JSON.
                ("headers.x-source", "spike"),
                ("headers.x-attempt", "1"),
                ("headers.x-flag", "true"),
                ("headers.x-ratio", "0.5"),
                ("headers.x-tags", r#"["a","b"]"#),
                ("headers.x-nested", r#"{"k":"v"}"#),
            ] {
                assert_eq!(json_body.meta(key), Some(value), "{key}");
            }

            // A non-UTF-8 body stays binary.
            assert_eq!(
                binary.payload,
                Payload::Binary(b"\x00\x01\x02\xff\xfe\x80keyhole\xc3(".to_vec())
            );
            assert_eq!(binary.meta("delivery_mode"), Some("1 (transient)"));
            assert_eq!(binary.meta("redelivered"), Some("false"));

            // `properties: []`: only how the message arrived.
            assert_eq!(
                plain.payload,
                Payload::Utf8("plain text body, no properties".into())
            );
            let keys: Vec<&str> = plain.meta.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, ["exchange", "routing_key", "redelivered"]);
        }
    }

    #[test]
    fn a_truncated_body_says_how_much_is_shown() {
        for json in [fixture!("get.truncated.v3"), fixture!("get.truncated.v4")] {
            let ev = &peeked(&json)[0];
            // The first 10 of 81 bytes: no longer valid JSON, so plain text.
            assert_eq!(ev.payload, Payload::Utf8(r#"{"order_id"#.into()));
            assert_eq!(ev.meta("truncated"), Some("showing 10 of 81 bytes"));
        }

        // A cut through a multi-byte character drops its first half, so the
        // body still reads as text: "caf" plus half of "é", of 5 bytes.
        let cut = peeked(
            r#"[{"payload":"Y2Fmww==","payload_encoding":"base64","payload_bytes":5,
                 "redelivered":false,"exchange":"ex","routing_key":"k","properties":[]}]"#,
        );
        assert_eq!(cut[0].payload, Payload::Utf8("caf".into()));
        assert_eq!(cut[0].meta("truncated"), Some("showing 3 of 5 bytes"));
        assert_eq!(cut[0].meta("exchange"), Some("ex"));
    }

    #[test]
    fn dead_lettered_and_requeued_messages_show_their_headers() {
        for json in [
            fixture!("get.dead-lettered.v3"),
            fixture!("get.dead-lettered.v4"),
        ] {
            let ev = &peeked(&json)[0];
            assert_eq!(ev.meta("exchange"), Some("spike.dlx"));
            assert_eq!(
                ev.meta("headers.x-first-death-reason"),
                Some("delivery_limit")
            );
            let death = ev.meta("headers.x-death").expect("x-death");
            let parsed: serde_json::Value = serde_json::from_str(death).expect("compact JSON");
            assert_eq!(parsed[0]["reason"], "delivery_limit");
            assert!(!death.contains('\n'), "compact: {death}");
        }
        // The count a requeue raises: `x-delivery-count` on 3.x,
        // `x-acquired-count` on 4.x.
        let v3 = peeked(&fixture!("get.quorum.requeued.v3"));
        assert!(v3.iter().all(|e| e.meta("redelivered") == Some("true")));
        assert_eq!(v3[2].meta("headers.x-delivery-count"), Some("1"));
        let v4 = peeked(&fixture!("get.quorum.requeued.v4"));
        assert_eq!(v4[2].meta("headers.x-acquired-count"), Some("1"));
        assert_eq!(v4[2].meta("headers.x-delivery-count"), None);
    }

    fn peek_req(spec: SubSpec, mode: PeekMode) -> PeekReq {
        PeekReq {
            spec,
            mode,
            limit: 200,
        }
    }

    fn queue_peek(mode: PeekMode) -> PeekReq {
        peek_req(SubSpec::Queue("spike.classic".into()), mode)
    }

    #[tokio::test]
    async fn each_peek_mode_sends_its_ackmode() {
        use crate::broker::http::stub::header;
        for (mode, ackmode) in [
            (PeekMode::Browse, "ack_requeue_true"),
            (PeekMode::Destructive, "ack_requeue_false"),
        ] {
            let (mut conn, server) =
                listing_conn(false, vec![(200, fixture!("get.classic.v4"))]).await;
            let events = conn.peek(queue_peek(mode)).await.expect("peeks");
            assert_eq!(events.len(), 3);
            let raw = server.join().unwrap().pop().unwrap();
            // Every non-alphanumeric byte of the vhost and queue is encoded.
            assert!(
                raw.starts_with("POST /api/queues/%2F/spike%2Eclassic/get "),
                "{raw}"
            );
            assert_eq!(header(&raw, "connection"), Some("close"));
            let body: serde_json::Value =
                serde_json::from_str(raw.split("\r\n\r\n").nth(1).unwrap()).unwrap();
            // The batch limit, and bodies capped at the preview size.
            assert_eq!(
                body,
                serde_json::json!({
                    "count": 200, "ackmode": ackmode, "encoding": "base64", "truncate": PREVIEW
                })
            );
        }
    }

    #[tokio::test]
    async fn skip_mode_sends_no_request() {
        // The stub answers only the probe: a `/get` would find it gone.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        let events = conn.peek(queue_peek(PeekMode::Skip)).await.expect("skips");
        assert!(events.is_empty());
        assert_eq!(server.join().unwrap().len(), 2, "only the probe's requests");
    }

    #[tokio::test]
    async fn an_empty_queue_peeks_to_nothing() {
        let (mut conn, server) = listing_conn(false, vec![(200, fixture!("get.empty.v4"))]).await;
        let events = conn
            .peek(queue_peek(PeekMode::Browse))
            .await
            .expect("peeks");
        server.join().unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn a_stream_refusal_reads_as_the_stream_hint() {
        for json in [
            fixture!("error.400.stream-get.v3"),
            fixture!("error.400.stream-get.v4"),
        ] {
            let (mut conn, server) = listing_conn(false, vec![(400, json)]).await;
            let err = conn
                .peek(queue_peek(PeekMode::Browse))
                .await
                .expect_err("streams refuse /get");
            server.join().unwrap();
            assert_eq!(format!("{err:#}"), STREAM_PEEK_HINT);
            assert!(
                conn.mgmt.client().is_ok(),
                "a refusal leaves the API usable"
            );
        }
    }

    #[tokio::test]
    async fn a_refused_peek_gives_the_brokers_reason() {
        let (mut conn, server) = listing_conn(
            false,
            vec![(401, fixture!("error.401.queue-read-refused.v4"))],
        )
        .await;
        let err = conn.peek(queue_peek(PeekMode::Browse)).await.unwrap_err();
        server.join().unwrap();
        let err = format!("{err:#}");
        assert!(
            err.starts_with(
                "reading queue `spike.classic`: permission denied (HTTP 401): ACCESS_REFUSED"
            ),
            "{err}"
        );
        assert!(conn.mgmt.client().is_ok());
    }

    #[tokio::test]
    async fn a_peek_before_a_good_probe_fails_fast_and_an_unreachable_one_is_recorded() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let err = conn.peek(queue_peek(PeekMode::Browse)).await.unwrap_err();
        assert_eq!(
            format!("{err:#}"),
            "management API unavailable at http://rabbit.example.com:15672: the management \
             API has not been probed yet"
        );

        // The API answers the probe, then stops listening.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let err = format!(
            "{:#}",
            conn.peek(queue_peek(PeekMode::Browse)).await.unwrap_err()
        );
        assert!(err.starts_with("management API unavailable at"), "{err}");
        assert!(err.contains("could not reach the management API"), "{err}");
        assert!(conn.mgmt.error().is_some(), "later calls fail fast");
    }

    #[tokio::test]
    async fn only_a_queue_is_peeked() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let exchange = SubSpec::Exchange {
            exchange: "ex".into(),
            binding_key: "#".into(),
        };
        let err = conn
            .peek(peek_req(exchange, PeekMode::Browse))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("only a queue holds messages"),
            "{err}"
        );
    }

    // -- stats -----------------------------------------------------------------

    /// The stats built from an overview and (for a `monitoring` user) nodes
    /// fixture.
    fn stats_from(overview: &str, nodes: Option<&str>, cluster_wide: bool) -> RabbitmqStats {
        let overview: Overview = serde_json::from_str(overview).expect("overview parses");
        let nodes = nodes.map(|n| serde_json::from_str(n).expect("nodes parse"));
        rabbitmq_stats(overview, nodes, cluster_wide)
    }

    #[test]
    fn stats_carry_versions_rates_totals_and_node_health() {
        let v3 = stats_from(&fixture!("overview.v3"), Some(&fixture!("nodes.v3")), true);
        assert_eq!(v3.rabbitmq_version.as_deref(), Some("3.13.7"));
        assert_eq!(v3.erlang_version.as_deref(), Some("26.2.5.16"));
        assert_eq!(v3.cluster_name.as_deref(), Some("rabbit@19942ffdc832"));
        assert!(v3.cluster_wide && !v3.stats_disabled);
        assert_eq!(
            (v3.publish_rate, v3.deliver_rate, v3.ack_rate),
            (Some(9.6), Some(14.4), Some(4.8))
        );
        assert_eq!(
            (v3.messages, v3.messages_ready, v3.messages_unacked),
            (Some(1998), Some(1977), Some(21))
        );
        assert_eq!(
            (v3.queues, v3.connections, v3.consumers),
            (Some(18), Some(2), Some(4))
        );
        assert_eq!(
            v3.nodes,
            [NodeHealth {
                name: "rabbit@19942ffdc832".into(),
                running: Some(true),
                // 653803 ms.
                uptime: Some(653),
                mem_used: Some(201_236_480),
                mem_limit: Some(13_226_541_056),
                mem_alarm: Some(false),
                fd_used: Some(52),
                fd_total: Some(1_048_576),
                disk_free: Some(134_650_884_096),
                disk_free_limit: Some(50_000_000),
                disk_free_alarm: Some(false),
                partitions: Vec::new(),
            }]
        );
        assert!(!v3.has_alarm());

        let v4 = stats_from(&fixture!("overview.v4"), Some(&fixture!("nodes.v4")), true);
        assert_eq!(v4.rabbitmq_version.as_deref(), Some("4.4.0"));
        assert_eq!(v4.publish_rate, Some(9.8));
        assert_eq!(v4.consumers, Some(4));
        assert_eq!(v4.nodes.len(), 1);
        assert_eq!(v4.nodes[0].fd_used, Some(48));
    }

    #[test]
    fn a_management_user_gets_no_node_health_and_no_connection_total() {
        for (json, queues) in [
            (fixture!("overview.management.v3"), 18),
            (fixture!("overview.management.v4"), 17),
        ] {
            let stats = stats_from(&json, None, false);
            assert!(!stats.cluster_wide);
            assert!(stats.rabbitmq_version.is_some());
            assert!(stats.nodes.is_empty(), "/api/nodes needs monitoring");
            // The overview counts only this user's own connections, which
            // isn't a server total, and has no consumer count at all.
            assert_eq!((stats.connections, stats.consumers), (None, None));
            // The user's vhosts' totals and rates are still there.
            assert_eq!(stats.queues, Some(queues));
            assert!(stats.messages.is_some() && stats.publish_rate.is_some());
        }
    }

    #[test]
    fn with_metrics_disabled_stats_keep_versions_and_counts_only() {
        let stats = stats_from(
            &fixture!("overview.no-metrics.v4"),
            Some(&fixture!("nodes.no-metrics.v4")),
            true,
        );
        assert!(stats.stats_disabled);
        assert_eq!(stats.rabbitmq_version.as_deref(), Some("4.4.0"));
        assert_eq!(
            (stats.publish_rate, stats.deliver_rate, stats.ack_rate),
            (None, None, None)
        );
        assert_eq!(
            (stats.messages, stats.messages_ready, stats.messages_unacked),
            (None, None, None)
        );
        assert_eq!(
            (stats.queues, stats.connections, stats.consumers),
            (Some(17), Some(1), None)
        );
        let node = &stats.nodes[0];
        assert_eq!(node.running, Some(true));
        assert_eq!(
            (node.uptime, node.mem_used, node.fd_used, node.disk_free),
            (None, None, None, None)
        );
        assert!(!stats.has_alarm(), "unknown alarms aren't alarms");
    }

    #[test]
    fn a_fresh_node_has_versions_and_counts_but_no_figures_yet() {
        // Captured within a second of a new 3.13.7 node accepting logins:
        // `message_stats` and `queue_totals` are `{}` until traffic arrives,
        // and the node has no figures before its first stats emission.
        let stats = stats_from(
            &fixture!("overview.fresh.v3"),
            Some(&fixture!("nodes.fresh.v3")),
            true,
        );
        assert!(!stats.stats_disabled);
        assert_eq!(stats.rabbitmq_version.as_deref(), Some("3.13.7"));
        assert!(stats.erlang_version.is_some() && stats.cluster_name.is_some());
        assert_eq!(
            (stats.publish_rate, stats.deliver_rate, stats.ack_rate),
            (None, None, None)
        );
        assert_eq!(
            (stats.messages, stats.messages_ready, stats.messages_unacked),
            (None, None, None)
        );
        assert_eq!(
            (stats.queues, stats.connections, stats.consumers),
            (Some(0), Some(0), Some(0))
        );
        assert_eq!(
            stats.nodes,
            [NodeHealth {
                name: "rabbit@e71afc9e7c51".into(),
                running: Some(true),
                ..Default::default()
            }]
        );
        assert!(!stats.has_alarm());
    }

    #[tokio::test]
    async fn stats_read_the_overview_then_the_nodes_for_a_monitoring_user() {
        use crate::broker::http::stub::header;
        let (mut conn, server) = probed_as(
            fixture!("whoami.monitoring.v3"),
            "3.13.7",
            false,
            vec![(200, fixture!("overview.v3")), (200, fixture!("nodes.v3"))],
        )
        .await;
        let stats = conn.stats().await.expect("stats");
        let requests = server.join().unwrap();
        let stats = stats.rabbitmq().expect("RabbitMQ stats");
        assert_eq!(stats.rabbitmq_version.as_deref(), Some("3.13.7"));
        assert!(stats.cluster_wide);
        assert_eq!(stats.nodes.len(), 1);
        // Both trimmed with `columns=`, after the probe's two requests.
        let lines: Vec<&str> = requests[2..]
            .iter()
            .map(|r| r.lines().next().unwrap())
            .collect();
        assert!(
            lines[0].starts_with("GET /api/overview?columns=rabbitmq_version,"),
            "{lines:?}"
        );
        assert!(
            lines[1].starts_with("GET /api/nodes?columns=name,"),
            "{lines:?}"
        );
        assert_eq!(lines.len(), 2);
        assert!(header(&requests[2], "authorization").is_some());
    }

    #[tokio::test]
    async fn stats_skip_the_nodes_without_the_monitoring_tag() {
        // The stub answers the probe and one overview: a `/api/nodes` request
        // would find it gone and fail the refresh.
        let (mut conn, server) = probed_as(
            fixture!("whoami.management.v4"),
            "4.4.0",
            false,
            vec![(200, fixture!("overview.management.v4"))],
        )
        .await;
        let stats = conn.stats().await.expect("stats");
        assert_eq!(server.join().unwrap().len(), 3, "probe + overview only");
        let stats = stats.rabbitmq().unwrap();
        assert!(!stats.cluster_wide && stats.nodes.is_empty());
        assert_eq!(stats.connections, None);
    }

    #[tokio::test]
    async fn refused_nodes_leave_the_overview() {
        // The user lost its monitoring tag after the probe.
        let (mut conn, server) = listing_conn(
            false,
            vec![
                (200, fixture!("overview.v4")),
                (401, fixture!("error.401.not-monitoring.v4")),
            ],
        )
        .await;
        let stats = conn.stats().await.expect("the overview still counts");
        server.join().unwrap();
        let stats = stats.rabbitmq().unwrap();
        assert_eq!(stats.rabbitmq_version.as_deref(), Some("4.4.0"));
        assert!(stats.nodes.is_empty());
        assert!(conn.mgmt.client().is_ok());
    }

    #[tokio::test]
    async fn stats_before_a_good_probe_fail_fast_without_a_request() {
        // No stub at all: a request would have nowhere to go but the default
        // (unreachable) URL, and would take the timeouts to fail.
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let started = std::time::Instant::now();
        let err = conn.stats().await.unwrap_err();
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        // The whole chain is in the message: the stats reply carries only that.
        assert_eq!(
            err.to_string(),
            "management API unavailable at http://rabbit.example.com:15672: the management \
             API has not been probed yet"
        );
    }

    #[tokio::test]
    async fn an_unreachable_api_fails_later_stats_fast_with_the_same_message() {
        // The stub answers the probe, then stops listening.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let first = conn.stats().await.unwrap_err().to_string();
        assert!(
            first.starts_with("management API unavailable at"),
            "{first}"
        );
        assert!(
            first.contains("could not reach the management API"),
            "{first}"
        );
        assert!(conn.mgmt.error().is_some(), "marked unreachable");
        // The next refresh doesn't touch the network, and says the same, so
        // the footer isn't told twice.
        let again = conn.stats().await.unwrap_err().to_string();
        assert_eq!(again, first);
    }

    #[tokio::test]
    async fn a_refused_overview_keeps_the_api_usable() {
        let (mut conn, server) =
            listing_conn(false, vec![(401, fixture!("error.401.bad-credentials.v4"))]).await;
        let err = conn.stats().await.unwrap_err().to_string();
        server.join().unwrap();
        assert!(err.contains("rejected the credentials"), "{err}");
        assert!(conn.mgmt.client().is_ok(), "a refusal is cheap to retry");
    }

    #[tokio::test]
    async fn the_dashboard_needs_a_successful_probe() {
        let unprobed = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let caps = unprobed.capabilities();
        assert_eq!(caps.r#type, crate::broker::BrokerType::Rabbitmq);
        assert!(!caps.can_dashboard);

        let (probed, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let caps = probed.capabilities();
        assert!(caps.can_dashboard, "the probe succeeded");
        assert!(!caps.can_console);
    }

    #[tokio::test]
    async fn publishing_needs_no_management_api() {
        // It goes over AMQP: offered without a usable API (as before a good
        // probe) and with one.
        let unprobed = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        assert!(unprobed.mgmt.client().is_err());
        assert!(unprobed.capabilities().can_publish);
        let (probed, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        assert!(probed.capabilities().can_publish);
    }

    #[test]
    fn a_confirmation_reads_as_confirmed_or_nacked() {
        assert_eq!(
            PublishOutcome::from(Confirmation::Ack(None)),
            PublishOutcome::Confirmed
        );
        assert_eq!(
            PublishOutcome::from(Confirmation::Nack(None)),
            PublishOutcome::Nacked
        );
    }

    #[test]
    fn a_confirmation_with_a_returned_message_reads_as_unroutable() {
        // A mandatory publish the broker could not route comes back with the
        // message, whether it is then acked or nacked.
        let returned = || lapin::message::BasicReturnMessage {
            delivery: Delivery::mock(1, "".into(), "nowhere".into(), false, Vec::new()),
            reply_code: 312,
            reply_text: "NO_ROUTE".into(),
        };
        assert_eq!(
            PublishOutcome::from(Confirmation::Ack(Some(returned()))),
            PublishOutcome::Unroutable
        );
        assert_eq!(
            PublishOutcome::from(Confirmation::Nack(Some(returned()))),
            PublishOutcome::Unroutable
        );
    }

    #[test]
    fn a_failed_publish_says_why_starting_with_the_kind_of_failure() {
        use ExchangeType::{Direct, Fanout, Headers, Topic};
        use PublishOutcome::{Confirmed, Nacked, Unroutable};
        let queue = SubSpec::Queue("orders".into());
        let exchange = SubSpec::Exchange {
            exchange: "keyhole.demo".into(),
            binding_key: "#".into(),
        };
        let why = |outcome, spec: &SubSpec, exchange_type, key| {
            publish_result(outcome, spec, exchange_type, key)
                .unwrap_err()
                .to_string()
        };
        assert!(publish_result(Confirmed, &queue, None, "orders").is_ok());
        assert!(publish_result(Confirmed, &exchange, Some(&Topic), "k").is_ok());
        // Through the default exchange, a missing queue is the only way to
        // miss.
        assert_eq!(
            why(Unroutable, &queue, None, "orders"),
            "unroutable: no queue named `orders`"
        );
        assert_eq!(
            why(Unroutable, &exchange, Some(&Topic), "order.nope"),
            "unroutable: no queue bound for key `order.nope`"
        );
        assert_eq!(
            why(Unroutable, &exchange, Some(&Direct), ""),
            "unroutable: no queue bound for the empty key"
        );
        // An exchange the listing hasn't seen is explained by its key.
        assert_eq!(
            why(Unroutable, &exchange, None, "k"),
            "unroutable: no queue bound for key `k`"
        );
        // Fanout and headers exchanges ignore the key, so it isn't named.
        assert_eq!(
            why(Unroutable, &exchange, Some(&Fanout), ""),
            "unroutable: no queue is bound to the exchange"
        );
        assert_eq!(
            why(Unroutable, &exchange, Some(&Headers), ""),
            "unroutable: no binding matches a message without headers"
        );
        assert_eq!(
            why(Nacked, &exchange, Some(&Topic), "k"),
            "NACKed: the broker didn't take the message"
        );
    }

    #[test]
    fn a_broker_refusal_reads_as_its_reply_text() {
        use lapin::protocol::{AMQPError, AMQPSoftError};
        let text = "ACCESS_REFUSED - cannot publish to internal exchange \
                    'amq.rabbitmq.trace' in vhost '/'";
        let refused = lapin::Error::from(lapin::ErrorKind::ProtocolError(AMQPError::new(
            AMQPSoftError::ACCESSREFUSED.into(),
            text.into(),
        )));
        assert_eq!(
            publish_error(refused, "exchange `amq.rabbitmq.trace`").to_string(),
            format!("refused: {text}")
        );
        // Anything else names what was being done.
        let lost = lapin::Error::from(lapin::ErrorKind::MissingHeartbeatError);
        assert_eq!(
            format!("{:#}", publish_error(lost, "queue `orders`")),
            "publishing to queue `orders`: no heartbeat received from server for too long"
        );
    }

    #[tokio::test]
    async fn publishing_needs_a_queue_or_an_exchange_and_a_connection() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let topic = PublishReq {
            spec: SubSpec::Topic("t".into()),
            routing_key: String::new(),
            body: Vec::new(),
        };
        let err = conn.publish(topic).await.unwrap_err().to_string();
        assert_eq!(err, "topic:t is not a RabbitMQ destination");
        let queue = PublishReq {
            spec: SubSpec::Queue("orders".into()),
            routing_key: "orders".into(),
            body: Vec::new(),
        };
        let err = conn.publish(queue).await.unwrap_err().to_string();
        assert_eq!(err, "connection is not established");
    }

    #[test]
    fn management_client_targets_the_profile_url_and_starts_unprobed() {
        let conn = RabbitmqConnection::new(profile(true), None, None, PREVIEW, Arc::default());
        assert_eq!(conn.mgmt.base_url(), "https://rabbit.example.com:15671");
        assert!(conn.mgmt.probe().is_none());
        assert!(
            conn.mgmt.client().is_err(),
            "unusable until connect probes it"
        );
    }

    // -- rosters ------------------------------------------------------------

    fn connections_of(json: &str) -> Vec<ClientConnection> {
        match connection_rows(serde_json::from_str(json).expect("connections parse")) {
            RosterRows::Connections(rows) => rows,
            other => panic!("{other:?}"),
        }
    }

    fn consumers_of(json: &str) -> Vec<QueueConsumer> {
        match consumer_rows(serde_json::from_str(json).expect("consumers parse")) {
            RosterRows::Consumers(rows) => rows,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn connection_rows_carry_the_client_name_peer_and_figures() {
        for json in [fixture!("connections.v3"), fixture!("connections.v4")] {
            let rows = connections_of(&json);
            // By name, whatever order the broker sent (3.13 sent them reversed).
            let names: Vec<&str> = rows.iter().map(ClientConnection::display_name).collect();
            assert_eq!(names, ["spike-load-admin", "spike-load-mgmt"]);
            let admin = &rows[0];
            assert_eq!(admin.client_name.as_deref(), Some("spike-load-admin"));
            assert_eq!(admin.user.as_deref(), Some("keyhole"));
            // The broker's own name is the address pair, peer first.
            let peer = admin.peer.as_deref().expect("peer");
            assert!(admin.name.starts_with(&format!("{peer} -> ")), "{admin:?}");
            assert_eq!(admin.channels, Some(3));
            assert_eq!(admin.state.as_deref(), Some("running"));
            assert!(admin.recv_rate.is_some() && admin.send_rate.is_some());
            assert!(admin.connected_at.is_some());
        }
        let v4 = connections_of(&fixture!("connections.v4"));
        let admin = &v4[0];
        assert_eq!(admin.peer.as_deref(), Some("172.17.0.1:56648"));
        assert_eq!(
            (admin.recv_rate, admin.send_rate),
            (Some(660.0), Some(1219.2))
        );
        // Milliseconds since the epoch.
        assert_eq!(
            admin.connected_at,
            Some(OffsetDateTime::from_unix_timestamp_nanos(1_794_066_383_016_000_000).unwrap())
        );
        // A management user's own connection; none with metrics off.
        let own = connections_of(&fixture!("connections.management.v4"));
        assert_eq!(own.len(), 1);
        assert_eq!(own[0].user.as_deref(), Some("spike-mgmt"));
        assert!(connections_of(&fixture!("connections.no-metrics.v4")).is_empty());
    }

    #[test]
    fn an_unnamed_connection_shows_the_brokers_name() {
        let row = client_connection(
            serde_json::from_str(
                r#"{"name":"[::1]:5000 -> [::1]:5672","user_provided_name":"",
                    "peer_host":"::1","peer_port":5000}"#,
            )
            .unwrap(),
        );
        assert_eq!(row.client_name, None, "an empty name is no name");
        assert_eq!(row.display_name(), "[::1]:5000 -> [::1]:5672");
        assert_eq!(row.peer.as_deref(), Some("[::1]:5000"), "IPv6 is bracketed");
        assert_eq!(row.connected_at, None);
        assert_eq!(peer_address("10.0.0.1", None), "10.0.0.1");
    }

    #[test]
    fn consumer_rows_shorten_the_channel_and_sort_by_queue() {
        for json in [fixture!("consumers.v3"), fixture!("consumers.v4")] {
            let rows = consumers_of(&json);
            let queues: Vec<&str> = rows.iter().map(|c| c.queue.as_str()).collect();
            assert_eq!(
                queues,
                [
                    "spike.consumed",
                    "spike.consumed",
                    "spike.quorum.busy",
                    "spike.quorum.busy"
                ]
            );
            let manual = &rows[0];
            assert_eq!(manual.tag, "spike-consumer-classic");
            assert_eq!(manual.ack_required, Some(true));
            assert_eq!(manual.prefetch, Some(10));
            assert_eq!(manual.active, Some(true));
            assert_eq!(manual.activity_status.as_deref(), Some("up"));
            // `peer -> broker (1)` loses the broker side every row shares.
            let channel = manual.channel.as_deref().unwrap();
            assert!(
                channel.ends_with(" (1)") && !channel.contains("->"),
                "{channel}"
            );
            let auto = &rows[2];
            assert_eq!(auto.tag, "spike-consumer-quorum");
            assert_eq!((auto.ack_required, auto.prefetch), (Some(false), Some(0)));
        }
        let v4 = consumers_of(&fixture!("consumers.v4"));
        assert_eq!(v4[0].channel.as_deref(), Some("172.17.0.1:56648 (1)"));
        assert_eq!(v4[1].channel.as_deref(), Some("172.17.0.1:56656 (1)"));

        // A consumer whose channel is gone keeps its row; without the
        // connection's name the channel keeps its own.
        let gone = queue_consumer(
            serde_json::from_str(
                r#"{"consumer_tag":"t","queue":{"name":"q"},"channel_details":[]}"#,
            )
            .unwrap(),
        );
        assert_eq!((gone.queue.as_str(), gone.channel), ("q", None));
        let bare = queue_consumer(
            serde_json::from_str(r#"{"consumer_tag":"t","channel_details":{"name":"a -> b (3)"}}"#)
                .unwrap(),
        );
        assert_eq!(bare.channel.as_deref(), Some("a -> b (3)"));
    }

    #[tokio::test]
    async fn a_monitoring_user_gets_every_connection() {
        let (mut conn, server) = probed_as(
            fixture!("whoami.monitoring.v3"),
            "3.13.7",
            false,
            vec![(200, fixture!("connections.v3"))],
        )
        .await;
        let roster = conn.roster(RosterKind::Connections).await.expect("roster");
        let requests = server.join().unwrap();
        assert!(!roster.own_only && !roster.stats_disabled);
        assert!(matches!(&roster.rows, RosterRows::Connections(rows) if rows.len() == 2));
        assert!(
            requests[2].starts_with("GET /api/vhosts/%2F/connections?columns=name,"),
            "{}",
            requests[2]
        );
    }

    #[tokio::test]
    async fn a_management_user_gets_its_own_consumers() {
        let (mut conn, server) = probed_as(
            fixture!("whoami.management.v4"),
            "4.4.0",
            false,
            vec![(200, fixture!("consumers.v4"))],
        )
        .await;
        let roster = conn.roster(RosterKind::Consumers).await.expect("roster");
        let requests = server.join().unwrap();
        assert!(roster.own_only, "no monitoring tag: only its own");
        assert!(matches!(&roster.rows, RosterRows::Consumers(rows) if rows.len() == 4));
        assert!(
            requests[2].starts_with("GET /api/consumers/%2F?columns=consumer_tag,"),
            "{}",
            requests[2]
        );
    }

    #[tokio::test]
    async fn with_metrics_off_a_roster_says_so_without_asking() {
        // The probe found the collector off. The broker would list no
        // connections and refuse the consumers, so nothing is asked.
        let (mut conn, server) = listing_conn(true, Vec::new()).await;
        for kind in [RosterKind::Connections, RosterKind::Consumers] {
            let roster = conn.roster(kind).await.expect("not a failure");
            assert!(roster.stats_disabled && roster.is_empty(), "{roster:?}");
        }
        assert_eq!(server.join().unwrap().len(), 2, "the probe's requests only");
        assert!(conn.mgmt.client().is_ok());
    }

    #[tokio::test]
    async fn a_stats_disabled_refusal_reads_as_metrics_off() {
        // The collector went off after the probe: `/api/consumers` says so
        // with a 400.
        let (mut conn, server) =
            listing_conn(false, vec![(400, fixture!("error.400.stats-disabled.v4"))]).await;
        let roster = conn
            .roster(RosterKind::Consumers)
            .await
            .expect("not a failure");
        server.join().unwrap();
        assert!(roster.stats_disabled);
        assert!(matches!(&roster.rows, RosterRows::Consumers(rows) if rows.is_empty()));
        assert!(conn.mgmt.client().is_ok());
    }

    #[tokio::test]
    async fn a_roster_before_a_good_probe_fails_fast_without_a_request() {
        let mut conn = RabbitmqConnection::new(profile(false), None, None, PREVIEW, Arc::default());
        let started = std::time::Instant::now();
        let err = conn.roster(RosterKind::Connections).await.unwrap_err();
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(
            err.to_string(),
            "management API unavailable at http://rabbit.example.com:15672: the management \
             API has not been probed yet"
        );
    }

    #[tokio::test]
    async fn an_unreachable_api_fails_later_rosters_fast_with_the_same_message() {
        // The stub answers the probe, then stops listening.
        let (mut conn, server) = listing_conn(false, Vec::new()).await;
        server.join().unwrap();
        let first = conn
            .roster(RosterKind::Consumers)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            first.starts_with("management API unavailable at")
                && first.contains("could not reach the management API"),
            "{first}"
        );
        assert!(conn.mgmt.error().is_some(), "marked unreachable");
        let again = conn
            .roster(RosterKind::Connections)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(again, first);
    }

    #[tokio::test]
    async fn a_refused_roster_keeps_the_api_usable() {
        let (mut conn, server) =
            listing_conn(false, vec![(401, fixture!("error.401.not-management.v4"))]).await;
        let err = conn
            .roster(RosterKind::Connections)
            .await
            .unwrap_err()
            .to_string();
        server.join().unwrap();
        assert!(err.contains("lacks the `management` tag"), "{err}");
        assert!(conn.mgmt.client().is_ok(), "a refusal is cheap to retry");
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    //! Run against a dockerized RabbitMQ (see `docker-compose.yml`): an AMQP
    //! 0.9.1 broker on `127.0.0.1:$KEYHOLE_TEST_RABBITMQ_PORT` (default 5673),
    //! creds `keyhole:keyhole` on vhost `/`. A non-`guest` user is required
    //! because RabbitMQ restricts `guest` to loopback, and the dockerized broker
    //! sees the host connection arriving from the bridge network. Each test uses
    //! uniquely-named exchanges and queues so the suite is parallel-safe, and
    //! deletes them again through a [`Cleanup`] guard. The management API is
    //! on `127.0.0.1:$KEYHOLE_TEST_RABBITMQ_MGMT_PORT` (default 15673).
    //!
    //! The suite runs against RabbitMQ 3.x and 4.x (the `rabbitmq4` compose
    //! service, `just test-int-rabbitmq4`), and on a fresh broker: it asserts
    //! only what both versions report from the start.
    use super::*;
    use crate::broker::SubSpec;

    /// The body bytes a test connection's peek fetches per message.
    const PREVIEW: usize = 4096;
    use lapin::options::{
        BasicGetOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions,
    };
    use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    fn test_port() -> u16 {
        std::env::var("KEYHOLE_TEST_RABBITMQ_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5673)
    }

    fn mgmt_port() -> u16 {
        std::env::var("KEYHOLE_TEST_RABBITMQ_MGMT_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(15673)
    }

    fn url() -> String {
        // vhost "/" → "%2F".
        format!("amqp://keyhole:keyhole@127.0.0.1:{}/%2F", test_port())
    }

    fn unique(prefix: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}.{}.{}", std::process::id(), n)
    }

    /// Deletes a test's queues and exchanges when it's dropped, so a test
    /// cleans up after a failed assertion too. Declare it before what it
    /// names: locals drop in reverse order, so the test's connections close
    /// first. It goes through the management API, whose blocking calls work
    /// in `drop` while a panic unwinds. One that was never created, or that
    /// the test already deleted, is a 404, which is fine.
    #[derive(Default)]
    struct Cleanup {
        queues: Vec<String>,
        exchanges: Vec<String>,
    }

    impl Cleanup {
        fn queues(names: &[&str]) -> Self {
            Self::default().and_queues(names)
        }

        fn exchanges(names: &[&str]) -> Self {
            Self::default().and_exchanges(names)
        }

        fn and_queues(mut self, names: &[&str]) -> Self {
            self.queues.extend(names.iter().map(|n| n.to_string()));
            self
        }

        fn and_exchanges(mut self, names: &[&str]) -> Self {
            self.exchanges.extend(names.iter().map(|n| n.to_string()));
            self
        }
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let agent = crate::broker::http::agent();
            let auth = crate::broker::http::basic_auth("keyhole", Some("keyhole"));
            let queues = self.queues.iter().map(|q| ("queues", q));
            let exchanges = self.exchanges.iter().map(|e| ("exchanges", e));
            for (kind, name) in queues.chain(exchanges) {
                let url = format!(
                    "http://127.0.0.1:{}/api/{kind}/%2F/{}",
                    mgmt_port(),
                    utf8_percent_encode(name, NON_ALPHANUMERIC)
                );
                let reply = agent
                    .delete(&url)
                    .header("Authorization", &auth)
                    // 3.13 may drop a kept-alive connection after a 404.
                    .header("Connection", "close")
                    .call();
                match reply {
                    Ok(r) if r.status().is_success() || r.status() == 404 => {}
                    Ok(r) => eprintln!("cleanup: deleting {kind} {name}: HTTP {}", r.status()),
                    Err(e) => eprintln!("cleanup: deleting {kind} {name}: {e}"),
                }
            }
        }
    }

    fn test_profile() -> RabbitmqProfile {
        RabbitmqProfile {
            name: "test".into(),
            host: "127.0.0.1".into(),
            port: test_port(),
            vhost: "/".into(),
            username: Some("keyhole".into()),
            password: None,
            tls: false,
            // The compose file maps the API off its default port.
            management_url: Some(format!("http://127.0.0.1:{}", mgmt_port())),
            management_username: None,
            management_password: None,
        }
    }

    async fn connected() -> RabbitmqConnection {
        connect_with(test_profile(), "keyhole").await
    }

    /// Connect `profile` with the test password for AMQP and `mgmt_password`
    /// for the management API.
    async fn connect_with(profile: RabbitmqProfile, mgmt_password: &str) -> RabbitmqConnection {
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("keyhole".to_string()),
            Some(mgmt_password.to_string()),
            PREVIEW,
            Arc::default(),
        );
        let caps = conn.connect().await.expect("connect to test RabbitMQ");
        assert_eq!(caps.r#type, crate::broker::BrokerType::Rabbitmq);
        conn
    }

    /// Declare a durable topic exchange over a throwaway connection.
    async fn declare_topic_exchange(name: &str) {
        let connection = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        channel
            .exchange_declare(
                name.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        connection.close(200, "bye".into()).await.ok();
    }

    /// Publish one message to `exchange` with `routing_key`.
    async fn publish(exchange: &str, routing_key: &str, body: &str) {
        let connection = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        connection.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn exchange_tap_receives_published_message_with_routing_key() {
        let exchange = unique("keyhole.it.topic");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        declare_topic_exchange(&exchange).await;

        let mut conn = connected().await;
        // Default `#` binding key → matches every routing key on a topic exchange.
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");

        // The spy queue is bound and consuming by the time `subscribe` returns,
        // and queues retain messages, so a single publish afterwards is observed
        // without the publish/attach race the non-retaining AMQP topic tail has.
        publish(&exchange, "order.created", "hello rabbit").await;

        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(ev.source, exchange);
        assert_eq!(ev.payload, Payload::Utf8("hello rabbit".into()));
        assert_eq!(ev.meta("routing_key"), Some("order.created"));
    }

    #[tokio::test]
    async fn exchange_tap_is_live_only_and_leaves_the_real_queue_intact() {
        let exchange = unique("keyhole.it.topic");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        declare_topic_exchange(&exchange).await;

        // A real queue bound to the exchange — the "production" consumer we must
        // neither steal from nor replay into. Exclusive so it auto-cleans on close.
        let real_queue = unique("keyhole.it.realq");
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let setup_ch = setup.create_channel().await.unwrap();
        setup_ch
            .queue_declare(
                real_queue.as_str().into(),
                QueueDeclareOptions {
                    exclusive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        setup_ch
            .queue_bind(
                real_queue.as_str().into(),
                exchange.as_str().into(),
                "#".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        // Publish BEFORE the tap exists: this reaches the real queue but not the
        // not-yet-created spy queue.
        publish(&exchange, "evt", "backlog-msg").await;

        // Open the tap (which declares + binds its own spy queue), then publish a
        // second message that fans out to both the real queue and the spy queue.
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        publish(&exchange, "evt", "live-msg").await;

        // The tap must observe ONLY the live message, never the pre-subscription
        // backlog: a destructive or replaying implementation that drained/read an
        // existing queue would surface "backlog-msg" here and fail this assertion.
        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(
            ev.payload,
            Payload::Utf8("live-msg".into()),
            "the tap is live-only: it must not see traffic published before it attached"
        );
        drop(stream); // close the tap connection → spy queue auto-deletes

        // The real queue must still hold BOTH messages: the tap ran on a parallel
        // spy queue and consumed only its own copies, taking nothing from here.
        let mut bodies = timeout(Duration::from_secs(5), async {
            let mut out: Vec<String> = Vec::new();
            while out.len() < 2 {
                match setup_ch
                    .basic_get(real_queue.as_str().into(), BasicGetOptions { no_ack: true })
                    .await
                {
                    Ok(Some(msg)) => out.push(String::from_utf8(msg.data.clone()).unwrap()),
                    Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
                    // Surface a real channel error instead of spinning to the timeout.
                    Err(e) => panic!("basic_get on the real queue failed: {e}"),
                }
            }
            out
        })
        .await
        .expect("real queue should still hold both messages");
        bodies.sort();
        assert_eq!(
            bodies,
            vec!["backlog-msg".to_string(), "live-msg".to_string()],
            "exchange tap must leave every message the real queue would have received"
        );

        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn an_overlong_name_or_key_is_refused_and_the_connection_carries_on() {
        let exchange = unique("keyhole.it.long");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        declare_topic_exchange(&exchange).await;
        let tap = |exchange: &str, key: &str| SubSpec::Exchange {
            exchange: exchange.into(),
            binding_key: key.into(),
        };

        let mut conn = connected().await;
        let long = "k".repeat(256);
        for (spec, expected) in [
            (tap(&exchange, &long), "binding key too long, max 255 bytes"),
            (tap(&long, "#"), "exchange name too long, max 255 bytes"),
        ] {
            let err = match conn.subscribe(spec).await {
                Ok(_) => panic!("{expected}: the tap can't open"),
                Err(e) => e,
            };
            assert_eq!(format!("{err:#}"), expected);
        }
        let err = publish_via(&mut conn, tap(&exchange, "#"), &long, "lost")
            .await
            .unwrap_err();
        assert_eq!(format!("{err:#}"), "routing key too long, max 255 bytes");

        // The same connection still answers, taps and publishes, with a key
        // of exactly 255 bytes that ends in a multibyte char.
        conn.ping().await.expect("the connection is alive");
        let max = format!("{}é", "k".repeat(253));
        assert_eq!(max.len(), 255);
        let mut stream = conn
            .subscribe(tap(&exchange, &max))
            .await
            .expect("exchange tap");
        publish_via(&mut conn, tap(&exchange, "#"), &max, "made it")
            .await
            .expect("confirmed: the tap's queue takes it");
        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(ev.payload, Payload::Utf8("made it".into()));
        assert_eq!(ev.meta("routing_key"), Some(max.as_str()));
    }

    #[tokio::test]
    async fn tapping_a_missing_exchange_errors() {
        let mut conn = connected().await;
        let missing = unique("keyhole.it.nope");
        // `BrokerEventStream` is not `Debug`, so match rather than `expect_err`.
        let err = match conn
            .subscribe(SubSpec::Exchange {
                exchange: missing,
                binding_key: "#".into(),
            })
            .await
        {
            Ok(_) => panic!("a non-existent exchange must fail the tap"),
            Err(e) => e,
        };
        // The top-level message names the operation; the broker's reply code is
        // preserved as the error source and shown in the `{:#}` chain (so a
        // NOT_FOUND is distinguishable from e.g. an ACCESS_REFUSED).
        let chain = format!("{err:#}");
        assert!(
            chain.contains("tapping exchange"),
            "error should name the operation: {chain}"
        );
        assert!(
            chain.contains("NOT_FOUND") || chain.to_lowercase().contains("no exchange"),
            "the AMQP reply code must survive in the error chain: {chain}"
        );
    }

    #[tokio::test]
    async fn connect_probes_the_management_api() {
        let conn = connected().await;
        let probe = conn
            .mgmt
            .probe()
            .unwrap_or_else(|| panic!("probe failed: {:?}", conn.mgmt.error()));
        assert_eq!(probe.user, "keyhole");
        assert!(
            probe.tags.iter().any(|t| t == "administrator"),
            "{:?}",
            probe.tags
        );
        assert!(probe.can_monitor());
        let major = probe.major_version().expect("a version like 3.13.7");
        assert!(major >= 3, "{:?}", probe.version);
    }

    /// Connect `profile` with the test password, giving up after 5s.
    async fn try_connect(profile: RabbitmqProfile) -> Option<anyhow::Result<Capabilities>> {
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("keyhole".to_string()),
            Some("keyhole".to_string()),
            PREVIEW,
            Arc::default(),
        );
        timeout(Duration::from_secs(5), conn.connect()).await.ok()
    }

    #[tokio::test]
    async fn an_ipv6_host_is_the_host_connected_to() {
        // The loopback address reaches the test broker.
        let mut profile = test_profile();
        profile.host = "::1".into();
        let caps = try_connect(profile).await.expect("in time");
        assert!(caps.is_ok(), "{caps:?}");
        // A documentation address reaches nothing. In a URL it read as
        // `localhost`, and logged in to the test broker.
        let mut profile = test_profile();
        profile.host = "2001:db8::1".into();
        let result = try_connect(profile).await;
        assert!(
            !matches!(result, Some(Ok(_))),
            "connected somewhere: {result:?}"
        );
    }

    #[tokio::test]
    async fn a_slash_in_the_host_is_a_connect_error() {
        // In a URL the `/…` became a 307-byte vhost, which lapin panicked on.
        let mut profile = test_profile();
        profile.host = format!("127.0.0.1/{}", "v".repeat(300));
        // The name lookup fails at once, well inside the time allowed.
        let result = try_connect(profile).await;
        assert!(
            matches!(result, Some(Err(_))),
            "a prompt connect error: {result:?}"
        );
    }

    #[tokio::test]
    async fn a_tls_connect_or_tap_on_a_plain_port_fails_instead_of_hanging() {
        // Both rustls backends are built in, so until keyhole chose one,
        // rustls panicked in lapin's I/O thread and the connect never returned.
        // The provider is installed once per process, so another test's
        // install can hide a missing call on this path; the fresh-process
        // tests in `tests/fresh_process_tls.rs` don't share it.
        let mut profile = test_profile();
        profile.tls = true;
        let result = try_connect(profile.clone()).await;
        assert!(
            matches!(result, Some(Err(_))),
            "a prompt TLS error: {result:?}"
        );
        // A tap opens a connection of its own.
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("keyhole".to_string()),
            None,
            PREVIEW,
            Arc::default(),
        );
        let tap = SubSpec::Exchange {
            exchange: "amq.topic".into(),
            binding_key: "#".into(),
        };
        match timeout(Duration::from_secs(5), conn.subscribe(tap)).await {
            Ok(Err(_)) => {}
            Ok(Ok(_)) => panic!("a TLS tap on a plain port can't open"),
            Err(_) => panic!("the tap hung"),
        }
    }

    #[test]
    fn the_management_api_over_tls_on_a_plain_port_is_an_error() {
        // ureq takes the installed provider (aws-lc-rs) instead of its own
        // `ring` fallback, and still completes a TLS attempt.
        // The provider is installed once per process, so another test's
        // install can hide a missing call on this path; the fresh-process
        // tests in `tests/fresh_process_tls.rs` don't share it.
        let url = format!("https://127.0.0.1:{}/api/overview", mgmt_port());
        let reply = crate::broker::http::agent().get(&url).call();
        assert!(reply.is_err(), "{reply:?}");
    }

    #[tokio::test]
    async fn a_dot_vhost_is_the_vhost_asked_for() {
        // In a URL `.` and `..` were dot segments, which left the empty vhost.
        for vhost in [".", ".."] {
            let mut profile = test_profile();
            profile.vhost = vhost.into();
            let err = match try_connect(profile).await.expect("in time") {
                Ok(_) => panic!("no vhost `{vhost}` on the test broker"),
                Err(e) => format!("{e:#}"),
            };
            assert!(err.contains(&format!("vhost {vhost} not found")), "{err}");
        }
    }

    #[tokio::test]
    async fn an_unreachable_management_api_leaves_tails_working() {
        // Bind then drop, so the port is (almost certainly) closed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut profile = test_profile();
        profile.management_url = Some(format!("http://{}", listener.local_addr().unwrap()));
        drop(listener);

        let mut conn = connect_with(profile, "keyhole").await;
        let error = conn.mgmt.error().expect("mgmt_error is recorded");
        assert!(
            error.contains("could not reach the management API"),
            "{error}"
        );
        assert!(conn.mgmt.client().is_err(), "later calls fail fast");

        // The AMQP side is unaffected: liveness and an exchange tap still work.
        conn.ping().await.expect("ping");
        let exchange = unique("keyhole.it.topic");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        declare_topic_exchange(&exchange).await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        publish(&exchange, "k", "still tailing").await;
        let ev = timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("tap timed out")
            .expect("stream ended");
        assert_eq!(ev.payload, Payload::Utf8("still tailing".into()));
    }

    #[tokio::test]
    async fn connect_offers_the_dashboard_only_when_the_api_answers() {
        let mut conn = RabbitmqConnection::new(
            test_profile(),
            Some("keyhole".into()),
            Some("keyhole".into()),
            PREVIEW,
            Arc::default(),
        );
        let caps = conn.connect().await.expect("connect");
        assert!(caps.can_dashboard, "{:?}", conn.mgmt.error());

        // Bind then drop, so the port is (almost certainly) closed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut profile = test_profile();
        profile.management_url = Some(format!("http://{}", listener.local_addr().unwrap()));
        drop(listener);
        let mut conn = RabbitmqConnection::new(
            profile,
            Some("keyhole".into()),
            Some("keyhole".into()),
            PREVIEW,
            Arc::default(),
        );
        let caps = conn.connect().await.expect("AMQP still connects");
        assert!(!caps.can_dashboard, "no stats band without the API");
    }

    #[tokio::test]
    async fn stats_report_the_version_totals_and_node_health() {
        // The `keyhole` user is an administrator, so the nodes are read too.
        // Only what a node reports from the start is checked: on a fresh node
        // (CI's) `message_stats` and `queue_totals` stay empty until traffic
        // arrives, and a node is only `name` and `running` until its first
        // stats emission. The fixture tests cover the figures.
        let mut conn = connected().await;
        let stats = conn.stats().await.expect("stats");
        let stats = stats.rabbitmq().expect("RabbitMQ stats");
        let version = stats.rabbitmq_version.as_deref().expect("a version");
        assert!(version.starts_with(['3', '4']), "{version}");
        assert!(stats.erlang_version.is_some() && stats.cluster_name.is_some());
        assert!(stats.cluster_wide);
        assert!(stats.queues.is_some() && stats.connections.is_some());
        let node = stats.nodes.first().expect("node health");
        assert!(node.name.starts_with("rabbit@"), "{}", node.name);
        assert_eq!(node.running, Some(true));
    }

    #[tokio::test]
    async fn a_wrong_management_password_is_explained() {
        let conn = connect_with(test_profile(), "not-the-password").await;
        let error = conn.mgmt.error().expect("mgmt_error is recorded");
        assert!(error.contains("rejected the credentials"), "{error}");
        assert!(error.contains("management_password"), "{error}");
    }

    #[tokio::test]
    async fn list_destinations_lists_a_real_queue_and_the_exchanges() {
        use crate::broker::QueueType;
        use lapin::options::QueueDeleteOptions;

        // A uniquely named queue holding one message.
        let queue = unique("keyhole.it.listed");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;
        publish("", &queue, "count me").await;

        let mut conn = connected().await;
        // The count lands with the broker's next stats emission (every 5 s).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let list = loop {
            let list = conn.list_destinations(false).await.expect("lists");
            let counted = list
                .items
                .iter()
                .any(|d| d.name == queue && d.messages == Some(1));
            if counted || tokio::time::Instant::now() > deadline {
                break list;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert!(!list.stats_disabled);
        let row = list
            .items
            .iter()
            .find(|d| d.name == queue)
            .expect("the new queue is listed");
        assert_eq!(row.kind, DestKind::Queue);
        assert_eq!(row.queue_type, Some(QueueType::Classic));
        assert_eq!((row.messages, row.ready), (Some(1), Some(1)));

        // Queues come first, then the exchanges — `amq.*` included, the
        // default `""` left out.
        let first_exchange = list
            .items
            .iter()
            .position(|d| d.kind == DestKind::Exchange)
            .expect("exchanges are listed");
        assert!(list.items[..first_exchange]
            .iter()
            .all(|d| d.kind == DestKind::Queue));
        let exchange_type = |name: &str| {
            list.items
                .iter()
                .find(|d| d.kind == DestKind::Exchange && d.name == name)
                .and_then(|d| d.exchange_type.clone())
        };
        assert_eq!(exchange_type("amq.topic"), Some(ExchangeType::Topic));
        assert_eq!(exchange_type("amq.direct"), Some(ExchangeType::Direct));
        assert_eq!(exchange_type("amq.fanout"), Some(ExchangeType::Fanout));
        assert_eq!(exchange_type("amq.headers"), Some(ExchangeType::Headers));
        assert!(list.items.iter().all(|d| !d.name.is_empty()));

        // Deleted on the server, it drops out of the next listing.
        channel
            .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
        let list = conn.list_destinations(false).await.expect("lists again");
        assert!(list.items.iter().all(|d| d.name != queue));
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn an_internal_exchange_can_be_tapped() {
        // Clients can't publish to an internal exchange, but a tap only binds
        // a queue to it. The firehose tracer is off, so nothing arrives; the
        // tap just has to attach.
        let mut conn = connected().await;
        let tap = conn
            .subscribe(SubSpec::Exchange {
                exchange: "amq.rabbitmq.trace".into(),
                binding_key: "#".into(),
            })
            .await;
        assert!(tap.is_ok(), "{:#}", tap.err().unwrap());
    }

    #[tokio::test]
    async fn a_headers_exchange_tap_receives_every_message() {
        use lapin::types::AMQPValue;

        let exchange = unique("keyhole.it.headers");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        let setup = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = setup.create_channel().await.unwrap();
        channel
            .exchange_declare(
                exchange.as_str().into(),
                ExchangeKind::Headers,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        // The tap binds with `#` and no arguments, which a headers exchange
        // matches against every message.
        let mut conn = connected().await;
        let mut stream = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("headers exchange tap");

        let mut headers = FieldTable::default();
        headers.insert("kind".into(), AMQPValue::LongString("order".into()));
        for (body, props) in [
            (
                "with headers",
                BasicProperties::default().with_headers(headers),
            ),
            ("without headers", BasicProperties::default()),
        ] {
            channel
                .basic_publish(
                    exchange.as_str().into(),
                    "".into(),
                    BasicPublishOptions::default(),
                    body.as_bytes(),
                    props,
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut bodies = Vec::new();
        for _ in 0..2 {
            match timeout(Duration::from_secs(8), stream.next()).await {
                Ok(Some(ev)) => bodies.push(ev.payload),
                _ => break,
            }
        }
        drop(stream);
        setup.close(200, "bye".into()).await.ok();

        assert_eq!(
            bodies,
            [
                Payload::Utf8("with headers".into()),
                Payload::Utf8("without headers".into()),
            ]
        );
    }

    /// A throwaway connection and channel for setting up a test's queues.
    async fn setup_channel() -> (Connection, Channel) {
        let connection = Connection::connect(url().as_str(), ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        (connection, channel)
    }

    /// Declare a durable `queue` with `args`: 4.x refuses a transient queue
    /// that isn't exclusive, and an exclusive one can't be read through the
    /// management API. A classic queue (no `x-queue-type`) also expires a
    /// minute after its last use, in case a run dies before its [`Cleanup`].
    async fn declare_queue(channel: &Channel, queue: &str, mut args: FieldTable) {
        let classic = !args.inner().keys().any(|k| k.as_str() == "x-queue-type");
        if classic {
            args.insert("x-expires".into(), lapin::types::AMQPValue::LongInt(60_000));
        }
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await
            .unwrap_or_else(|e| panic!("declaring {queue}: {e:#}"));
    }

    /// Peek `queue` in `mode`.
    async fn peek_queue(
        conn: &mut RabbitmqConnection,
        queue: &str,
        mode: PeekMode,
    ) -> anyhow::Result<Vec<BrokerEvent>> {
        conn.peek(PeekReq {
            spec: SubSpec::Queue(queue.to_string()),
            mode,
            limit: 200,
        })
        .await
    }

    /// Drain `queue` over `channel` (auto-ack), waiting for `n` messages:
    /// each body and whether it was redelivered, in queue order.
    async fn drain(channel: &Channel, queue: &str, n: usize) -> Vec<(String, bool)> {
        timeout(Duration::from_secs(5), async {
            let mut out = Vec::new();
            while out.len() < n {
                match channel
                    .basic_get(queue.into(), BasicGetOptions { no_ack: true })
                    .await
                {
                    Ok(Some(msg)) => out.push((
                        String::from_utf8(msg.data.clone()).unwrap(),
                        msg.redelivered,
                    )),
                    Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
                    Err(e) => panic!("basic_get on {queue} failed: {e}"),
                }
            }
            out
        })
        .await
        .unwrap_or_else(|_| panic!("{queue} should hold {n} messages"))
    }

    fn bodies(events: &[BrokerEvent]) -> Vec<String> {
        events.iter().map(|e| e.payload.as_text()).collect()
    }

    #[tokio::test]
    async fn browse_peek_leaves_every_message_on_the_queue_flagged_redelivered() {
        let queue = unique("keyhole.it.peek");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;
        for body in ["first", "second", "third"] {
            publish("", &queue, body).await;
        }

        let mut conn = connected().await;
        let events = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("browse peek");
        assert_eq!(bodies(&events), ["first", "second", "third"]);
        for ev in &events {
            assert_eq!(ev.source, queue);
            assert_eq!(ev.meta("exchange"), Some("(default)"));
            assert_eq!(ev.meta("routing_key"), Some(queue.as_str()));
            assert_eq!(ev.meta("redelivered"), Some("false"));
        }
        // A second peek reads the same messages, in order, now flagged.
        let again = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("second peek");
        assert_eq!(bodies(&again), ["first", "second", "third"]);
        assert!(again.iter().all(|e| e.meta("redelivered") == Some("true")));

        // The real queue still holds every message, in order, flagged
        // redelivered: the peeks took nothing.
        assert_eq!(
            drain(&channel, &queue, 3).await,
            [
                ("first".to_string(), true),
                ("second".to_string(), true),
                ("third".to_string(), true),
            ]
        );
        let rest = channel
            .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap();
        assert!(rest.is_none(), "and nothing more");
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn destructive_peek_takes_the_messages_off_the_queue() {
        let queue = unique("keyhole.it.peekd");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;
        publish("", &queue, "one").await;
        publish("", &queue, "two").await;

        let mut conn = connected().await;
        let events = peek_queue(&mut conn, &queue, PeekMode::Destructive)
            .await
            .expect("destructive peek");
        assert_eq!(bodies(&events), ["one", "two"]);
        let empty = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("peek again");
        assert!(empty.is_empty(), "the destructive peek removed them");
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn peeking_a_stream_queue_explains_why_not() {
        let queue = unique("keyhole.it.stream");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        let mut args = FieldTable::default();
        args.insert(
            "x-queue-type".into(),
            lapin::types::AMQPValue::LongString("stream".into()),
        );
        declare_queue(&channel, &queue, args).await;

        let mut conn = connected().await;
        let err = peek_queue(&mut conn, &queue, PeekMode::Browse).await;
        setup.close(200, "bye".into()).await.ok();
        let err = err.expect_err("a stream refuses basic.get");
        assert_eq!(format!("{err:#}"), STREAM_PEEK_HINT);
    }

    /// The listing's row for a new durable quorum queue declared with
    /// `x-delivery-limit: limit` (deleted again), and the broker's major
    /// version.
    async fn listed_quorum_queue(limit: i32) -> (DestinationInfo, Option<u32>) {
        use crate::broker::QueueType;
        let queue = unique("keyhole.it.quorum");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        let mut args = FieldTable::default();
        args.insert(
            "x-queue-type".into(),
            lapin::types::AMQPValue::LongString("quorum".into()),
        );
        args.insert(
            "x-delivery-limit".into(),
            lapin::types::AMQPValue::LongInt(limit),
        );
        declare_queue(&channel, &queue, args).await;

        let mut conn = connected().await;
        let major = conn.mgmt.probe().and_then(mgmt::Probe::major_version);
        let list = conn.list_destinations(false).await;
        setup.close(200, "bye".into()).await.ok();
        let list = list.expect("lists");
        let row = list
            .items
            .into_iter()
            .find(|d| d.name == queue)
            .expect("the quorum queue is listed");
        assert_eq!(row.queue_type, Some(QueueType::Quorum));
        (row, major)
    }

    #[tokio::test]
    async fn the_listing_marks_a_limited_quorum_queue_on_3x_only() {
        let (row, major) = listed_quorum_queue(3).await;
        let expected = if major == Some(3) { Some(3) } else { None };
        assert_eq!(row.peek_delivery_limit, expected, "RabbitMQ {major:?}");
    }

    #[tokio::test]
    async fn the_listing_marks_a_negative_limit_on_3x_only() {
        // 3.x accepts `-1` and drops or dead-letters a message on its first
        // requeue. 4.x reads it as no limit (and lists it as `unlimited`).
        let (row, major) = listed_quorum_queue(-1).await;
        let expected = if major == Some(3) { Some(-1) } else { None };
        assert_eq!(row.peek_delivery_limit, expected, "RabbitMQ {major:?}");
    }

    /// Poll `fetch` on the blocking pool until `done` holds (the stats behind
    /// connection and consumer lists land a moment after the event).
    async fn eventually<T: Send + 'static>(
        fetch: impl Fn() -> anyhow::Result<T> + Send + Sync + Clone + 'static,
        done: impl Fn(&T) -> bool,
    ) -> T {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let f = fetch.clone();
            let value = tokio::task::spawn_blocking(f)
                .await
                .unwrap()
                .expect("request");
            if done(&value) || tokio::time::Instant::now() > deadline {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    #[tokio::test]
    async fn management_client_lists_and_reads_a_real_queue() {
        let mut conn = connected().await;
        let client = conn.mgmt.client().expect("probe succeeded").clone();

        // A queue whose name needs encoding (`#`, `/`, space, non-ASCII).
        let queue = unique("keyhole.it.q #1/ü");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;
        publish("", &queue, "peek me").await;

        let c = client.clone();
        let listing = tokio::task::spawn_blocking(move || c.queues())
            .await
            .unwrap()
            .expect("lists queues");
        let row = listing
            .items
            .iter()
            .find(|q| q.name == queue)
            .expect("the new queue is listed");
        assert_eq!(row.queue_type, crate::broker::QueueType::Classic);

        // A requeueing get reads the message and puts it back, flagged.
        for redelivered in [false, true] {
            let (c, q) = (client.clone(), queue.clone());
            let msgs =
                tokio::task::spawn_blocking(move || c.get(&q, 1, AckMode::AckRequeueTrue, None))
                    .await
                    .unwrap()
                    .expect("gets the message");
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0].body().unwrap(), b"peek me");
            assert_eq!(msgs[0].redelivered, redelivered);
            assert_eq!(msgs[0].routing_key, queue);
        }

        // A missing queue's 404 doesn't poison the next request (the 3.13
        // keep-alive quirk).
        let c = client.clone();
        let err = tokio::task::spawn_blocking(move || {
            c.get("keyhole.it.missing", 1, AckMode::AckRequeueTrue, None)
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(format!("{err:#}").contains("queue not found"), "{err:#}");
        let c = client.clone();
        tokio::task::spawn_blocking(move || c.exchanges())
            .await
            .unwrap()
            .expect("the next request still works");

        // This connection, and the consumer behind an open tap, are listed.
        let _tap = conn
            .subscribe(SubSpec::Exchange {
                exchange: "amq.topic".into(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        let c = client.clone();
        let connections = eventually(
            move || c.connections(),
            |rows: &Vec<mgmt::ConnectionInfo>| {
                rows.iter().any(|r| {
                    r.user_provided_name
                        .as_deref()
                        .is_some_and(|n| n.starts_with("keyhole-test-"))
                })
            },
        )
        .await;
        let ours = connections
            .iter()
            .find(|r| {
                r.user_provided_name
                    .as_deref()
                    .is_some_and(|n| n.starts_with("keyhole-test-"))
            })
            .expect("our connection is listed");
        assert_eq!(ours.user.as_deref(), Some("keyhole"));
        assert!(ours.peer_port.is_some() && ours.channels.is_some());
        let c = client.clone();
        let consumers = eventually(
            move || c.consumers(),
            |rows: &Vec<mgmt::ConsumerInfo>| {
                rows.iter().any(|r| r.queue.name.starts_with("amq.gen-"))
            },
        )
        .await;
        let tap = consumers
            .iter()
            .find(|r| r.queue.name.starts_with("amq.gen-"))
            .expect("the tap's consumer is listed");
        assert_eq!(tap.ack_required, Some(false), "the tap auto-acks");
        assert!(tap.channel_details.name.is_some());

        setup.close(200, "bye".into()).await.ok();
    }

    /// The queues `list` shows.
    fn queue_names(list: &DestinationList) -> Vec<String> {
        list.items
            .iter()
            .filter(|d| d.kind == DestKind::Queue)
            .map(|d| d.name.clone())
            .collect()
    }

    #[tokio::test]
    async fn the_listing_hides_our_tap_queues_but_not_another_clients() {
        // Another client's queue shaped like a spy queue: server-named,
        // exclusive and auto-delete.
        let (other, other_channel) = setup_channel().await;
        let theirs = other_channel
            .queue_declare(
                "".into(),
                QueueDeclareOptions {
                    exclusive: true,
                    auto_delete: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap()
            .name()
            .as_str()
            .to_owned();

        // The connection has a registry of its own, so other tests' taps
        // don't count.
        let mut conn = connected().await;
        let tap = conn
            .subscribe(SubSpec::Exchange {
                exchange: "amq.topic".into(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");
        let [ours] = &conn.tap_queues.names()[..] else {
            panic!("the tap notes one queue: {:?}", conn.tap_queues.names());
        };
        let ours = ours.clone();
        assert!(ours.starts_with("amq.gen-"), "{ours}");

        // Both queues are on the broker, but the list shows only theirs.
        let client = conn.mgmt.client().expect("probe succeeded").clone();
        let names = |l: &mgmt::Listing<QueueInfo>| -> Vec<String> {
            l.items.iter().map(|q| q.name.clone()).collect()
        };
        let (c, o, t) = (client.clone(), ours.clone(), theirs.clone());
        let on_broker = names(
            &eventually(
                move || c.queues(),
                move |l| {
                    let n = names(l);
                    n.contains(&o) && n.contains(&t)
                },
            )
            .await,
        );
        assert!(
            on_broker.contains(&ours) && on_broker.contains(&theirs),
            "both queues are on the broker: {on_broker:?}"
        );
        let assert_hidden = |list: DestinationList| {
            let shown = queue_names(&list);
            assert!(!shown.contains(&ours), "{ours} is hidden: {shown:?}");
            assert!(shown.contains(&theirs), "{theirs} is listed: {shown:?}");
        };
        assert_hidden(conn.list_destinations(false).await.expect("lists"));

        // Closed, the tap's queue stays hidden while the broker deletes it,
        // and after.
        drop(tap);
        assert_hidden(conn.list_destinations(false).await.expect("lists"));
        let (c, o) = (client.clone(), ours.clone());
        let on_broker =
            names(&eventually(move || c.queues(), move |l| !names(l).contains(&o)).await);
        assert!(
            !on_broker.contains(&ours),
            "the broker deletes the closed tap's queue: {on_broker:?}"
        );
        assert_hidden(conn.list_destinations(false).await.expect("lists"));
        assert_eq!(conn.tap_queues.names(), [ours], "kept until it expires");
        other.close(200, "bye".into()).await.ok();
    }

    /// Ask for `kind` until `done` holds (the broker's stats behind the
    /// lists land a moment after the connection or consumer appears).
    async fn roster_until(
        conn: &mut RabbitmqConnection,
        kind: RosterKind,
        done: impl Fn(&Roster) -> bool,
    ) -> Roster {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let roster = conn.roster(kind).await.expect("roster");
            if done(&roster) || tokio::time::Instant::now() > deadline {
                return roster;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    #[tokio::test]
    async fn rosters_list_this_connection_and_its_taps_consumer() {
        // A profile name of its own, so this test's connections (named
        // `keyhole-<profile>-<n>`) stand out from other tests'.
        let mut profile = test_profile();
        profile.name = unique("roster");
        let prefix = format!("keyhole-{}-", profile.name);
        let exchange = unique("keyhole.it.roster");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        let mut conn = connect_with(profile, "keyhole").await;
        declare_topic_exchange(&exchange).await;
        // The tap opens a connection of its own and consumes a server-named
        // queue (auto-ack).
        let _tap = conn
            .subscribe(SubSpec::Exchange {
                exchange: exchange.clone(),
                binding_key: "#".into(),
            })
            .await
            .expect("exchange tap");

        let ours = |rows: &[ClientConnection]| -> Vec<ClientConnection> {
            rows.iter()
                .filter(|r| {
                    r.client_name
                        .as_deref()
                        .is_some_and(|n| n.starts_with(&prefix))
                })
                .cloned()
                .collect()
        };
        let roster = roster_until(
            &mut conn,
            RosterKind::Connections,
            |r| matches!(&r.rows, RosterRows::Connections(rows) if ours(rows).len() == 2),
        )
        .await;
        // The test user is an administrator: every connection is listed.
        assert!(!roster.own_only && !roster.stats_disabled);
        let RosterRows::Connections(rows) = &roster.rows else {
            panic!("{roster:?}")
        };
        let mine = ours(rows);
        assert_eq!(mine.len(), 2, "this connection and the tap's: {rows:?}");
        for row in &mine {
            assert_eq!(row.display_name(), row.client_name.as_deref().unwrap());
            assert_eq!(row.user.as_deref(), Some("keyhole"));
            let peer = row.peer.as_deref().expect("a peer address");
            assert!(row.name.starts_with(&format!("{peer} -> ")), "{row:?}");
        }

        let peers: Vec<String> = mine.iter().filter_map(|r| r.peer.clone()).collect();
        let on_our_channel = |c: &QueueConsumer| {
            c.channel
                .as_deref()
                .is_some_and(|ch| peers.iter().any(|p| ch.starts_with(&format!("{p} ("))))
        };
        let roster = roster_until(
            &mut conn,
            RosterKind::Consumers,
            |r| matches!(&r.rows, RosterRows::Consumers(rows) if rows.iter().any(on_our_channel)),
        )
        .await;
        let RosterRows::Consumers(rows) = &roster.rows else {
            panic!("{roster:?}")
        };
        let tap = rows
            .iter()
            .find(|c| on_our_channel(c))
            .unwrap_or_else(|| panic!("the tap's consumer is listed: {rows:?}"));
        assert!(tap.queue.starts_with("amq.gen-"), "{tap:?}");
        assert!(tap.tag.starts_with("keyhole-"), "{tap:?}");
        assert_eq!(tap.ack_required, Some(false), "the tap auto-acks");
    }

    // -- publish ----------------------------------------------------------

    /// Publish `body` through `conn`, as the browser does.
    async fn publish_via(
        conn: &mut RabbitmqConnection,
        spec: SubSpec,
        routing_key: &str,
        body: &str,
    ) -> anyhow::Result<()> {
        conn.publish(PublishReq {
            spec,
            routing_key: routing_key.to_string(),
            body: body.as_bytes().to_vec(),
        })
        .await
    }

    #[tokio::test]
    async fn a_message_published_to_a_queue_is_peeked_back() {
        let queue = unique("keyhole.it.pub");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;

        // No listing first: publishing needs only the AMQP connection.
        let mut conn = connected().await;
        publish_via(&mut conn, SubSpec::Queue(queue.clone()), &queue, "hello")
            .await
            .expect("confirmed");
        let events = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("peek");
        assert_eq!(bodies(&events), ["hello"]);
        let ev = &events[0];
        assert_eq!(ev.meta("exchange"), Some("(default)"));
        assert_eq!(ev.meta("routing_key"), Some(queue.as_str()));
        assert_eq!(ev.meta("delivery_mode"), Some("2 (persistent)"));
        assert_eq!(ev.meta("content_type"), None, "sent as typed");
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn a_routing_key_takes_an_exchange_publish_to_its_bound_queue() {
        let exchange = unique("keyhole.it.pubx");
        let queue = unique("keyhole.it.pubq");
        let _cleanup = Cleanup::queues(&[&queue]).and_exchanges(&[&exchange]);
        declare_topic_exchange(&exchange).await;
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;
        channel
            .queue_bind(
                queue.as_str().into(),
                exchange.as_str().into(),
                "order.*".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let mut conn = connected().await;
        let spec = SubSpec::Exchange {
            exchange: exchange.clone(),
            binding_key: "#".into(),
        };
        publish_via(&mut conn, spec, "order.test", r#"{"id":1}"#)
            .await
            .expect("confirmed");
        let events = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("peek");
        assert_eq!(bodies(&events), [r#"{"id":1}"#]);
        assert_eq!(events[0].meta("exchange"), Some(exchange.as_str()));
        assert_eq!(events[0].meta("routing_key"), Some("order.test"));
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn an_unroutable_publish_is_reported_and_the_channel_stays_usable() {
        let exchange = unique("keyhole.it.nobody");
        let _cleanup = Cleanup::exchanges(&[&exchange]);
        declare_topic_exchange(&exchange).await;
        let mut conn = connected().await;
        // The listing tells the connection which exchanges are topic ones.
        conn.list_destinations(false).await.expect("listed");
        let spec = SubSpec::Exchange {
            exchange: exchange.clone(),
            binding_key: "#".into(),
        };
        let err = publish_via(&mut conn, spec.clone(), "nobody.home", "lost")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "unroutable: no queue bound for key `nobody.home`"
        );
        let missing = unique("keyhole.it.missing");
        let err = publish_via(&mut conn, SubSpec::Queue(missing.clone()), &missing, "lost")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("unroutable: no queue named `{missing}`")
        );
        // A returned message leaves the channel open.
        let channel = conn.publish_channel.clone().expect("still open");
        assert!(channel.status().connected());
    }

    #[tokio::test]
    async fn a_queue_that_rejects_publishes_when_full_nacks_one() {
        let queue = unique("keyhole.it.full");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        let mut args = FieldTable::default();
        args.insert("x-max-length".into(), lapin::types::AMQPValue::LongInt(1));
        args.insert(
            "x-overflow".into(),
            lapin::types::AMQPValue::LongString("reject-publish".into()),
        );
        declare_queue(&channel, &queue, args).await;

        let mut conn = connected().await;
        let spec = SubSpec::Queue(queue.clone());
        publish_via(&mut conn, spec.clone(), &queue, "fits")
            .await
            .expect("room for one");
        let err = publish_via(&mut conn, spec.clone(), &queue, "overflows")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "NACKed: the broker didn't take the message"
        );
        setup.close(200, "bye".into()).await.ok();
    }

    #[tokio::test]
    async fn a_refused_publish_closes_only_its_channel_and_the_next_publish_works() {
        let queue = unique("keyhole.it.after");
        let _cleanup = Cleanup::queues(&[&queue]);
        let (setup, channel) = setup_channel().await;
        declare_queue(&channel, &queue, FieldTable::default()).await;

        let mut conn = connected().await;
        // Clients can't publish to an internal exchange: the broker closes the
        // channel with a 403.
        let trace = SubSpec::Exchange {
            exchange: "amq.rabbitmq.trace".into(),
            binding_key: "#".into(),
        };
        let err = publish_via(&mut conn, trace, "k", "refused")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("refused: ACCESS_REFUSED - cannot publish to internal exchange"),
            "{err}"
        );
        // The connection survives, and the next publish opens a new channel.
        conn.ping().await.expect("the connection is alive");
        publish_via(&mut conn, SubSpec::Queue(queue.clone()), &queue, "after")
            .await
            .expect("a new channel");
        let events = peek_queue(&mut conn, &queue, PeekMode::Browse)
            .await
            .expect("peek");
        assert_eq!(bodies(&events), ["after"]);
        setup.close(200, "bye".into()).await.ok();
    }
}
