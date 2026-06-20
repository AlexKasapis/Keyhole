//! The per-connection actor: a long-lived task that owns a
//! `Box<dyn BrokerConnection>` and serves [`ConnCommand`]s from the UI,
//! emitting results as [`AppEvent`]s. Driving every broker through this one
//! loop keeps the UI broker-agnostic and keeps all `!Sync` client state off the
//! render thread.
//!
//! Subscriptions are different from request/reply commands: each one opens a
//! dedicated tail (its own socket) and runs in a separate, per-tail-cancellable
//! task. The actor keeps a registry of live tails so it can toggle their
//! recording and stop them individually.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use futures_util::StreamExt;
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::{
    BrokerConnection, BrokerEventStream, BrowseReq, Capabilities, ConnId, InspectReq, SubSpec,
};
use crate::event::AppEvent;
use crate::recording::{RecordSink, Recorder, RecordingStatus, FLUSH_EVERY};

/// How often a recording is flushed regardless of volume.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// A command sent from the UI to a connection actor.
#[derive(Debug)]
pub enum ConnCommand {
    Browse(BrowseReq),
    Inspect(InspectReq),
    RefreshStats,
    Ping,
    /// Open a tail for `spec`; start recording immediately if `record`.
    Subscribe {
        sub_id: u32,
        spec: SubSpec,
        record: bool,
    },
    /// Toggle recording on an existing tail.
    SetRecording {
        sub_id: u32,
        on: bool,
    },
    /// Stop and drop a tail.
    StopSubscription {
        sub_id: u32,
    },
    /// Execute a read-only command in the console and return its rendered reply.
    Exec {
        command: String,
    },
}

/// A handle the UI keeps for an open connection. Cloneable-free: the UI owns
/// exactly one per connection and talks to the actor through `cmd_tx`.
#[derive(Debug)]
pub struct ConnHandle {
    pub id: ConnId,
    pub name: String,
    pub caps: Capabilities,
    cmd_tx: Sender<ConnCommand>,
    cancel: CancellationToken,
}

impl ConnHandle {
    /// Best-effort command send. Never blocks the render loop: if the actor's
    /// inbox is full or gone, the command is dropped (the UI stays responsive).
    pub fn send(&self, cmd: ConnCommand) {
        if let Err(err) = self.cmd_tx.try_send(cmd) {
            tracing::debug!(conn = self.id.0, %err, "dropped connection command");
        }
    }

    /// Stop the actor task (and, via cancellation, all its tails).
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// A live tail the actor is tracking.
struct SubEntry {
    /// Toggles the tail's recording on/off.
    record_tx: watch::Sender<bool>,
    /// Stops just this tail.
    cancel: CancellationToken,
}

/// Connect, then spawn the actor loop. Returns the handle once connected so the
/// caller learns capabilities (and connection errors) synchronously.
pub async fn spawn_connection(
    id: ConnId,
    name: String,
    mut conn: Box<dyn BrokerConnection>,
    events: Sender<AppEvent>,
    tracker: &TaskTracker,
    parent_cancel: &CancellationToken,
    recordings_dir: PathBuf,
) -> anyhow::Result<ConnHandle> {
    let caps = conn.connect().await?;
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ConnCommand>(64);
    let cancel = parent_cancel.child_token();
    let task_cancel = cancel.clone();
    let tail_tracker = tracker.clone();
    let actor_events = events.clone();
    let conn_name = name.clone();

    tracker.spawn(async move {
        let mut subs: HashMap<u32, SubEntry> = HashMap::new();
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => break,
                maybe_cmd = cmd_rx.recv() => match maybe_cmd {
                    Some(ConnCommand::Subscribe { sub_id, spec, record }) => {
                        start_subscription(
                            id, sub_id, spec, record, conn.as_mut(),
                            &actor_events, &tail_tracker, &task_cancel,
                            &recordings_dir, &conn_name, &mut subs,
                        )
                        .await;
                    }
                    Some(ConnCommand::SetRecording { sub_id, on }) => {
                        if let Some(entry) = subs.get(&sub_id) {
                            let _ = entry.record_tx.send(on);
                        }
                    }
                    Some(ConnCommand::StopSubscription { sub_id }) => {
                        if let Some(entry) = subs.remove(&sub_id) {
                            entry.cancel.cancel();
                        }
                    }
                    Some(cmd) => process(id, conn.as_mut(), cmd, &actor_events).await,
                    None => break,
                },
            }
        }
        // Stop any still-running tails on shutdown.
        for (_, entry) in subs.drain() {
            entry.cancel.cancel();
        }
        tracing::debug!(conn = id.0, "connection actor stopped");
    });

    Ok(ConnHandle {
        id,
        name,
        caps,
        cmd_tx,
        cancel,
    })
}

/// Open a tail for `spec` and register it. On failure, report it and register
/// nothing so the UI can mark the tab ended.
#[allow(clippy::too_many_arguments)]
async fn start_subscription(
    id: ConnId,
    sub_id: u32,
    spec: SubSpec,
    record: bool,
    conn: &mut dyn BrokerConnection,
    events: &Sender<AppEvent>,
    tracker: &TaskTracker,
    conn_cancel: &CancellationToken,
    recordings_dir: &std::path::Path,
    connection: &str,
    subs: &mut HashMap<u32, SubEntry>,
) {
    match conn.subscribe(spec.clone()).await {
        Ok(stream) => {
            // A non-fatal advisory (e.g. keyspace notifications disabled) is
            // surfaced once, before the tail starts streaming. UI-only — it is
            // never written to the recording.
            let notice = conn.tail_notice(&spec).await;
            let (record_tx, record_rx) = watch::channel(record);
            let cancel = conn_cancel.child_token();
            let params = TailParams {
                id,
                sub_id,
                spec,
                events: events.clone(),
                recordings_dir: recordings_dir.to_path_buf(),
                connection: connection.to_string(),
            };
            tracker.spawn(run_tail(params, stream, record_rx, cancel.clone()));
            subs.insert(sub_id, SubEntry { record_tx, cancel });
            let _ = events
                .send(AppEvent::SubscriptionStarted { id, sub_id })
                .await;
            if let Some(notice) = notice {
                let _ = events
                    .send(AppEvent::SubscriptionNotice { id, sub_id, notice })
                    .await;
            }
        }
        Err(e) => {
            let _ = events
                .send(AppEvent::SubscriptionEnded {
                    id,
                    sub_id,
                    reason: Some(e.to_string()),
                })
                .await;
        }
    }
}

/// Static context for a tail task (the parts that don't change per event).
struct TailParams {
    id: ConnId,
    sub_id: u32,
    spec: SubSpec,
    events: Sender<AppEvent>,
    recordings_dir: PathBuf,
    connection: String,
}

/// Drive one tail: forward events to the UI (lossy), record them when recording
/// is on (lossless), and flush periodically. Ends on cancel or source close.
async fn run_tail(
    p: TailParams,
    mut stream: BrokerEventStream,
    mut record_rx: watch::Receiver<bool>,
    cancel: CancellationToken,
) {
    let mut recorder: Option<(Recorder<RecordSink>, PathBuf)> = None;
    let mut flush = tokio::time::interval(FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    if *record_rx.borrow_and_update() {
        recorder = open_recorder(&p).await;
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            changed = record_rx.changed() => {
                if changed.is_err() {
                    break; // sender dropped -> the actor is tearing this tail down
                }
                let on = *record_rx.borrow();
                match (on, recorder.is_some()) {
                    (true, false) => recorder = open_recorder(&p).await,
                    (false, true) => close_recorder(&p, &mut recorder).await,
                    _ => {}
                }
            }
            _ = flush.tick() => {
                if let Some((rec, _)) = &mut recorder {
                    let _ = rec.flush();
                }
            }
            item = stream.next() => match item {
                Some(ev) => {
                    if let Some((rec, _)) = &mut recorder {
                        match rec.record(&ev) {
                            Ok(()) => {
                                if rec.records().is_multiple_of(FLUSH_EVERY) {
                                    let _ = rec.flush();
                                    let _ = p.events.try_send(AppEvent::RecordingUpdate {
                                        id: p.id,
                                        sub_id: p.sub_id,
                                        status: RecordingStatus::Progress {
                                            records: rec.records(),
                                            bytes: rec.bytes(),
                                        },
                                    });
                                }
                            }
                            Err(e) => {
                                let _ = p.events.send(AppEvent::RecordingUpdate {
                                    id: p.id,
                                    sub_id: p.sub_id,
                                    status: RecordingStatus::Failed { error: e.to_string() },
                                }).await;
                                recorder = None;
                            }
                        }
                    }
                    // Lossy forward: the UI may drop under a firehose; the recorder never does.
                    let _ = p.events.try_send(AppEvent::Realtime {
                        id: p.id,
                        sub_id: p.sub_id,
                        event: ev,
                    });
                }
                None => {
                    let _ = p.events.send(AppEvent::SubscriptionEnded {
                        id: p.id,
                        sub_id: p.sub_id,
                        reason: Some("source closed".to_string()),
                    }).await;
                    break;
                }
            },
        }
    }
    close_recorder(&p, &mut recorder).await;
}

/// Open a fresh recording file for this tail and announce it.
async fn open_recorder(p: &TailParams) -> Option<(Recorder<RecordSink>, PathBuf)> {
    match RecordSink::create(
        &p.recordings_dir,
        &p.connection,
        &p.spec,
        OffsetDateTime::now_utc(),
    ) {
        Ok(sink) => {
            let path = sink.path().to_path_buf();
            let _ = p
                .events
                .send(AppEvent::RecordingUpdate {
                    id: p.id,
                    sub_id: p.sub_id,
                    status: RecordingStatus::Started { path: path.clone() },
                })
                .await;
            Some((Recorder::new(sink, p.connection.clone(), &p.spec), path))
        }
        Err(e) => {
            let _ = p
                .events
                .send(AppEvent::RecordingUpdate {
                    id: p.id,
                    sub_id: p.sub_id,
                    status: RecordingStatus::Failed {
                        error: e.to_string(),
                    },
                })
                .await;
            None
        }
    }
}

/// Flush and close the current recording (if any), announcing final counters.
async fn close_recorder(p: &TailParams, recorder: &mut Option<(Recorder<RecordSink>, PathBuf)>) {
    if let Some((mut rec, path)) = recorder.take() {
        let _ = rec.flush();
        let _ = p
            .events
            .send(AppEvent::RecordingUpdate {
                id: p.id,
                sub_id: p.sub_id,
                status: RecordingStatus::Stopped {
                    records: rec.records(),
                    bytes: rec.bytes(),
                    path,
                },
            })
            .await;
    }
}

async fn process(
    id: ConnId,
    conn: &mut dyn BrokerConnection,
    cmd: ConnCommand,
    events: &Sender<AppEvent>,
) {
    let result = match cmd {
        ConnCommand::Browse(req) => match conn.browse(req).await {
            Ok(page) => events.send(AppEvent::KeysPage { id, page }).await,
            Err(e) => emit_error(events, id, "browse", e).await,
        },
        ConnCommand::Inspect(req) => {
            let key = req.key.clone();
            match conn.inspect(req).await {
                Ok(value) => events.send(AppEvent::ValueLoaded { id, key, value }).await,
                Err(e) => emit_error(events, id, "inspect", e).await,
            }
        }
        ConnCommand::RefreshStats => match conn.stats().await {
            Ok(stats) => events.send(AppEvent::StatsUpdated { id, stats }).await,
            Err(e) => emit_error(events, id, "stats", e).await,
        },
        ConnCommand::Ping => match conn.ping().await {
            Ok(()) => Ok(()),
            Err(e) => {
                events
                    .send(AppEvent::Disconnected {
                        id,
                        reason: e.to_string(),
                    })
                    .await
            }
        },
        ConnCommand::Exec { command } => {
            let result = conn
                .exec_readonly(&command)
                .await
                .map_err(|e| e.to_string());
            events
                .send(AppEvent::CommandResult {
                    id,
                    command,
                    result,
                })
                .await
        }
        // Subscription commands are intercepted by the actor loop before
        // `process` is ever reached (they mutate the live-tail registry), so this
        // arm documents that invariant rather than silently no-op'ing.
        ConnCommand::Subscribe { .. }
        | ConnCommand::SetRecording { .. }
        | ConnCommand::StopSubscription { .. } => {
            unreachable!("subscription commands are handled in the actor loop")
        }
    };
    // A send error means the UI is gone; the actor will stop on cancellation.
    let _ = result;
}

async fn emit_error(
    events: &Sender<AppEvent>,
    id: ConnId,
    context: &str,
    error: anyhow::Error,
) -> Result<(), tokio::sync::mpsc::error::SendError<AppEvent>> {
    events
        .send(AppEvent::ConnError {
            id,
            context: context.to_string(),
            error: error.to_string(),
        })
        .await
}

#[cfg(test)]
pub(crate) mod mock {
    //! A configurable in-memory [`BrokerConnection`] for unit tests, plus a
    //! [`handle`] helper that spawns a mock-backed connection actor. Used here
    //! and by the `app`/`ui` test suites to obtain a real [`ConnHandle`] without
    //! a live broker.

    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    use super::{spawn_connection, ConnHandle};
    use crate::broker::{
        BrokerConnection, BrokerEvent, BrokerEventStream, BrowsePage, BrowseReq, Capabilities,
        ConnId, InspectReq, ServerStats, SubSpec, ValueView,
    };
    use crate::event::AppEvent;

    /// How a mock subscription behaves when `subscribe` is called.
    pub enum SubBehavior {
        /// Yield these events in order, then end (actor reports "source closed").
        Once(Vec<BrokerEvent>),
        /// Pull events from this receiver until its sender is dropped/closed.
        Channel(mpsc::Receiver<BrokerEvent>),
        /// Fail to subscribe with this message.
        Fail(String),
    }

    /// A [`BrokerConnection`] whose every operation is scripted by its fields.
    pub struct MockBroker {
        pub databases: u32,
        /// Report AMQP capabilities (no browse/dashboard/console) on connect.
        pub amqp: bool,
        pub connect_err: Option<String>,
        pub ping_err: Option<String>,
        pub browse: Option<BrowsePage>,
        pub browse_err: Option<String>,
        pub inspect: ValueView,
        pub stats: ServerStats,
        pub sub: Option<SubBehavior>,
        /// Advisory returned from `tail_notice` (e.g. keyspace notifications off).
        pub notice: Option<String>,
    }

    impl MockBroker {
        /// A mock that succeeds at everything, reporting `databases` databases.
        pub fn new(databases: u32) -> Self {
            Self {
                databases,
                amqp: false,
                connect_err: None,
                ping_err: None,
                browse: None,
                browse_err: None,
                inspect: ValueView::Missing,
                stats: ServerStats::default(),
                sub: None,
                notice: None,
            }
        }

        pub fn boxed(self) -> Box<dyn BrokerConnection> {
            Box::new(self)
        }
    }

    #[async_trait]
    impl BrokerConnection for MockBroker {
        async fn connect(&mut self) -> anyhow::Result<Capabilities> {
            match &self.connect_err {
                Some(e) => anyhow::bail!("{e}"),
                None if self.amqp => Ok(Capabilities::amqp()),
                None => Ok(Capabilities::redis(self.databases)),
            }
        }

        async fn ping(&mut self) -> anyhow::Result<()> {
            match &self.ping_err {
                Some(e) => anyhow::bail!("{e}"),
                None => Ok(()),
            }
        }

        async fn browse(&mut self, req: BrowseReq) -> anyhow::Result<BrowsePage> {
            if let Some(e) = &self.browse_err {
                anyhow::bail!("{e}");
            }
            Ok(self.browse.clone().unwrap_or(BrowsePage {
                db: req.db,
                entries: Vec::new(),
                next_cursor: 0,
            }))
        }

        async fn inspect(&mut self, _req: InspectReq) -> anyhow::Result<ValueView> {
            Ok(self.inspect.clone())
        }

        async fn stats(&mut self) -> anyhow::Result<ServerStats> {
            Ok(self.stats.clone())
        }

        async fn subscribe(&mut self, _spec: SubSpec) -> anyhow::Result<BrokerEventStream> {
            match self.sub.take().unwrap_or(SubBehavior::Once(Vec::new())) {
                SubBehavior::Fail(e) => anyhow::bail!("{e}"),
                SubBehavior::Once(events) => Ok(Box::pin(futures_util::stream::iter(events))),
                SubBehavior::Channel(rx) => Ok(Box::pin(futures_util::stream::unfold(
                    rx,
                    |mut rx| async move { rx.recv().await.map(|ev| (ev, rx)) },
                ))),
            }
        }

        async fn tail_notice(&mut self, _spec: &SubSpec) -> Option<String> {
            self.notice.clone()
        }
    }

    /// Spawn a default mock-backed connection actor and return its live handle.
    /// The actor's emitted events are drained into the void (callers that use
    /// this assert on `App` state, not actor output), so commands never block.
    pub async fn handle(id: u32, name: &str, databases: u32) -> ConnHandle {
        let (tx, mut rx) = mpsc::channel::<AppEvent>(256);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let tracker = TaskTracker::new();
        let cancel = CancellationToken::new();
        spawn_connection(
            ConnId(id),
            name.to_string(),
            MockBroker::new(databases).boxed(),
            tx,
            &tracker,
            &cancel,
            std::env::temp_dir(),
        )
        .await
        .expect("mock connect")
    }

    /// Like [`handle`], but the mock reports AMQP capabilities (no browse /
    /// dashboard / console), for testing capability-gated UI behaviour.
    pub async fn amqp_handle(id: u32, name: &str) -> ConnHandle {
        let (tx, mut rx) = mpsc::channel::<AppEvent>(256);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let tracker = TaskTracker::new();
        let cancel = CancellationToken::new();
        let mut mock = MockBroker::new(1);
        mock.amqp = true;
        spawn_connection(
            ConnId(id),
            name.to_string(),
            mock.boxed(),
            tx,
            &tracker,
            &cancel,
            std::env::temp_dir(),
        )
        .await
        .expect("mock amqp connect")
    }
}

#[cfg(test)]
mod tests {
    use super::mock::{MockBroker, SubBehavior};
    use super::*;
    use crate::broker::{
        BrokerEvent, BrowsePage, BrowseReq, EntryMeta, InspectReq, Payload, PayloadEncoding,
        ServerStats, SubSpec, Ttl, ValueType, ValueView,
    };
    use crate::recording::RecordingStatus;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use time::OffsetDateTime;
    use tokio::sync::mpsc::{self, Receiver};

    /// A unique temp directory for a recording test (cleaned up by the caller).
    fn temp_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("brokertui-actor-{tag}-{}-{n}", std::process::id()))
    }

    fn ev(source: &str, body: &str) -> BrokerEvent {
        BrokerEvent {
            ts: OffsetDateTime::UNIX_EPOCH,
            source: source.to_string(),
            payload: Payload::Utf8(body.to_string()),
            meta: Vec::new(),
        }
    }

    /// Spawn an actor from `mock`, returning its handle, event receiver, and the
    /// tracker/cancel keeping it alive (held by the caller so they don't drop).
    async fn spawn(
        mock: MockBroker,
        dir: PathBuf,
    ) -> (
        ConnHandle,
        Receiver<AppEvent>,
        TaskTracker,
        CancellationToken,
    ) {
        let (tx, rx) = mpsc::channel::<AppEvent>(256);
        let tracker = TaskTracker::new();
        let cancel = CancellationToken::new();
        let handle = spawn_connection(
            ConnId(1),
            "c".into(),
            mock.boxed(),
            tx,
            &tracker,
            &cancel,
            dir,
        )
        .await
        .expect("connect");
        (handle, rx, tracker, cancel)
    }

    /// Next event within a generous timeout, or `None` if the actor stays silent.
    async fn next(rx: &mut Receiver<AppEvent>) -> Option<AppEvent> {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .ok()
            .flatten()
    }

    /// Drain every event until the channel goes quiet for `ms` milliseconds.
    async fn drain_for(rx: &mut Receiver<AppEvent>, ms: u64) -> Vec<AppEvent> {
        let mut out = Vec::new();
        while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(ms), rx.recv()).await {
            out.push(e);
        }
        out
    }

    /// Pump events until one matches `pred` (returns true) or the actor goes
    /// silent (returns false). Lets a test synchronize on an observable effect.
    async fn wait_for(rx: &mut Receiver<AppEvent>, pred: impl Fn(&AppEvent) -> bool) -> bool {
        while let Some(e) = next(rx).await {
            if pred(&e) {
                return true;
            }
        }
        false
    }

    #[tokio::test]
    async fn connect_failure_is_surfaced() {
        let mut mock = MockBroker::new(1);
        mock.connect_err = Some("refused".into());
        let (tx, _rx) = mpsc::channel::<AppEvent>(8);
        let tracker = TaskTracker::new();
        let cancel = CancellationToken::new();
        let result = spawn_connection(
            ConnId(1),
            "c".into(),
            mock.boxed(),
            tx,
            &tracker,
            &cancel,
            std::env::temp_dir(),
        )
        .await;
        assert!(
            result.is_err(),
            "connect error must propagate to the caller"
        );
    }

    #[tokio::test]
    async fn browse_emits_keys_page() {
        let mut mock = MockBroker::new(16);
        mock.browse = Some(BrowsePage {
            db: 2,
            entries: vec![EntryMeta {
                key: "k".into(),
                vtype: ValueType::String,
                ttl: Ttl::NoExpire,
            }],
            next_cursor: 7,
        });
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("browse")).await;
        handle.send(ConnCommand::Browse(BrowseReq {
            db: 2,
            pattern: "*".into(),
            cursor: 0,
            page_size: 10,
        }));
        match next(&mut rx).await {
            Some(AppEvent::KeysPage { id, page }) => {
                assert_eq!(id, ConnId(1));
                assert_eq!(page.db, 2);
                assert_eq!(page.entries.len(), 1);
                assert_eq!(page.next_cursor, 7);
            }
            other => panic!("expected KeysPage, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn browse_error_emits_conn_error() {
        let mut mock = MockBroker::new(1);
        mock.browse_err = Some("boom".into());
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("browse-err")).await;
        handle.send(ConnCommand::Browse(BrowseReq {
            db: 0,
            pattern: "*".into(),
            cursor: 0,
            page_size: 10,
        }));
        match next(&mut rx).await {
            Some(AppEvent::ConnError { context, error, .. }) => {
                assert_eq!(context, "browse");
                assert!(error.contains("boom"), "error was {error:?}");
            }
            other => panic!("expected ConnError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn inspect_emits_value_loaded() {
        let mut mock = MockBroker::new(1);
        mock.inspect = ValueView::Str {
            total_bytes: 1,
            shown_bytes: 1,
            text: "x".into(),
            encoding: PayloadEncoding::Utf8,
        };
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("inspect")).await;
        handle.send(ConnCommand::Inspect(InspectReq {
            db: 0,
            key: "k".into(),
            offset: 0,
            limit: 10,
        }));
        match next(&mut rx).await {
            Some(AppEvent::ValueLoaded { key, .. }) => assert_eq!(key, "k"),
            other => panic!("expected ValueLoaded, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn stats_emits_stats_updated() {
        let mut mock = MockBroker::new(1);
        mock.stats = ServerStats {
            redis_version: Some("9.9".into()),
            ..Default::default()
        };
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("stats")).await;
        handle.send(ConnCommand::RefreshStats);
        match next(&mut rx).await {
            Some(AppEvent::StatsUpdated { stats, .. }) => {
                assert_eq!(stats.redis_version.as_deref(), Some("9.9"));
            }
            other => panic!("expected StatsUpdated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ping_ok_is_silent() {
        let (handle, mut rx, _t, _c) = spawn(MockBroker::new(1), temp_dir("ping-ok")).await;
        handle.send(ConnCommand::Ping);
        // A healthy ping emits nothing; the channel should stay quiet.
        let quiet = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
        assert!(quiet.is_err(), "ping success must not emit an event");
    }

    #[tokio::test]
    async fn ping_failure_emits_disconnected() {
        let mut mock = MockBroker::new(1);
        mock.ping_err = Some("dead".into());
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("ping-fail")).await;
        handle.send(ConnCommand::Ping);
        match next(&mut rx).await {
            Some(AppEvent::Disconnected { reason, .. }) => {
                assert!(reason.contains("dead"), "reason was {reason:?}");
            }
            other => panic!("expected Disconnected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribe_forwards_events_then_ends() {
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Once(vec![ev("c", "a"), ev("c", "b")]));
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("sub")).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 5,
            spec: SubSpec::Channel("c".into()),
            record: false,
        });
        let events = drain_for(&mut rx, 300).await;
        let started = events
            .iter()
            .filter(|e| matches!(e, AppEvent::SubscriptionStarted { sub_id, .. } if *sub_id == 5))
            .count();
        let realtime = events
            .iter()
            .filter(|e| matches!(e, AppEvent::Realtime { sub_id, .. } if *sub_id == 5))
            .count();
        let ended = events
            .iter()
            .filter(|e| {
                matches!(e, AppEvent::SubscriptionEnded { reason: Some(r), .. } if r == "source closed")
            })
            .count();
        assert_eq!(started, 1, "one SubscriptionStarted");
        assert_eq!(realtime, 2, "both events forwarded");
        assert_eq!(ended, 1, "ends with 'source closed'");
    }

    #[tokio::test]
    async fn subscribe_failure_emits_ended() {
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Fail("nope".into()));
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("sub-fail")).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 1,
            spec: SubSpec::Channel("c".into()),
            record: false,
        });
        match next(&mut rx).await {
            Some(AppEvent::SubscriptionEnded { sub_id, reason, .. }) => {
                assert_eq!(sub_id, 1);
                assert!(
                    reason.as_deref().unwrap_or("").contains("nope"),
                    "reason was {reason:?}"
                );
            }
            other => panic!("expected SubscriptionEnded, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn recording_writes_jsonl_file() {
        let dir = temp_dir("rec");
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Once(vec![
            ev("rec", "a"),
            ev("rec", "b"),
            ev("rec", "cc"),
        ]));
        let (handle, mut rx, _t, _c) = spawn(mock, dir.clone()).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 1,
            spec: SubSpec::Channel("rec-chan".into()),
            record: true,
        });
        let events = drain_for(&mut rx, 400).await;

        assert!(
            events.iter().any(|e| matches!(
                e,
                AppEvent::RecordingUpdate {
                    status: RecordingStatus::Started { .. },
                    ..
                }
            )),
            "recording should announce Started"
        );
        let stopped_records = events.iter().find_map(|e| match e {
            AppEvent::RecordingUpdate {
                status: RecordingStatus::Stopped { records, .. },
                ..
            } => Some(*records),
            _ => None,
        });
        assert_eq!(stopped_records, Some(3), "all three events recorded");

        let files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("jsonl"))
            .collect();
        assert_eq!(files.len(), 1, "exactly one recording file written");
        let content = std::fs::read_to_string(&files[0]).unwrap();
        assert_eq!(content.lines().count(), 3, "one JSONL line per event");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_recording_toggles_recorder() {
        let dir = temp_dir("toggle");
        let (tx_ev, rx_ev) = mpsc::channel::<BrokerEvent>(16);
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Channel(rx_ev));
        let (handle, mut rx, _t, _c) = spawn(mock, dir.clone()).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 1,
            spec: SubSpec::Channel("c".into()),
            record: false,
        });
        // Toggle recording on, then wait for the recorder to actually open
        // before toggling off — the watch channel coalesces rapid changes, so
        // synchronizing on the Started effect keeps the toggle deterministic.
        handle.send(ConnCommand::SetRecording {
            sub_id: 1,
            on: true,
        });
        assert!(
            wait_for(&mut rx, |e| matches!(
                e,
                AppEvent::RecordingUpdate {
                    status: RecordingStatus::Started { .. },
                    ..
                }
            ))
            .await,
            "turning recording on opens a recorder"
        );
        tx_ev.send(ev("c", "hello")).await.unwrap();
        handle.send(ConnCommand::SetRecording {
            sub_id: 1,
            on: false,
        });
        assert!(
            wait_for(&mut rx, |e| matches!(
                e,
                AppEvent::RecordingUpdate {
                    status: RecordingStatus::Stopped { .. },
                    ..
                }
            ))
            .await,
            "turning recording off closes the recorder"
        );
        drop(tx_ev); // end the stream

        let wrote_file = std::fs::read_dir(&dir)
            .map(|rd| rd.flatten().any(|e| e.path().extension().is_some()))
            .unwrap_or(false);
        assert!(wrote_file, "a recording file should have been created");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stop_subscription_halts_forwarding() {
        let (tx_ev, rx_ev) = mpsc::channel::<BrokerEvent>(16);
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Channel(rx_ev));
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("stop")).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 1,
            spec: SubSpec::Channel("c".into()),
            record: false,
        });
        tx_ev.send(ev("c", "one")).await.unwrap();
        let before = drain_for(&mut rx, 200).await;
        assert!(
            before
                .iter()
                .any(|e| matches!(e, AppEvent::Realtime { .. })),
            "the first event forwards before stopping"
        );

        handle.send(ConnCommand::StopSubscription { sub_id: 1 });
        tokio::time::sleep(Duration::from_millis(100)).await; // let the cancel land
        let _ = tx_ev.send(ev("c", "two")).await; // ignored: tail is gone
        let after = drain_for(&mut rx, 200).await;
        assert!(
            !after.iter().any(|e| matches!(e, AppEvent::Realtime { .. })),
            "no events forward after StopSubscription, got {after:?}"
        );
    }

    #[tokio::test]
    async fn parent_cancel_tears_down_live_tail() {
        // Cancelling the parent token must stop the actor AND drain its live
        // tails (the `subs.drain()` teardown path), not just the command loop.
        let (tx_ev, rx_ev) = mpsc::channel::<BrokerEvent>(16);
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Channel(rx_ev));
        let (handle, mut rx, _t, cancel) = spawn(mock, temp_dir("pcancel")).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 1,
            spec: SubSpec::Channel("c".into()),
            record: false,
        });
        tx_ev.send(ev("c", "one")).await.unwrap();
        assert!(
            wait_for(&mut rx, |e| matches!(e, AppEvent::Realtime { .. })).await,
            "the first event forwards before cancelling"
        );

        cancel.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await; // let the cancel land
        let _ = tx_ev.send(ev("c", "two")).await; // ignored: the tail is gone
        let after = drain_for(&mut rx, 200).await;
        assert!(
            !after.iter().any(|e| matches!(e, AppEvent::Realtime { .. })),
            "no events forward after parent cancel, got {after:?}"
        );
    }

    #[tokio::test]
    async fn subscribe_emits_tail_notice() {
        // A broker advisory (e.g. keyspace notifications disabled) surfaces once
        // as a SubscriptionNotice after the tail starts.
        let mut mock = MockBroker::new(1);
        mock.sub = Some(SubBehavior::Once(Vec::new()));
        mock.notice = Some("notifications disabled".into());
        let (handle, mut rx, _t, _c) = spawn(mock, temp_dir("notice")).await;
        handle.send(ConnCommand::Subscribe {
            sub_id: 9,
            spec: SubSpec::Keyspace { db: 0 },
            record: false,
        });
        assert!(
            wait_for(&mut rx, |e| {
                matches!(e, AppEvent::SubscriptionNotice { sub_id, notice, .. }
                    if *sub_id == 9 && notice.contains("disabled"))
            })
            .await,
            "tail_notice should surface as a SubscriptionNotice"
        );
    }

    #[tokio::test]
    async fn shutdown_stops_the_actor() {
        let (handle, _rx, tracker, _c) = spawn(MockBroker::new(1), temp_dir("shutdown")).await;
        handle.shutdown();
        tracker.close();
        let stopped = tokio::time::timeout(Duration::from_secs(2), tracker.wait()).await;
        assert!(stopped.is_ok(), "actor task must stop after shutdown");
    }
}
