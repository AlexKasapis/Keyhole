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
        // Subscription commands are handled in the actor loop (they need state).
        ConnCommand::Subscribe { .. }
        | ConnCommand::SetRecording { .. }
        | ConnCommand::StopSubscription { .. } => Ok(()),
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
