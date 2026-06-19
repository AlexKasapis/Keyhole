//! The per-connection actor: a long-lived task that owns a
//! `Box<dyn BrokerConnection>` and serves [`ConnCommand`]s from the UI,
//! emitting results as [`AppEvent`]s. Driving every broker through this one
//! loop keeps the UI broker-agnostic and keeps all `!Sync` client state off the
//! render thread.

use tokio::sync::mpsc::{self, Sender};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::{BrokerConnection, BrowseReq, Capabilities, ConnId, InspectReq};
use crate::event::AppEvent;

/// A command sent from the UI to a connection actor.
#[derive(Debug)]
pub enum ConnCommand {
    Browse(BrowseReq),
    Inspect(InspectReq),
    RefreshStats,
    Ping,
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

    /// Stop the actor task.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
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
) -> anyhow::Result<ConnHandle> {
    let caps = conn.connect().await?;
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ConnCommand>(64);
    let cancel = parent_cancel.child_token();
    let task_cancel = cancel.clone();

    tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => break,
                maybe_cmd = cmd_rx.recv() => match maybe_cmd {
                    Some(cmd) => process(id, conn.as_mut(), cmd, &events).await,
                    None => break,
                },
            }
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
