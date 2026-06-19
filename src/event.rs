//! The async↔UI boundary: the [`AppEvent`] enum plus the background tasks that
//! feed it. Tasks own no UI state — they only emit events into the channel that
//! the render loop drains. Broker results arrive here from connection actors.

use std::time::Duration;

use crossterm::event::Event as CrosstermEvent;
use futures_util::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::broker::actor::ConnHandle;
use crate::broker::{BrowsePage, ConnId, ServerStats, ValueView};

/// Everything the render loop reacts to.
#[derive(Debug)]
pub enum AppEvent {
    /// A terminal input event (key, resize, mouse, …).
    Input(CrosstermEvent),
    /// Periodic tick for clock/stat refresh and animations.
    Tick,
    /// A connection finished connecting; carries its handle (with capabilities).
    Connected { handle: ConnHandle },
    /// A connection dropped or failed liveness.
    Disconnected { id: ConnId, reason: String },
    /// A page of browse results.
    KeysPage { id: ConnId, page: BrowsePage },
    /// A key's inspected value.
    ValueLoaded {
        id: ConnId,
        key: String,
        value: ValueView,
    },
    /// Refreshed server statistics.
    StatsUpdated { id: ConnId, stats: ServerStats },
    /// A non-fatal error from a connection operation.
    ConnError {
        id: ConnId,
        context: String,
        error: String,
    },
}

/// Forward terminal input events into the channel until cancelled.
pub fn spawn_input(tx: Sender<AppEvent>, cancel: CancellationToken, tracker: &TaskTracker) {
    tracker.spawn(async move {
        let mut reader = crossterm::event::EventStream::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                maybe_event = reader.next() => match maybe_event {
                    Some(Ok(event)) => {
                        if tx.send(AppEvent::Input(event)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(err)) => tracing::warn!(error = %err, "terminal input error"),
                    None => break,
                },
            }
        }
    });
}

/// Emit a [`AppEvent::Tick`] on a fixed interval until cancelled.
pub fn spawn_tick(
    tx: Sender<AppEvent>,
    cancel: CancellationToken,
    tracker: &TaskTracker,
    period: Duration,
) {
    tracker.spawn(async move {
        let mut interval = tokio::time::interval(period);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if tx.send(AppEvent::Tick).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
}
