//! The async↔UI boundary: the [`AppEvent`] enum plus the background tasks that
//! feed it. Tasks own no UI state — they only emit events into the channel that
//! the render loop drains. New variants (broker data, recording status, …)
//! arrive in later phases.

use std::time::Duration;

use crossterm::event::Event as CrosstermEvent;
use futures_util::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Everything the render loop reacts to.
#[derive(Debug)]
pub enum AppEvent {
    /// A terminal input event (key, resize, mouse, …).
    Input(CrosstermEvent),
    /// Periodic tick for clock/stat refresh and animations.
    Tick,
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
