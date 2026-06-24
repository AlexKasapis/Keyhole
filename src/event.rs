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
use crate::broker::{BrokerEvent, BrowsePage, ConnId, ServerStats, SubSpec, ValueView};
use crate::recording::RecordingStatus;

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
    /// The result of a queue peek (AMQP): a bounded batch of the messages
    /// currently in `spec`. `spec` lets the UI discard a stale peek (the user
    /// moved to another destination before it returned).
    Peeked {
        id: ConnId,
        spec: SubSpec,
        events: Vec<BrokerEvent>,
    },
    /// The result of a publish (AMQP): `Ok` when the broker accepted the
    /// message, `Err` with a reason otherwise. `target` is the destination label
    /// for the confirmation/failure status.
    Published {
        id: ConnId,
        target: String,
        result: Result<(), String>,
    },
    /// Refreshed server statistics.
    StatsUpdated { id: ConnId, stats: ServerStats },
    /// A non-fatal error from a connection operation.
    ConnError {
        id: ConnId,
        context: String,
        error: String,
    },
    /// A live event from a subscription/tail (lossy: high-rate path).
    Realtime {
        id: ConnId,
        sub_id: u32,
        event: BrokerEvent,
    },
    /// A subscription's tail is established and receiving.
    SubscriptionStarted { id: ConnId, sub_id: u32 },
    /// A non-fatal advisory for a tail (e.g. keyspace notifications disabled).
    SubscriptionNotice {
        id: ConnId,
        sub_id: u32,
        notice: String,
    },
    /// The result of a read-only command-console execution.
    CommandResult {
        id: ConnId,
        command: String,
        result: Result<String, String>,
    },
    /// A subscription's tail stopped (source closed, failed, or was stopped).
    SubscriptionEnded {
        id: ConnId,
        sub_id: u32,
        reason: Option<String>,
    },
    /// A change in a tail's recording (started/progress/stopped/failed).
    RecordingUpdate {
        id: ConnId,
        sub_id: u32,
        status: RecordingStatus,
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

#[cfg(test)]
mod tests {
    use super::{spawn_tick, AppEvent};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    #[tokio::test]
    async fn tick_task_emits_then_stops_on_cancel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        spawn_tick(tx, cancel.clone(), &tracker, Duration::from_millis(20));

        // The first interval tick fires immediately, so a Tick arrives promptly.
        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
        assert!(matches!(first, Ok(Some(AppEvent::Tick))), "expected a Tick");

        // Cancellation stops the task; the tracker then drains cleanly.
        cancel.cancel();
        tracker.close();
        tokio::time::timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("tick task stops on cancel");
    }
}
