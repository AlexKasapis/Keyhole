//! The spy queues Keyhole's own exchange taps declare, so RabbitMQ's
//! destination list can leave them out.
//!
//! A spy queue is server-named (`amq.gen-…`), exclusive and auto-delete,
//! which is also how many other clients' private queues look, so it can't be
//! told apart by its name or flags. Keyhole remembers the names its taps were
//! given instead, in one [`TapQueues`] the app shares with every RabbitMQ
//! connection. The broker never reuses a generated name, and clients can't
//! declare `amq.*` ones, so the name alone identifies a spy queue on any
//! broker or vhost.
//!
//! Timing:
//! - A name is noted when the broker's declare-ok reaches Keyhole, before the
//!   tap is handed back. A connection runs one command at a time, so none of
//!   its own listings can fall in between. Another connection's can: a listing
//!   the broker answers in the round trip between creating the queue and the
//!   declare-ok arriving shows the queue until that connection's next refresh.
//! - The broker deletes a closed tap's queue once it sees the tap's connection
//!   close: at once on a clean close, or when missed heartbeats reap a dead
//!   connection. The name stays hidden for [`RETENTION`] after the tap closes,
//!   well past either, and is dropped the next time the registry is used.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::broker::{DestKind, DestinationList};

/// How long a closed tap's queue stays hidden: well past the broker deleting
/// it, even after reaping a connection that died without closing. A broker
/// with heartbeats off may keep a dead tap's queue longer, and then it shows
/// in the list again; it really is still there.
const RETENTION: Duration = Duration::from_secs(10 * 60);

/// The spy queues of this session's taps, by name.
#[derive(Default)]
pub struct TapQueues {
    /// When each queue's tap closed; `None` while it's open.
    queues: Mutex<HashMap<String, Option<Instant>>>,
}

impl TapQueues {
    /// Note `name`, a spy queue just declared, as an open tap's until the
    /// returned guard drops.
    pub(super) fn register(self: &Arc<Self>, name: &str) -> TapQueueGuard {
        self.note_at(name, Instant::now());
        TapQueueGuard {
            queues: Arc::clone(self),
            name: name.to_string(),
        }
    }

    /// Note `name` as an open tap's at `now`.
    fn note_at(&self, name: &str, now: Instant) {
        let mut queues = self.lock();
        drop_expired(&mut queues, now);
        queues.insert(name.to_string(), None);
    }

    /// Take the spy queues out of `list`.
    pub(super) fn hide(&self, list: &mut DestinationList) {
        self.hide_at(list, Instant::now());
    }

    /// [`Self::hide`] at `now`.
    fn hide_at(&self, list: &mut DestinationList, now: Instant) {
        let mut queues = self.lock();
        drop_expired(&mut queues, now);
        if queues.is_empty() {
            return;
        }
        list.items
            .retain(|d| d.kind != DestKind::Queue || !queues.contains_key(&d.name));
    }

    /// Mark `name`'s tap closed at `now`, which its retention counts from.
    fn close_at(&self, name: &str, now: Instant) {
        if let Some(closed) = self.lock().get_mut(name) {
            *closed = Some(now);
        }
    }

    /// The spy queues noted, open or not.
    #[cfg(test)]
    pub(super) fn names(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Option<Instant>>> {
        // Every update is a single map operation, so a panic elsewhere can't
        // leave the map half-changed.
        self.queues.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Forget the queues whose taps closed [`RETENTION`] or more before `now`.
fn drop_expired(queues: &mut HashMap<String, Option<Instant>>, now: Instant) {
    queues.retain(|_, closed| closed.is_none_or(|t| now.saturating_duration_since(t) < RETENTION));
}

/// Marks its spy queue's tap closed when dropped: when the tap ends, or when
/// it fails after declaring the queue.
pub(super) struct TapQueueGuard {
    queues: Arc<TapQueues>,
    name: String,
}

impl Drop for TapQueueGuard {
    fn drop(&mut self) {
        self.queues.close_at(&self.name, Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::actor::mock::{exchange_row, queue_row};
    use crate::broker::{ExchangeType, QueueType};

    /// A listing of these queues, plus an exchange.
    fn listing(queues: &[&str]) -> DestinationList {
        let mut items: Vec<_> = queues
            .iter()
            .map(|q| queue_row(q, QueueType::Classic, 0))
            .collect();
        items.push(exchange_row("amq.topic", ExchangeType::Topic));
        DestinationList {
            items,
            ..Default::default()
        }
    }

    /// What `taps` leaves of a listing of `queues` at `now`.
    fn shown_at(taps: &TapQueues, queues: &[&str], now: Instant) -> Vec<String> {
        let mut list = listing(queues);
        taps.hide_at(&mut list, now);
        list.items.into_iter().map(|d| d.name).collect()
    }

    fn shown(taps: &TapQueues, queues: &[&str]) -> Vec<String> {
        shown_at(taps, queues, Instant::now())
    }

    #[test]
    fn an_open_taps_queue_is_hidden_and_other_clients_queues_stay() {
        let taps = Arc::new(TapQueues::default());
        let _tap = taps.register("amq.gen-ours");
        assert_eq!(
            shown(&taps, &["amq.gen-ours", "amq.gen-theirs", "orders"]),
            ["amq.gen-theirs", "orders", "amq.topic"]
        );
        // However long it stays open.
        let later = Instant::now() + 2 * RETENTION;
        assert_eq!(shown_at(&taps, &["amq.gen-ours"], later), ["amq.topic"]);
    }

    #[test]
    fn a_closed_or_failed_taps_queue_stays_hidden_until_it_expires() {
        // A tap that fails after declaring its queue drops the guard just as
        // a tap that ends does.
        let taps = Arc::new(TapQueues::default());
        drop(taps.register("amq.gen-ours"));
        let closed = Instant::now();
        // Gone from a listing or not, it's kept until it expires.
        assert_eq!(shown(&taps, &[]), ["amq.topic"]);
        let almost = closed + RETENTION - Duration::from_secs(1);
        assert_eq!(shown_at(&taps, &["amq.gen-ours"], almost), ["amq.topic"]);
        assert_eq!(taps.names(), ["amq.gen-ours"]);
        // Then dropped.
        let expired = closed + RETENTION + Duration::from_secs(1);
        assert_eq!(
            shown_at(&taps, &["amq.gen-ours"], expired),
            ["amq.gen-ours", "amq.topic"]
        );
        assert!(taps.names().is_empty());
    }

    #[test]
    fn retention_counts_from_the_close_not_the_open() {
        let taps = TapQueues::default();
        let opened = Instant::now();
        taps.note_at("amq.gen-ours", opened);
        // Open for longer than the retention, then closed.
        let closed = opened + 2 * RETENTION;
        taps.close_at("amq.gen-ours", closed);
        let almost = closed + RETENTION - Duration::from_secs(1);
        assert_eq!(shown_at(&taps, &["amq.gen-ours"], almost), ["amq.topic"]);
        let expired = closed + RETENTION;
        assert_eq!(
            shown_at(&taps, &["amq.gen-ours"], expired),
            ["amq.gen-ours", "amq.topic"]
        );
    }

    #[test]
    fn registering_drops_expired_queues() {
        let taps = Arc::new(TapQueues::default());
        drop(taps.register("amq.gen-old"));
        let closed = Instant::now();
        taps.note_at("amq.gen-new", closed + RETENTION + Duration::from_secs(1));
        assert_eq!(taps.names(), ["amq.gen-new"]);
    }

    #[test]
    fn an_exchange_is_never_hidden() {
        let taps = Arc::new(TapQueues::default());
        let _tap = taps.register("amq.topic");
        assert_eq!(shown(&taps, &[]), ["amq.topic"]);
    }
}
