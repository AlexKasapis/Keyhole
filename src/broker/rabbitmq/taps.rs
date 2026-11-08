//! The spy queues Keyhole's own exchange taps declare, so RabbitMQ's
//! destination list can leave them out.
//!
//! A spy queue is server-named (`amq.gen-…`), exclusive and auto-delete, which
//! is also how many other clients' private queues look, so it can't be told
//! apart by its name or flags. Keyhole remembers the names its taps were given
//! instead. The registry is process-wide ([`TAP_QUEUES`]): a tap on one
//! connection is hidden from every connection's list, including from a
//! reconnect's while the old tap's queue is still being deleted.
//!
//! Two timing edges:
//! - A name is noted as soon as the broker has declared the queue, before the
//!   tap is handed back. A connection's actor runs one command at a time, so
//!   none of its listings can fall between the two.
//! - When a tap closes, the broker deletes its queue only after it sees the
//!   tap's connection close. The name stays hidden until a complete listing of
//!   the same broker and vhost no longer has it, and is forgotten then.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::broker::{DestKind, DestinationList};

/// Every tap's spy queue in this process.
pub(super) static TAP_QUEUES: TapQueues = TapQueues::new();

/// Spy queues by name.
#[derive(Default)]
pub(super) struct TapQueues {
    queues: Mutex<BTreeMap<String, TapQueue>>,
}

struct TapQueue {
    /// The broker and vhost the queue was declared on. Only a listing of the
    /// same one can show that the queue is gone.
    scope: String,
    /// Whether its tap is still running.
    open: bool,
}

impl TapQueues {
    pub(super) const fn new() -> Self {
        Self {
            queues: Mutex::new(BTreeMap::new()),
        }
    }

    /// Note `name`, a spy queue just declared on `scope`, as an open tap's
    /// until the returned guard drops.
    pub(super) fn register(&self, scope: &str, name: &str) -> TapQueueGuard<'_> {
        self.lock().insert(
            name.to_string(),
            TapQueue {
                scope: scope.to_string(),
                open: true,
            },
        );
        TapQueueGuard {
            queues: self,
            name: name.to_string(),
        }
    }

    /// Take the spy queues out of `list`, a listing of `scope`. A closed tap's
    /// queue is forgotten once a complete listing of its scope doesn't have
    /// it; a truncated listing may just have cut it off.
    pub(super) fn hide(&self, scope: &str, list: &mut DestinationList) {
        let mut queues = self.lock();
        if queues.is_empty() {
            return;
        }
        let listed = |name: &str| {
            list.items
                .iter()
                .any(|d| d.kind == DestKind::Queue && d.name == name)
        };
        if !list.truncated {
            queues.retain(|name, q| q.open || q.scope != scope || listed(name));
        }
        list.items
            .retain(|d| d.kind != DestKind::Queue || !queues.contains_key(&d.name));
    }

    /// The spy queues noted, open or not.
    #[cfg(test)]
    pub(super) fn names(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, TapQueue>> {
        // Every update is a single map operation, so a panic elsewhere can't
        // leave the map half-changed.
        self.queues.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Marks its spy queue's tap closed when dropped.
pub(super) struct TapQueueGuard<'a> {
    queues: &'a TapQueues,
    name: String,
}

impl Drop for TapQueueGuard<'_> {
    fn drop(&mut self) {
        if let Some(q) = self.queues.lock().get_mut(&self.name) {
            q.open = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::actor::mock::{exchange_row, queue_row};
    use crate::broker::{ExchangeType, QueueType};

    const SCOPE: &str = "rabbit:5672/shop";

    /// A complete listing of these queues, plus an exchange.
    fn listing(queues: &[&str]) -> DestinationList {
        let mut items: Vec<_> = queues
            .iter()
            .map(|q| queue_row(q, QueueType::Classic, 0))
            .collect();
        items.push(exchange_row("amq.topic", ExchangeType::Topic));
        DestinationList {
            items,
            truncated: false,
            stats_disabled: false,
        }
    }

    /// What `taps` leaves of a listing of `queues` on `scope`.
    fn shown(taps: &TapQueues, scope: &str, queues: &[&str]) -> Vec<String> {
        let mut list = listing(queues);
        taps.hide(scope, &mut list);
        list.items.into_iter().map(|d| d.name).collect()
    }

    #[test]
    fn an_open_taps_queue_is_hidden_and_other_clients_queues_stay() {
        let taps = TapQueues::new();
        let _tap = taps.register(SCOPE, "amq.gen-ours");
        assert_eq!(
            shown(&taps, SCOPE, &["amq.gen-ours", "amq.gen-theirs", "orders"]),
            ["amq.gen-theirs", "orders", "amq.topic"]
        );
    }

    #[test]
    fn a_closed_taps_queue_stays_hidden_until_the_broker_deletes_it() {
        let taps = TapQueues::new();
        drop(taps.register(SCOPE, "amq.gen-ours"));
        // The broker hasn't seen the tap's connection close yet.
        assert_eq!(shown(&taps, SCOPE, &["amq.gen-ours"]), ["amq.topic"]);
        assert_eq!(shown(&taps, SCOPE, &["amq.gen-ours"]), ["amq.topic"]);
        // Gone from a complete listing: forgotten.
        assert_eq!(shown(&taps, SCOPE, &[]), ["amq.topic"]);
        assert!(taps.names().is_empty());
    }

    #[test]
    fn an_open_taps_queue_missing_from_a_listing_is_kept() {
        let taps = TapQueues::new();
        let _tap = taps.register(SCOPE, "amq.gen-ours");
        assert_eq!(shown(&taps, SCOPE, &[]), ["amq.topic"]);
        assert_eq!(shown(&taps, SCOPE, &["amq.gen-ours"]), ["amq.topic"]);
    }

    #[test]
    fn only_a_complete_listing_of_the_same_vhost_forgets_a_closed_tap() {
        let taps = TapQueues::new();
        drop(taps.register(SCOPE, "amq.gen-ours"));
        let mut truncated = listing(&[]);
        truncated.truncated = true;
        taps.hide(SCOPE, &mut truncated);
        shown(&taps, "rabbit:5672/other", &[]);
        assert_eq!(shown(&taps, SCOPE, &["amq.gen-ours"]), ["amq.topic"]);
    }

    #[test]
    fn a_tap_is_hidden_from_another_scopes_listing() {
        // The same broker, reached under another host name.
        let taps = TapQueues::new();
        let _tap = taps.register(SCOPE, "amq.gen-ours");
        assert_eq!(
            shown(&taps, "127.0.0.1:5672/shop", &["amq.gen-ours"]),
            ["amq.topic"]
        );
    }

    #[test]
    fn an_exchange_is_never_hidden() {
        let taps = TapQueues::new();
        let _tap = taps.register(SCOPE, "amq.topic");
        assert_eq!(shown(&taps, SCOPE, &[]), ["amq.topic"]);
    }
}
