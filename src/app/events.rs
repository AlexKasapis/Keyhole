//! `App` handlers for broker and tick `AppEvent`s — the update half of
//! the loop. Part of the `app` module (overview in `app.rs`); split out to
//! keep that file a thin spine. Methods operate on `App`'s private state.

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;

use super::*;

impl App {
    // -- event handling ------------------------------------------------------

    /// Apply the events already queued in `rx` without blocking, folding a burst
    /// into one update batch. The render loop calls this after a blocking `recv`
    /// so a high-rate feed (e.g. redis `MONITOR`) collapses into a single redraw
    /// instead of one redraw per event — the difference between a responsive UI
    /// and one pinned re-rendering at the event rate.
    ///
    /// Crucially, it drains only the backlog present *on entry* (a snapshot of
    /// [`Receiver::len`]), not "until empty": under a sustained firehose the
    /// producer(s) can refill the channel as fast as we drain it, so a
    /// drain-until-empty loop would spin for the entire burst and never return to
    /// draw — the feed visibly freezes for seconds, then lurches forward in one
    /// giant jump. Bounding the batch to the entry backlog guarantees the loop
    /// gets to repaint at its frame cadence; events that arrive mid-drain are
    /// picked up on the next pass. Returns early if a handled event requested
    /// quit, so the loop exits promptly rather than finishing a firehose first.
    pub fn drain_events(&mut self, rx: &mut Receiver<AppEvent>) {
        // A snapshot, so we don't chase a moving target. `len()` never exceeds
        // the channel capacity, so this batch is always bounded.
        let mut budget = rx.len();
        while self.running && budget > 0 {
            match rx.try_recv() {
                Ok(event) => self.handle_event(event),
                // Empty: caught up early. Disconnected: every sender is gone, so
                // the loop is about to end anyway. Either way, stop draining.
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
            budget -= 1;
        }
    }

    pub fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::Input(Event::Key(key)) => self.handle_key(key),
            AppEvent::Input(Event::Mouse(mouse)) => self.handle_mouse(mouse.kind),
            AppEvent::Input(_) => {}
            AppEvent::Tick => self.on_tick(),
            AppEvent::Connected { handle } => self.on_connected(handle),
            AppEvent::Disconnected { id, reason } => self.on_disconnected(id, reason),
            AppEvent::KeysPage { id, page } => self.on_keys_page(id, page),
            AppEvent::ValueLoaded { id, key, value } => self.on_value(id, key, value),
            AppEvent::Peeked { id, spec, result } => self.on_peeked(id, spec, result),
            AppEvent::Published { id, target, result } => self.on_published(id, target, result),
            AppEvent::DestinationsDiscovered { id, result } => {
                self.on_destinations_discovered(id, result)
            }
            AppEvent::DestinationsListed { id, result } => self.on_destinations_listed(id, result),
            AppEvent::StatsUpdated { id, result } => self.on_stats(id, result),
            AppEvent::RosterUpdated { id, kind, result } => self.on_roster(id, kind, result),
            AppEvent::ConnError { id, context, error } => self.on_conn_error(id, context, error),
            AppEvent::Realtime { id, sub_id, event } => self.on_realtime(id, sub_id, event),
            AppEvent::SubscriptionStarted { id, sub_id } => self.on_sub_started(id, sub_id),
            AppEvent::SubscriptionNotice { id, sub_id, notice } => {
                self.on_sub_notice(id, sub_id, notice)
            }
            AppEvent::SubscriptionEnded { id, sub_id, reason } => {
                self.on_sub_ended(id, sub_id, reason)
            }
            AppEvent::RecordingUpdate { id, sub_id, status } => {
                self.on_recording_update(id, sub_id, status)
            }
            AppEvent::CommandResult {
                id,
                command,
                result,
            } => self.on_command_result(id, command, result),
        }
    }

    /// Route mouse scroll to the focused list/pane (click selection is not
    /// tracked — the immediate-mode render keeps no hit-test map). Ignored
    /// during text entry so a scroll can't disturb a half-typed command.
    pub(super) fn handle_mouse(&mut self, kind: MouseEventKind) {
        // A modal overlay (palette / settings) or a text-entry mode owns input,
        // so the scroll wheel must not reach the screen beneath them.
        if self.mode != InputMode::Normal || self.palette.is_some() || self.settings.is_some() {
            return;
        }
        match kind {
            MouseEventKind::ScrollDown => self.nav(1),
            MouseEventKind::ScrollUp => self.nav(-1),
            _ => {}
        }
    }

    pub(super) fn on_tick(&mut self) {
        self.now = OffsetDateTime::now_utc();
        // Self-dismiss a transient notification once its time is up.
        self.expire_status();
        let refresh_ticks = self.browse_refresh_ticks;
        let on_browser = self.screen == Screen::Browser;
        let mut refresh_id = None;
        if let Some(conn) = self.active_conn_mut() {
            conn.dashboard.stat_ticks += 1;
            if conn.dashboard.stat_ticks >= STATS_REFRESH_TICKS {
                conn.dashboard.stat_ticks = 0;
                // Only brokers with a dashboard answer RefreshStats; others would
                // just surface an "unsupported" error each tick.
                if conn.caps.can_dashboard {
                    conn.request_stats();
                }
                // A roster refreshes only while its tab is on screen, like
                // MONITOR, which keeps a broker with many clients from
                // listing them all every couple of seconds.
                if let Some(kind) = conn.roster_tab().filter(|_| on_browser) {
                    conn.request_roster(kind);
                }
                // Liveness check; a failure surfaces as Disconnected.
                conn.handle.send(ConnCommand::Ping);
            }
            // Auto-refresh the browser's list on its own clock, independent of
            // navigation, so keys (Redis) or queues and exchanges (RabbitMQ)
            // added or removed server-side appear without the user touching
            // anything. Gated to the Browser screen (no point refreshing while
            // a tail is on screen) and never stacked on top of a refresh that
            // is still running.
            if refresh_ticks > 0
                && on_browser
                && conn.caps.auto_refresh()
                && conn.refresh_due(refresh_ticks)
            {
                refresh_id = Some((conn.id, conn.caps.uses_key_scan()));
            }
        }
        match refresh_id {
            // A background refresh: keep the current list visible and swap in the
            // fresh scan once it completes.
            Some((id, true)) => self.start_scan(id, false),
            // A dead management API isn't re-probed here, so it fails fast
            // instead of stalling the connection's actor on every tick.
            Some((id, false)) => self.list_destinations(id, false),
            None => {}
        }
    }

    pub(super) fn on_connected(&mut self, handle: ConnHandle) {
        let conn = Connection::new(handle);
        let id = conn.id;
        let caps = conn.caps.clone();
        self.connections.push(conn);
        self.active = Some(self.connections.len() - 1);
        self.screen = initial_screen(&caps);
        self.enter_browser();
        // The Browser's Server band shows the green "connected" dot, so no
        // transient footer message is needed for the success case (errors still
        // surface there).
        self.health = ConnHealth::Connected;
        // Kick off the broker-appropriate first load.
        match caps.r#type {
            BrokerType::Redis => self.start_scan(id, true),
            BrokerType::Amqp => {
                // A curated destination browser. Seed the list from the saved
                // profile, then peek whatever queue lands selected.
                self.seed_destinations(id);
                self.request_peek(id);
                // Enrich the curated list with the broker's own topics/queues when
                // the profile names a management URL. Silent: a no-op when
                // discovery isn't configured, and failures stay off the footer on
                // connect.
                self.discover_destinations(id, false);
            }
            // AMQP 0.9.1 can't list destinations, so the queues and exchanges
            // come from the management API. `connect` has just probed it, so
            // this doesn't probe again.
            BrokerType::Rabbitmq => self.list_destinations(id, false),
        }
        if caps.can_dashboard {
            self.request_stats(id);
        }
    }

    pub(super) fn on_disconnected(&mut self, id: ConnId, reason: String) {
        if let Some(idx) = self.connections.iter().position(|c| c.id == id) {
            let name = self.connections[idx].name.clone();
            let shown = self.active_id();
            self.connections[idx].handle.shutdown();
            self.connections.remove(idx);
            self.active = if self.connections.is_empty() {
                None
            } else {
                Some(0)
            };
            if self.active_id() != shown {
                self.clear_subscribe_prompt();
            }
            if self.connections.is_empty() {
                self.screen = Screen::Home;
                // No connection left: record the error health. The footer status
                // (set below) carries the detailed reason for the user.
                self.health = ConnHealth::Error;
            }
            self.set_status(format!("{name} disconnected: {reason}"), true);
        }
    }

    pub(super) fn on_keys_page(&mut self, id: ConnId, page: BrowsePage) {
        let page_size = self.scan_count;
        // Fold the page into the scan in progress. A multi-page scan drives
        // itself to completion by issuing the next request here (not on
        // navigation), so the full keyspace loads on its own.
        let next = match self.conn_by_id_mut(id) {
            Some(conn) => {
                let step = conn.apply_page(page, page_size);
                // `apply_page` no longer rebuilds the view itself — the cost is
                // steered here so a many-page scan stays linear. A completed scan
                // always rebuilds (the final list must be exact); a foreground
                // scan rebuilds progressively but throttled; a background refresh
                // keeps the old view until its atomic swap on completion.
                match &step {
                    ScanStep::Stale => return, // page from a superseded scan; drop it
                    ScanStep::Done => {
                        // The first finished scan starts the tree fully folded so
                        // entering the browser shows only the top-level namespaces.
                        conn.collapse_groups_on_first_load();
                        conn.rebuild_view();
                    }
                    ScanStep::Continue(_) if conn.browser.scan_live => {
                        conn.rebuild_view_throttled(VIEW_REBUILD_INTERVAL)
                    }
                    ScanStep::Continue(_) => {}
                }
                match step {
                    ScanStep::Continue(req) => Some(req),
                    _ => None,
                }
            }
            None => return,
        };
        // Land the highlight on the first row as soon as there is one to show.
        if let Some(conn) = self.conn_by_id_mut(id) {
            if conn.browser.table.selected().is_none() && !conn.browser.view.is_empty() {
                conn.browser.table.select(Some(0));
            }
        }
        if let Some(req) = next {
            if let Some(conn) = self.conn_by_id(id) {
                conn.handle.send(ConnCommand::Browse(req));
            }
        }
        self.request_selected_value(id);
    }

    pub(super) fn on_value(&mut self, id: ConnId, key: String, value: ValueView) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if conn.inspector.value_key.as_deref() == Some(key.as_str()) {
                conn.inspector.value = Some(value);
            }
        }
    }

    pub(super) fn on_peeked(
        &mut self,
        id: ConnId,
        spec: SubSpec,
        result: Result<Vec<BrokerEvent>, String>,
    ) {
        if let Err(error) = &result {
            // The footer reports every failed peek, as it always has; the pane
            // below explains the current one in full.
            self.set_status(format!("[{}] peek: {error}", id.0), true);
        }
        let Some(conn) = self.conn_by_id_mut(id) else {
            return;
        };
        // Drop a stale reply: the user may have peeked another destination
        // before this one returned (mirrors `on_value`'s key guard).
        if conn.peek.peeked.as_ref() != Some(&spec) {
            return;
        }
        conn.peek.pending = false;
        conn.peek.scroll = 0;
        // A fresh batch resets the message-list cursor and the search filter.
        conn.peek.selected = 0;
        conn.peek.filter.clear();
        match result {
            Ok(events) => {
                // A full batch means the queue likely holds more than we read,
                // so the count line can flag that the view is truncated.
                conn.peek.limit_hit = events.len() >= VALUE_LIMIT;
                conn.peek.events = events;
                conn.peek.error = None;
            }
            Err(error) => {
                conn.peek.events.clear();
                conn.peek.error = Some(error);
            }
        }
    }

    /// AMQP destination discovery returned: merge the broker's topics/queues
    /// into the curated browser (deduped against what's already there) and
    /// persist, or report why discovery failed. The merge preserves the current
    /// selection; when nothing was selected it lands on the first row and peeks
    /// it so the pane isn't left blank.
    pub(super) fn on_destinations_discovered(
        &mut self,
        id: ConnId,
        result: Result<Vec<SubSpec>, String>,
    ) {
        let specs = match result {
            Ok(specs) => specs,
            Err(e) => return self.set_status(format!("Discovery failed: {e}"), true),
        };
        let total = specs.len();
        let Some((added, newly_selected)) = self.conn_by_id_mut(id).map(|conn| {
            let had_selection = conn.destinations.table.selected().is_some();
            let dests: Vec<Destination> = specs.iter().filter_map(Destination::from_spec).collect();
            let added = conn.merge_destinations(dests);
            // `merge_destinations` selects the first row only when nothing was
            // selected before, so this detects that it just did.
            let newly_selected = !had_selection && conn.destinations.table.selected().is_some();
            (added, newly_selected)
        }) else {
            return;
        };
        if added > 0 {
            self.persist_destinations(id);
        }
        if newly_selected {
            self.request_peek(id);
        }
        self.set_status(
            format!("Discovered {total} destinations ({added} new)"),
            false,
        );
    }

    /// A RabbitMQ listing returned: replace the destination list (the cursor
    /// stays on the same queue or exchange), or keep the rows already shown and
    /// record why the listing failed, which the pane explains.
    pub(super) fn on_destinations_listed(
        &mut self,
        id: ConnId,
        result: Result<DestinationList, String>,
    ) {
        let Some(conn) = self.conn_by_id_mut(id) else {
            return;
        };
        conn.destinations.list_in_flight = false;
        conn.destinations.listing = match result {
            Ok(list) => {
                conn.replace_destinations(
                    list.items.into_iter().map(Destination::from_info).collect(),
                );
                Listing::Listed {
                    truncated: list.truncated,
                    stats_disabled: list.stats_disabled,
                }
            }
            Err(e) => Listing::Failed(e),
        };
        // The peeked queue was deleted and the cursor moved on: the message
        // list it had the keyboard in is gone. Nothing is peeked again.
        if !conn.peek_is_selected() {
            conn.peek.focused = false;
        }
    }

    /// A publish completed: confirm or report it, and remember the outcome
    /// for the details pane. AMQP 1.0 then re-peeks when the message landed in
    /// the queue currently selected, so the user sees it appear. A RabbitMQ
    /// peek takes the messages off the queue, so a publish never runs one: it
    /// drops the queue's peek, now out of date, so the pane shows the outcome
    /// and Enter peeks again. Its failure goes on the footer as the broker
    /// layer words it (unroutable, NACKed, refused), cut to fit; the details
    /// pane shows it in full.
    pub(super) fn on_published(&mut self, id: ConnId, target: String, result: Result<(), String>) {
        let mut repeeks = true;
        if let Some(conn) = self.conn_by_id_mut(id) {
            conn.publish.last = Some((target.clone(), result.clone()));
            repeeks = conn.caps.repeeks_after_publish();
            // Only a peek shown before the publish is out of date. One still
            // in flight runs after it on the serial actor (and has already
            // touched the queue), so its reply is kept for `on_peeked`.
            if !repeeks
                && !conn.peek.pending
                && conn
                    .peek
                    .peeked
                    .as_ref()
                    .is_some_and(|s| s.label() == target)
            {
                conn.peek = Default::default();
            }
        }
        if !repeeks {
            match result {
                Ok(()) => self.set_status(format!("Published to {target}"), false),
                Err(e) => self.set_status(crate::ui::fit_status(&e), true),
            }
            return;
        }
        match result {
            Ok(()) => {
                self.set_status(format!("Published to {target}"), false);
                let reselect = self
                    .conn_by_id(id)
                    .and_then(|c| c.selected_destination())
                    .is_some_and(|d| d.canonical() == target && matches!(d.kind, DestKind::Queue));
                if reselect {
                    self.request_peek(id);
                }
            }
            Err(e) => self.set_status(format!("Publish to {target} failed: {e}"), true),
        }
    }

    pub(super) fn on_stats(&mut self, id: ConnId, result: Result<BrokerStats, String>) {
        let Some(conn) = self.conn_by_id_mut(id) else {
            return;
        };
        conn.dashboard.in_flight = false;
        let error = match result {
            Ok(stats) => {
                // Store the reply and extend the history that the Server
                // Details graphs draw from.
                conn.dashboard.error = None;
                conn.dashboard.record(stats);
                return;
            }
            Err(error) => error,
        };
        // RabbitMQ asks its management API every couple of seconds, and a dead
        // API answers each time with the same cached error, so the footer only
        // hears about a failure when it changes, and the Server Details tab
        // shows it in place of figures gone stale. Redis reports every failure
        // on the footer and keeps its last figures.
        if conn.caps.r#type != BrokerType::Redis {
            if conn.dashboard.error.as_deref() == Some(error.as_str()) {
                return;
            }
            conn.dashboard.error = Some(error.clone());
        }
        self.on_conn_error(id, "stats".to_string(), error);
    }

    /// A roster refresh returned: show the new list, or why it failed. Like
    /// RabbitMQ's stats, a dead API answers every refresh with the same
    /// cached error, so the footer only hears about a failure when it changes.
    pub(super) fn on_roster(
        &mut self,
        id: ConnId,
        kind: RosterKind,
        result: Result<Roster, String>,
    ) {
        let Some(conn) = self.conn_by_id_mut(id) else {
            return;
        };
        let panel = conn.rosters.get_mut(kind);
        panel.in_flight = false;
        let error = match result {
            Ok(roster) => {
                panel.error = None;
                panel.roster = Some(roster);
                return;
            }
            Err(error) => error,
        };
        if panel.error.as_deref() == Some(error.as_str()) {
            return;
        }
        panel.error = Some(error.clone());
        self.on_conn_error(id, kind.noun().to_string(), error);
    }

    pub(super) fn on_conn_error(&mut self, id: ConnId, context: String, error: String) {
        // An error with nothing connected means a connect attempt failed (auth,
        // dial, unsupported broker) — record error health. Errors raised on a
        // live connection leave it connected; the connection is still up.
        if self.active_conn().is_none() {
            self.health = ConnHealth::Error;
        }
        self.set_status(format!("[{}] {context}: {error}", id.0), true);
    }
}
