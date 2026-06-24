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
            AppEvent::Peeked { id, spec, events } => self.on_peeked(id, spec, events),
            AppEvent::StatsUpdated { id, stats } => self.on_stats(id, stats),
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
                    conn.handle.send(ConnCommand::RefreshStats);
                }
                // Liveness check; a failure surfaces as Disconnected.
                conn.handle.send(ConnCommand::Ping);
            }
            // Auto-refresh the key browser on its own clock, independent of
            // navigation, so keys added or removed server-side appear without
            // the user touching anything. Gated to the Browser screen (no point
            // re-scanning while a tail is on screen) and never stacked on top of
            // a scan that is still running.
            if refresh_ticks > 0 && on_browser && conn.caps.uses_key_scan() {
                conn.browser.browse_ticks += 1;
                if conn.browser.browse_ticks >= refresh_ticks {
                    conn.browser.browse_ticks = 0;
                    if !conn.browser.scanning {
                        refresh_id = Some(conn.id);
                    }
                }
            }
        }
        if let Some(id) = refresh_id {
            // A background refresh: keep the current list visible and swap in the
            // fresh scan once it completes.
            self.start_scan(id, false);
        }
    }

    pub(super) fn on_connected(&mut self, handle: ConnHandle) {
        let conn = Connection::new(handle);
        let id = conn.id;
        let caps = conn.caps.clone();
        self.connections.push(conn);
        self.active = Some(self.connections.len() - 1);
        self.screen = initial_screen(&caps);
        // A freshly-focused browser becomes the `b` target.
        self.note_browser_view();
        // The Browser's Server band shows the green "connected" dot, so no
        // transient footer message is needed for the success case (errors still
        // surface there).
        self.health = ConnHealth::Connected;
        // Kick off the broker-appropriate first load.
        if caps.uses_key_scan() {
            self.start_scan(id, true);
        } else if caps.can_browse {
            // AMQP: a curated destination browser. Seed the list from the saved
            // profile, then peek whatever queue lands selected.
            self.seed_destinations(id);
            self.request_peek(id);
        }
        if caps.can_dashboard {
            self.request_stats(id);
        }
    }

    pub(super) fn on_disconnected(&mut self, id: ConnId, reason: String) {
        if let Some(idx) = self.connections.iter().position(|c| c.id == id) {
            let name = self.connections[idx].name.clone();
            self.connections[idx].handle.shutdown();
            self.connections.remove(idx);
            // Forget a dropped connection as the `b` target.
            if self.last_browser == Some(id) {
                self.last_browser = None;
            }
            self.active = if self.connections.is_empty() {
                None
            } else {
                Some(0)
            };
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

    pub(super) fn on_peeked(&mut self, id: ConnId, spec: SubSpec, events: Vec<BrokerEvent>) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            // Drop a stale peek: the user may have moved to another destination
            // before this batch returned (mirrors `on_value`'s key guard).
            if conn.peek.peeked.as_ref() == Some(&spec) {
                conn.peek.events = events;
                conn.peek.pending = false;
                conn.peek.scroll = 0;
            }
        }
    }

    pub(super) fn on_stats(&mut self, id: ConnId, stats: ServerStats) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            // Store the reply and extend the ops/keys history that the Server
            // Details graphs draw from.
            conn.dashboard.record(stats);
        }
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
