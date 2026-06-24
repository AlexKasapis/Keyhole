//! `App` realtime surface: live-tail/subscription event handlers and the
//! bottom-panel tab + feed machinery. Part of the `app` module.

use super::*;

impl App {
    // -- realtime events -----------------------------------------------------

    /// Reset per-frame transient state at the start of each render frame. The
    /// render loop calls this once per drawn frame, before draining that frame's
    /// events. For now it just refills the Monitor feed's reveal budget (see
    /// [`MONITOR_REVEAL_PER_FRAME`]), which paces a firehose into a steady scroll.
    pub fn begin_frame(&mut self) {
        self.monitor_reveal_budget = MONITOR_REVEAL_PER_FRAME;
    }

    pub(super) fn on_realtime(&mut self, id: ConnId, sub_id: u32, event: BrokerEvent) {
        // The Monitor feed is a firehose: count every event (so the tally tracks
        // true throughput) but reveal only a paced few per frame so it scrolls
        // steadily instead of teleporting a whole batch; the surplus is dropped
        // from the on-screen feed (the recording keeps it). Other feeds, which
        // aren't firehoses by nature, store everything.
        let mut reveal = self.monitor_reveal_budget;
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                // First event implicitly confirms the tail is live (even while
                // paused — the broker keeps streaming; we just stop tracking).
                if sub.state == SubState::Connecting {
                    sub.state = SubState::Active;
                }
                // While paused, drop the event instead of buffering it so the
                // scrollback and the received tally stay frozen.
                if !sub.paused {
                    if matches!(sub.spec, SubSpec::Monitor) {
                        if reveal > 0 {
                            sub.push(event);
                            reveal -= 1;
                        } else {
                            sub.skip();
                        }
                    } else {
                        sub.push(event);
                    }
                }
            }
        }
        self.monitor_reveal_budget = reveal;
    }

    pub(super) fn on_sub_started(&mut self, id: ConnId, sub_id: u32) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                if sub.state == SubState::Connecting {
                    sub.state = SubState::Active;
                }
            }
        }
    }

    pub(super) fn on_sub_notice(&mut self, id: ConnId, sub_id: u32, notice: String) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                sub.notice = Some(notice.clone());
            }
        }
        self.set_status(notice, true);
    }

    pub(super) fn on_command_result(
        &mut self,
        id: ConnId,
        command: String,
        result: Result<String, String>,
    ) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            let (output, is_error) = match result {
                Ok(out) => (out, false),
                Err(err) => (err, true),
            };
            conn.console.pending = None;
            conn.console.entries.push(ConsoleEntry {
                command,
                output,
                is_error,
            });
            // Snap back to the latest reply (offset 0 == following the bottom).
            conn.console.scroll = 0;
        }
    }

    pub(super) fn on_sub_ended(&mut self, id: ConnId, sub_id: u32, reason: Option<String>) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                sub.state = SubState::Ended(reason.clone());
                sub.recording = RecordState::Off;
            }
        }
        if let Some(reason) = reason {
            self.set_status(format!("tail ended: {reason}"), true);
        }
    }

    pub(super) fn on_recording_update(&mut self, id: ConnId, sub_id: u32, status: RecordingStatus) {
        let mut note: Option<(String, bool)> = None;
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                match status {
                    RecordingStatus::Started { path } => {
                        note = Some((format!("recording → {}", path.display()), false));
                        sub.recording = RecordState::On {
                            records: 0,
                            bytes: 0,
                            path,
                        };
                    }
                    RecordingStatus::Progress { records, bytes } => {
                        if let RecordState::On {
                            records: r,
                            bytes: b,
                            ..
                        } = &mut sub.recording
                        {
                            *r = records;
                            *b = bytes;
                        }
                    }
                    RecordingStatus::Stopped {
                        records,
                        bytes,
                        path,
                    } => {
                        note = Some((
                            format!("recorded {records} events ({bytes} B) → {}", path.display()),
                            false,
                        ));
                        sub.recording = RecordState::Off;
                    }
                    RecordingStatus::Failed { error } => {
                        note = Some((format!("recording failed: {error}"), true));
                        sub.recording = RecordState::Off;
                    }
                }
            }
        }
        if let Some((message, is_error)) = note {
            self.set_status(message, is_error);
        }
    }

    // -- realtime tails / recordings -----------------------------------------

    /// Cycle the Browser's bottom-panel tab by `delta` (Tab / Shift-Tab — the
    /// only way to move between tabs), then reconcile the panel with the new
    /// focus. No-op off the Browser or without an active connection.
    pub(super) fn cycle_panel(&mut self, delta: i32) {
        if self.screen != Screen::Browser {
            return;
        }
        if let Some(conn) = self.active_conn_mut() {
            conn.cycle_panel(delta);
        }
        // Moving focus gives the Pub/Sub and Tail anchors a fresh prompt.
        self.subscribe_buf.clear();
        self.sync_panel_focus();
    }

    /// Reconcile the bottom panel with the focused tab: start/stop the
    /// focus-scoped MONITOR and keyspace feeds (live only while their tab is
    /// focused), set the input mode the focused tab implies, and give the
    /// Pub/Sub and Tail anchors a fresh prompt. No-op for brokers without a
    /// panel (non-Redis) or off the Browser screen.
    pub(super) fn sync_panel_focus(&mut self) {
        if self.screen != Screen::Browser || !self.active_can_tail() {
            return;
        }
        let Some(conn) = self.active_conn() else {
            return;
        };
        let active = conn.active_panel();
        let db = conn.db;

        // MONITOR runs only while its tab is focused.
        let monitor_id = self.feed_id(|s| matches!(s, SubSpec::Monitor));
        match (matches!(active, PanelTab::Monitor), monitor_id) {
            (true, None) => self.start_feed(SubSpec::Monitor),
            (false, Some(id)) => self.stop_feed(id),
            _ => {}
        }

        // The keyspace feed runs only while focused and is scoped to the
        // connection's db; if a running feed targets a different db it restarts.
        let keyspace = self.feed_id_if(
            |s| matches!(s, SubSpec::Keyspace { .. }),
            |s| matches!(s, SubSpec::Keyspace { db: d } if *d == db),
        );
        match (matches!(active, PanelTab::Keyspace), keyspace) {
            (true, None) => self.start_feed(SubSpec::Keyspace { db }),
            (true, Some((id, false))) => {
                self.stop_feed(id);
                self.start_feed(SubSpec::Keyspace { db });
            }
            (false, Some((id, _))) => self.stop_feed(id),
            _ => {}
        }

        self.sync_panel_mode();
    }

    /// The input mode the focused pane implies. Text capture happens only while
    /// the *bottom subpanel* has the keyboard ([`PaneFocus::Bottom`]) and the
    /// selected tab is a text anchor (Console → Command, Pub/Sub / Tail →
    /// Subscribe). With the keys pane focused — or a live-feed tab selected — the
    /// mode is Normal, so the key list keeps its bindings while a text subpanel
    /// is merely on screen. Idempotent — driven from the pane-focus / tab moves.
    pub(super) fn sync_panel_mode(&mut self) {
        let Some(conn) = self.active_conn() else {
            return;
        };
        let bottom = conn.focus == PaneFocus::Bottom;
        self.mode = match (bottom, conn.active_panel()) {
            (true, PanelTab::Console) => InputMode::Command,
            (true, PanelTab::PubSub | PanelTab::Tail) => InputMode::Subscribe,
            _ => InputMode::Normal,
        };
    }

    /// Stop and drop every focus-scoped feed (MONITOR / keyspace) on the active
    /// connection — called when the panel loses focus by leaving the Browser.
    pub(super) fn stop_focus_feeds(&mut self) {
        let ids: Vec<u32> = self
            .active_conn()
            .map(|c| {
                c.subs
                    .iter()
                    .filter(|s| matches!(s.spec, SubSpec::Monitor | SubSpec::Keyspace { .. }))
                    .map(|s| s.sub_id)
                    .collect()
            })
            .unwrap_or_default();
        for id in ids {
            self.stop_feed(id);
        }
    }

    /// The id of the active connection's first tail whose spec matches `pred`.
    pub(super) fn feed_id(&self, pred: impl Fn(&SubSpec) -> bool) -> Option<u32> {
        self.active_conn()
            .and_then(|c| c.subs.iter().find(|s| pred(&s.spec)).map(|s| s.sub_id))
    }

    /// Like [`Self::feed_id`], also reporting whether the found tail satisfies a
    /// secondary `ok` predicate (used to detect a keyspace feed on a stale db).
    pub(super) fn feed_id_if(
        &self,
        pred: impl Fn(&SubSpec) -> bool,
        ok: impl Fn(&SubSpec) -> bool,
    ) -> Option<(u32, bool)> {
        self.active_conn().and_then(|c| {
            c.subs
                .iter()
                .find(|s| pred(&s.spec))
                .map(|s| (s.sub_id, ok(&s.spec)))
        })
    }

    /// Start a focus-scoped feed (MONITOR / keyspace) without changing the
    /// focused tab — these render under their fixed anchor, not as a `Sub` tab.
    /// They start *paused*: the feed subscribes at the broker but drops events
    /// until the user presses `p` to begin following, so simply focusing the tab
    /// doesn't immediately flood the panel with a live MONITOR stream.
    pub(super) fn start_feed(&mut self, spec: SubSpec) {
        let Some(id) = self.active_id() else {
            return;
        };
        let capacity = self.tail_scrollback;
        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        if let Some(conn) = self.conn_by_id_mut(id) {
            conn.handle.send(ConnCommand::Subscribe {
                sub_id,
                spec: spec.clone(),
                record: false,
            });
            let mut sub = Subscription::new(sub_id, spec, capacity);
            sub.paused = true;
            conn.subs.push(sub);
        }
    }

    /// Stop and drop the tail with `sub_id` from the active connection.
    pub(super) fn stop_feed(&mut self, sub_id: u32) {
        if let Some(conn) = self.active_conn_mut() {
            if let Some(pos) = conn.subs.iter().position(|s| s.sub_id == sub_id) {
                let sub = conn.subs.remove(pos);
                conn.handle
                    .send(ConnCommand::StopSubscription { sub_id: sub.sub_id });
            }
        }
    }

    /// Play/pause the focused live feed: pausing stops tracking incoming events
    /// (they are dropped, not buffered), resuming snaps the viewport back to the
    /// newest event and follows again. Only acts on a feed tab.
    pub(super) fn toggle_play_pause(&mut self) {
        if self.screen != Screen::Browser {
            return;
        }
        let paused = {
            let Some(sub) = self
                .active_conn_mut()
                .and_then(|c| c.panel_subscription_mut())
            else {
                self.set_status("no live feed on this tab to pause".to_string(), true);
                return;
            };
            sub.paused = !sub.paused;
            if !sub.paused {
                sub.follow = true;
                sub.offset = 0;
            }
            sub.paused
        };
        let msg = if paused {
            "feed paused"
        } else {
            "feed resumed"
        };
        self.set_status(msg.to_string(), false);
    }

    /// Whether the active connection can open realtime tails. Tails live in the
    /// Browser's bottom panel, hosted by Redis (alongside its console) and AMQP
    /// (its only panel). See [`Capabilities::can_tail`].
    pub(super) fn active_can_tail(&self) -> bool {
        self.active_conn()
            .map(|c| c.caps.can_tail())
            .unwrap_or(false)
    }

    /// Submit the focused anchor's always-shown prompt: subscribe to a channel /
    /// pattern on the Pub/Sub tab, or tail a stream on the Tail tab (an empty
    /// Tail prompt tails the selected stream key). Which is built follows the
    /// focused tab, not the buffer's contents.
    pub(super) fn submit_subscribe(&mut self) {
        let raw = self.subscribe_buf.trim().to_string();
        self.subscribe_buf.clear();
        let (panel, db, is_amqp) = match self.active_conn() {
            Some(c) => (c.active_panel(), c.db, c.caps.kind == BrokerKind::Amqp),
            None => return,
        };
        // AMQP's single Tail anchor accepts a full source spec (topic:name /
        // queue:name); there is no separate Pub/Sub or stream-key shorthand.
        if is_amqp {
            if matches!(panel, PanelTab::Tail) && !raw.is_empty() {
                match SubSpec::parse(&raw, 0) {
                    Ok(spec) => self.start_subscribe(spec),
                    Err(e) => self.set_status(e.to_string(), true),
                }
            }
            return;
        }
        match panel {
            PanelTab::PubSub => {
                if !raw.is_empty() {
                    self.start_subscribe(pubsub_spec(&raw));
                }
            }
            PanelTab::Tail => {
                if raw.is_empty() {
                    self.tail_selected_key();
                } else {
                    self.start_subscribe(SubSpec::Stream {
                        key: stream_key(&raw),
                        db,
                    });
                }
            }
            // Submit only does something while a text-input anchor is focused.
            _ => {}
        }
    }

    /// Open (or focus, if it already exists) a pub/sub or stream tail tab. Only
    /// reachable on a Redis browser, where the bottom panel hosts the tab.
    pub(super) fn start_subscribe(&mut self, spec: SubSpec) {
        let Some(id) = self.active_id() else {
            self.set_status("no active connection".to_string(), true);
            return;
        };
        if !self.active_can_tail() {
            self.set_status(
                "realtime tails are not available for this broker".to_string(),
                true,
            );
            return;
        }
        let capacity = self.tail_scrollback;
        let label = spec.label();

        // Focus an existing live tail for the same spec rather than duplicating.
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(pos) = conn
                .subs
                .iter()
                .position(|s| s.spec == spec && !matches!(s.state, SubState::Ended(_)))
            {
                conn.focus_sub(pos);
                self.set_status(format!("already tailing {label}"), false);
                self.sync_panel_focus();
                return;
            }
        }

        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        if let Some(conn) = self.conn_by_id_mut(id) {
            conn.handle.send(ConnCommand::Subscribe {
                sub_id,
                spec: spec.clone(),
                record: false,
            });
            conn.subs.push(Subscription::new(sub_id, spec, capacity));
            // Jump focus to the new tail's tab so its feed is visible.
            let new_idx = conn.subs.len() - 1;
            conn.focus_sub(new_idx);
        }
        self.set_status(format!("subscribing to {label}…"), false);
        self.sync_panel_focus();
    }

    pub(super) fn tail_selected_key(&mut self) {
        if self.screen != Screen::Browser {
            return;
        }
        let selected = self
            .active_conn()
            .and_then(|c| c.selected().map(|e| (e.key.clone(), e.vtype, c.db)));
        let Some((key, vtype, db)) = selected else {
            self.set_status("no stream key selected to tail".to_string(), true);
            return;
        };
        if vtype != ValueType::Stream {
            self.set_status(
                format!(
                    "'{key}' is a {} — only streams can be tailed",
                    vtype.label()
                ),
                true,
            );
            return;
        }
        self.start_subscribe(SubSpec::Stream { key, db });
    }

    pub(super) fn toggle_recording(&mut self) {
        let info = self.active_conn().and_then(|c| {
            c.active_subscription()
                .map(|s| (c.id, s.sub_id, s.recording.is_on(), &s.state))
                .map(|(id, sub, on, st)| (id, sub, on, matches!(st, SubState::Ended(_))))
        });
        let Some((id, sub_id, on, ended)) = info else {
            self.set_status("no active tail to record".to_string(), true);
            return;
        };
        if ended {
            self.set_status("tail has ended; start a new one".to_string(), true);
            return;
        }
        let turn_on = !on;
        if let Some(conn) = self.conn_by_id(id) {
            conn.handle.send(ConnCommand::SetRecording {
                sub_id,
                on: turn_on,
            });
        }
        let msg = if turn_on {
            "starting recording…"
        } else {
            "stopping recording…"
        };
        self.set_status(msg.to_string(), false);
    }

    /// Close the focused pub/sub or stream tab, stopping its tail. The five fixed
    /// anchors (Console / Monitor / Keyspace / Pub/Sub / Tail) cannot be closed.
    pub(super) fn close_active_tab(&mut self) {
        if self.screen != Screen::Browser {
            return;
        }
        let removed = {
            let Some(conn) = self.active_conn_mut() else {
                return;
            };
            match conn.active_panel() {
                PanelTab::Sub(i) if i < conn.subs.len() => {
                    let sub = conn.subs.remove(i);
                    conn.handle
                        .send(ConnCommand::StopSubscription { sub_id: sub.sub_id });
                    // Land focus back on the anchor the closed tail belonged to.
                    let anchor = if matches!(sub.spec, SubSpec::Stream { .. }) {
                        PanelTab::Tail
                    } else {
                        PanelTab::PubSub
                    };
                    if let Some(pos) = conn.panel_slots().iter().position(|t| *t == anchor) {
                        conn.panel_tab = pos;
                    }
                    Some(sub.label)
                }
                _ => None,
            }
        };
        match removed {
            Some(label) => {
                self.set_status(format!("closed {label}"), false);
                self.sync_panel_focus();
            }
            None => self.set_status("only pub/sub and tail tabs can be closed".to_string(), true),
        }
    }
}
