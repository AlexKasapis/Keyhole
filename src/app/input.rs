//! `App` input handling: key/mouse dispatch, the per-mode key handlers, and
//! list/pane navigation. Part of the `app` module (overview in `app.rs`).

use super::*;

impl App {
    // -- input ---------------------------------------------------------------

    pub(super) fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return; // ignore key-release/repeat (Windows emits these)
        }
        // The full-screen text-entry modals own the keyboard wholesale until they
        // are dismissed, so their input is never treated as a command.
        match self.mode {
            InputMode::Filter => return self.handle_filter_key(key),
            InputMode::Form => return self.handle_form_key(key),
            InputMode::Rename => return self.handle_rename_key(key),
            // Normal / Command / Subscribe are reconciled from the focused pane.
            InputMode::Normal | InputMode::Command | InputMode::Subscribe => {}
        }
        match self.screen {
            Screen::Browser => self.handle_browser_key(key),
            // The home screens have a single navigable list; keys map straight to
            // actions (the bottom-panel focus model is Browser-only).
            Screen::Home | Screen::Recordings => {
                if let Some(action) = action::map_key(&key) {
                    self.apply(action);
                }
            }
        }
    }

    /// Dispatch a Browser key by the focused pane. The pane-focus controls run
    /// first and are non-printable, so they work even while a text subpanel is
    /// capturing input — you can never get trapped in the console / anchor
    /// prompts. The keys pane then runs the (feed-control-free) keymap, while a
    /// focused text or feed subpanel handles the key itself.
    fn handle_browser_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            // Ctrl-C always quits (even mid-typing); Ctrl-↑/↓ move the keyboard
            // between the keys pane and the bottom subpanel.
            (true, KeyCode::Char('c')) => {
                self.running = false;
                return;
            }
            (true, KeyCode::Up) => {
                self.set_pane_focus(PaneFocus::Keys);
                return;
            }
            (true, KeyCode::Down) => {
                self.set_pane_focus(PaneFocus::Bottom);
                return;
            }
            // Tab / Shift-Tab move into and cycle the bottom subpanels.
            (false, KeyCode::Tab) => {
                self.focus_or_cycle_panel(1);
                return;
            }
            (false, KeyCode::BackTab) => {
                self.focus_or_cycle_panel(-1);
                return;
            }
            // Esc steps the keyboard back to the keys pane; from the keys pane it
            // leaves the Browser (the global "back").
            (false, KeyCode::Esc) => {
                if self.bottom_focused() {
                    self.set_pane_focus(PaneFocus::Keys);
                } else {
                    self.apply(Action::Back);
                }
                return;
            }
            _ => {}
        }

        if self.bottom_focused() {
            match self.active_conn().map(|c| c.active_panel()) {
                Some(PanelTab::Console) => self.handle_command_key(key),
                Some(PanelTab::PubSub | PanelTab::Tail) => self.handle_subscribe_key(key),
                // The Server Details tab has no feed: navigation scrolls its
                // client list, and the feed controls (p/x/r) don't apply.
                Some(PanelTab::ServerDetails) => self.handle_details_key(key),
                // A live-feed tab (Monitor / Keyspace / a pub-sub or stream tail):
                // navigation scrolls the feed and p/x/r control it.
                _ => self.handle_feed_key(key),
            }
        } else if let Some(action) = action::map_keys_focus(&key) {
            self.apply(action);
        }
    }

    /// Whether the active connection's bottom subpanel currently has the keyboard.
    pub(super) fn bottom_focused(&self) -> bool {
        self.active_conn()
            .map(|c| c.focus == PaneFocus::Bottom)
            .unwrap_or(false)
    }

    /// Move the keyboard between the keys pane and the bottom subpanel. Focusing
    /// the bottom is a no-op when the broker has no panel (non-Redis). Reconciles
    /// the panel mode + focus-scoped feeds and freshens the anchor prompt.
    pub(super) fn set_pane_focus(&mut self, focus: PaneFocus) {
        if focus == PaneFocus::Bottom && !self.active_can_tail() {
            return;
        }
        if let Some(conn) = self.active_conn_mut() {
            conn.focus = focus;
        }
        if focus == PaneFocus::Bottom {
            // A fresh prompt on the Pub/Sub and Tail anchors when entering them.
            self.subscribe_buf.clear();
        }
        self.sync_panel_focus();
    }

    /// Tab / Shift-Tab: the first press from the keys pane drops the keyboard onto
    /// the currently shown subpanel; further presses cycle through the subpanels.
    pub(super) fn focus_or_cycle_panel(&mut self, delta: i32) {
        if !self.active_can_tail() {
            return;
        }
        if self.bottom_focused() {
            self.cycle_panel(delta);
        } else {
            self.set_pane_focus(PaneFocus::Bottom);
        }
    }

    /// Keys while a Browser live-feed tab (Monitor / Keyspace / a pub-sub or
    /// stream tail) is focused: navigation scrolls the feed's scrollback, and
    /// p/x/r play-pause / close / record it. Focus moves are handled upstream.
    fn handle_feed_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (false, KeyCode::Char('j') | KeyCode::Down) => self.scroll_feed(-1),
            (false, KeyCode::Char('k') | KeyCode::Up) => self.scroll_feed(1),
            (true, KeyCode::Char('d')) => self.scroll_feed(-FEED_SCROLL_STEP),
            (true, KeyCode::Char('u')) => self.scroll_feed(FEED_SCROLL_STEP),
            (_, KeyCode::PageDown) => self.scroll_feed(-FEED_SCROLL_STEP),
            (_, KeyCode::PageUp) => self.scroll_feed(FEED_SCROLL_STEP),
            (false, KeyCode::Char('g') | KeyCode::Home) => self.feed_to_edge(true),
            (false, KeyCode::Char('G') | KeyCode::End) => self.feed_to_edge(false),
            (false, KeyCode::Char('p')) => self.toggle_play_pause(),
            (false, KeyCode::Char('x')) => self.close_active_tab(),
            (false, KeyCode::Char('r')) => self.toggle_recording(),
            (false, KeyCode::Char('m')) => self.apply(Action::ToggleMouse),
            (false, KeyCode::Char('?')) => self.apply(Action::ToggleHelp),
            _ => {}
        }
    }

    /// Keys while the Server Details tab is focused. The tab is a passive
    /// overview (graphs + the connected-client list), so the only navigation is
    /// scrolling the client list; there is no feed to play/pause, close, or
    /// record. Focus moves are handled upstream in [`Self::handle_browser_key`].
    fn handle_details_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (false, KeyCode::Char('j') | KeyCode::Down) => self.scroll_details(1),
            (false, KeyCode::Char('k') | KeyCode::Up) => self.scroll_details(-1),
            (true, KeyCode::Char('d')) => self.scroll_details(FEED_SCROLL_STEP),
            (true, KeyCode::Char('u')) => self.scroll_details(-FEED_SCROLL_STEP),
            (_, KeyCode::PageDown) => self.scroll_details(FEED_SCROLL_STEP),
            (_, KeyCode::PageUp) => self.scroll_details(-FEED_SCROLL_STEP),
            (false, KeyCode::Char('g') | KeyCode::Home) => self.scroll_details(i32::MIN),
            (false, KeyCode::Char('G') | KeyCode::End) => self.scroll_details(i32::MAX),
            (false, KeyCode::Char('m')) => self.apply(Action::ToggleMouse),
            (false, KeyCode::Char('?')) => self.apply(Action::ToggleHelp),
            _ => {}
        }
    }

    pub(super) fn handle_rename_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.mode = InputMode::Normal;
                self.rename_buf.clear();
            }
            KeyCode::Enter => self.submit_rename(),
            KeyCode::Char(c) => self.rename_buf.push(c),
            KeyCode::Backspace => {
                self.rename_buf.pop();
            }
            _ => {}
        }
    }

    pub(super) fn apply(&mut self, action: Action) {
        // A pending chord confirmation ("Press X again …") is shown as a
        // `Confirm` notification. Any key that isn't the chord's own repeat
        // breaks the chord, so the prompt must vanish at once — with nothing
        // taking its place (`clear_confirm` leaves any other status alone).
        //
        // Any input other than a repeated Back cancels a pending quit
        // confirmation (see `Action::Back`).
        if action != Action::Back && self.quit_armed {
            self.quit_armed = false;
            self.clear_confirm();
        }
        // Likewise, any input other than a repeated `d` cancels a pending
        // recording-delete confirmation (see `Action::DeleteRecording`).
        if action != Action::DeleteRecording && self.recordings_delete_armed {
            self.recordings_delete_armed = false;
            self.clear_confirm();
        }
        match action {
            Action::Quit => self.running = false,
            // Global "back": close the help overlay first if it's open, then
            // step out of the current screen toward Connections. From
            // Connections (the home screen) there is nowhere further back, so a
            // first Esc arms a quit confirmation and a second consecutive Esc
            // closes the app (Browser → Connections → Esc → Esc → quit).
            Action::Back => {
                if self.show_help {
                    self.show_help = false;
                    self.quit_armed = false;
                } else if self.screen != Screen::Home {
                    // Leaving the Browser unfocuses the panel: stop the
                    // focus-scoped feeds and drop back to normal navigation.
                    self.stop_focus_feeds();
                    self.mode = InputMode::Normal;
                    self.screen = Screen::Home;
                    self.quit_armed = false;
                } else if self.quit_armed {
                    self.running = false;
                } else {
                    self.quit_armed = true;
                    self.set_confirm("Press Esc again to quit".to_string());
                }
            }
            Action::Up => self.nav(-1),
            Action::Down => self.nav(1),
            // In the Browser these page the focused value pane and on the
            // Recordings tab the focused recording viewer (both list-navigated
            // with ↑↓ / g / G); on the Connections tab they page the list.
            Action::PageUp => match self.screen {
                Screen::Browser => self.scroll_value(-VALUE_SCROLL_STEP),
                Screen::Recordings => self.scroll_recording(-VALUE_SCROLL_STEP),
                Screen::Home => self.nav(-10),
            },
            Action::PageDown => match self.screen {
                Screen::Browser => self.scroll_value(VALUE_SCROLL_STEP),
                Screen::Recordings => self.scroll_recording(VALUE_SCROLL_STEP),
                Screen::Home => self.nav(10),
            },
            Action::Top => self.nav_edge(true),
            Action::Bottom => self.nav_edge(false),
            Action::Enter => match self.screen {
                Screen::Home => self.connect_selected_profile(),
                // On a group header, fold/unfold it; on a key, no-op.
                Screen::Browser => self.toggle_selected_group(),
                _ => {}
            },
            Action::AddConnection => {
                self.form = Some(ConnForm::new());
                self.mode = InputMode::Form;
            }
            // `b` jumps to the most recently viewed browser (falling back to the
            // active connection); reachable from either home-area tab.
            Action::GotoBrowser => self.goto_browser(),
            // `d` deletes the selected recording on the Recordings tab, after a
            // confirming second press; a no-op elsewhere.
            Action::DeleteRecording => {
                if self.screen == Screen::Recordings {
                    self.confirm_delete_recording();
                }
            }
            Action::StartFilter => {
                if self.screen == Screen::Browser && self.active.is_some() {
                    self.filter.clear();
                    self.mode = InputMode::Filter;
                }
            }
            // Tab / Shift-Tab move between tabs: in the Browser they cycle the
            // bottom-panel tabs (also starting/stopping the focus-scoped
            // MONITOR/keyspace feeds and setting the focused tab's mode); in the
            // home area they switch between the Connections and Recordings tabs.
            Action::PrevTab => match self.screen {
                Screen::Browser => self.cycle_panel(-1),
                Screen::Home | Screen::Recordings => self.switch_home_tab(),
            },
            Action::NextTab => match self.screen {
                Screen::Browser => self.cycle_panel(1),
                Screen::Home | Screen::Recordings => self.switch_home_tab(),
            },
            // `p` freezes/resumes the focused live feed's view.
            Action::PlayPause => self.toggle_play_pause(),
            // `x` closes the focused pub/sub or stream tab (the fixed tabs stay).
            Action::CloseTab => self.close_active_tab(),
            // `[`/`]` change DB in the Browser.
            Action::DbPrev => self.change_db(-1),
            Action::DbNext => self.change_db(1),
            // Browser key-list ordering and grouping. Each mutates the active
            // connection's view and reports the new state in the status bar.
            Action::CycleSort => self.browser_view(|c| {
                c.cycle_sort();
                format!(
                    "sort: {} {}",
                    c.browser.sort.label(),
                    sort_arrow(c.browser.sort_desc)
                )
            }),
            Action::ToggleSortDir => self.browser_view(|c| {
                c.toggle_sort_dir();
                format!(
                    "sort: {} {}",
                    c.browser.sort.label(),
                    sort_arrow(c.browser.sort_desc)
                )
            }),
            Action::ToggleAllGroups => self.browser_view(|c| {
                c.toggle_all_groups();
                "toggled all groups".to_string()
            }),
            Action::ToggleCollapse => self.toggle_selected_group(),
            // `r` on the Browser toggles recording for the focused live feed
            // (a no-op on the Console / Pub/Sub / Tail anchors — no feed to
            // record); on the Recordings tab it renames the selected recording.
            Action::Refresh => match self.screen {
                Screen::Browser => self.toggle_recording(),
                Screen::Recordings => self.start_rename(),
                Screen::Home => {}
            },
            Action::ToggleHelp => self.show_help = !self.show_help,
            // Flip the desired capture state and report it; the render loop
            // applies the change to the real terminal (keeping terminal I/O out
            // of `App`). Off hands native text selection back to the user.
            Action::ToggleMouse => {
                self.mouse_capture = !self.mouse_capture;
                let msg = if self.mouse_capture {
                    "Mouse capture on — scroll wheel scrolls (text selection off)"
                } else {
                    "Mouse capture off — drag to select/copy text (scroll wheel off)"
                };
                self.set_status(msg.to_string(), false);
            }
        }
    }

    pub(super) fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.mode = InputMode::Normal,
            KeyCode::Enter => {
                self.apply_filter();
                self.mode = InputMode::Normal;
            }
            KeyCode::Char(c) => self.filter.push(c),
            KeyCode::Backspace => {
                self.filter.pop();
            }
            _ => {}
        }
    }

    /// The Pub/Sub and Tail anchor prompts: type a spec and Enter to subscribe /
    /// tail. Focus moves (Tab / Ctrl-↑↓ / Esc) are handled in
    /// [`Self::handle_browser_key`] before this runs, so this only edits the buffer.
    pub(super) fn handle_subscribe_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => self.submit_subscribe(),
            KeyCode::Char(c) => self.subscribe_buf.push(c),
            KeyCode::Backspace => {
                self.subscribe_buf.pop();
            }
            _ => {}
        }
    }

    pub(super) fn handle_form_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.form = None;
                self.mode = InputMode::Normal;
            }
            KeyCode::Enter => self.submit_form(),
            // Tab / Shift-Tab are the sole field-movement keys; the arrow keys
            // were duplicate bindings and have been dropped.
            KeyCode::Tab => {
                if let Some(form) = &mut self.form {
                    form.focus_next();
                }
            }
            KeyCode::BackTab => {
                if let Some(form) = &mut self.form {
                    form.focus_prev();
                }
            }
            // ←/→ flip the focused boolean field (TLS / broker Kind). Space is no
            // longer a toggle — it types a literal space into the text fields,
            // matching every other text-entry surface.
            KeyCode::Left | KeyCode::Right => {
                if let Some(form) = &mut self.form {
                    match form.focus {
                        ConnForm::TLS_FOCUS => form.tls = !form.tls,
                        ConnForm::KIND_FOCUS => form.toggle_kind(),
                        _ => {}
                    }
                }
            }
            KeyCode::Char(c) => {
                if let Some(form) = &mut self.form {
                    if form.focus < ConnForm::FIELD_COUNT {
                        form.fields[form.focus].push(c);
                    }
                }
            }
            KeyCode::Backspace => {
                if let Some(form) = &mut self.form {
                    if form.focus < ConnForm::FIELD_COUNT {
                        form.fields[form.focus].pop();
                    }
                }
            }
            _ => {}
        }
    }

    // -- navigation ----------------------------------------------------------

    pub(super) fn nav(&mut self, delta: i32) {
        match self.screen {
            Screen::Home => {
                let len = self.profiles.len();
                let next = move_selection(self.profile_state.selected(), len, delta);
                self.profile_state.select(next);
            }
            Screen::Browser => {
                if let Some(idx) = self.active {
                    let conn = &mut self.connections[idx];
                    // Selection moves through rendered rows (group headers +
                    // keys), so it ranges over the view, not the raw key list.
                    let next = move_selection(
                        conn.browser.table.selected(),
                        conn.browser.view.len(),
                        delta,
                    );
                    conn.browser.table.select(next);
                }
                // Navigation only moves the highlight; the key list refreshes on
                // its own timer (see `on_tick`), not as a side effect of moving.
                if let Some(id) = self.active_id() {
                    self.request_selected_value(id);
                }
            }
            Screen::Recordings => {
                let len = self.recordings.len();
                let next = move_selection(self.recordings_state.selected(), len, delta);
                self.recordings_state.select(next);
                self.load_recording_view();
            }
        }
    }

    pub(super) fn nav_edge(&mut self, top: bool) {
        match self.screen {
            Screen::Home => {
                let len = self.profiles.len();
                if len > 0 {
                    self.profile_state
                        .select(Some(if top { 0 } else { len - 1 }));
                }
            }
            Screen::Browser => {
                if let Some(idx) = self.active {
                    let conn = &mut self.connections[idx];
                    let len = conn.browser.view.len();
                    if len > 0 {
                        conn.browser
                            .table
                            .select(Some(if top { 0 } else { len - 1 }));
                    }
                }
                if let Some(id) = self.active_id() {
                    self.request_selected_value(id);
                }
            }
            Screen::Recordings => {
                let len = self.recordings.len();
                if len > 0 {
                    self.recordings_state
                        .select(Some(if top { 0 } else { len - 1 }));
                }
                self.load_recording_view();
            }
        }
    }

    /// Scroll the Browser value pane by `delta` logical lines (negative = up).
    /// The offset is clamped against the value's height when rendered, so an
    /// over-scroll just rests at the bottom.
    pub(super) fn scroll_value(&mut self, delta: i32) {
        if let Some(conn) = self.active_conn_mut() {
            let next = conn.inspector.value_scroll as i32 + delta;
            conn.inspector.value_scroll = next.clamp(0, u16::MAX as i32) as u16;
        }
    }

    /// Scroll the Server Details client list by `delta` rows (negative = up).
    /// `i32::MIN`/`i32::MAX` jump to the top/bottom. The offset is clamped
    /// against the list height when rendered, so an over-scroll rests at the end.
    pub(super) fn scroll_details(&mut self, delta: i32) {
        if let Some(conn) = self.active_conn_mut() {
            // Saturating: `g`/`G` pass i32::MIN/MAX to jump to the ends, which a
            // plain add would overflow.
            let next = (conn.dashboard.details_scroll as i32).saturating_add(delta);
            conn.dashboard.details_scroll = next.clamp(0, u16::MAX as i32) as u16;
        }
    }

    /// Scroll the Recordings viewer pane by `delta` logical lines (negative =
    /// up). Clamped against the content height when rendered, so an over-scroll
    /// just rests at the bottom.
    pub(super) fn scroll_recording(&mut self, delta: i32) {
        let next = self.recordings_scroll as i32 + delta;
        self.recordings_scroll = next.clamp(0, u16::MAX as i32) as u16;
    }

    pub(super) fn change_db(&mut self, delta: i32) {
        if self.screen != Screen::Browser {
            return;
        }
        let Some(idx) = self.active else { return };
        let conn = &mut self.connections[idx];
        let max = conn.caps.databases.saturating_sub(1) as i32;
        let new_db = (conn.db as i32 + delta).clamp(0, max) as u32;
        if new_db == conn.db {
            return;
        }
        conn.db = new_db;
        let id = conn.id;
        self.set_status(format!("Switched to db{new_db}"), false);
        self.start_scan(id, true);
        // A focused keyspace feed is db-scoped, so restart it on the new db.
        self.sync_panel_focus();
    }

    /// Apply a view-setting mutation to the active connection while on the
    /// Browser, surfacing the status string `f` returns. No-op off the Browser
    /// or without an active connection.
    pub(super) fn browser_view<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Connection) -> String,
    {
        if self.screen != Screen::Browser {
            return;
        }
        let Some(conn) = self.active_conn_mut() else {
            return;
        };
        let msg = f(conn);
        self.set_status(msg, false);
    }

    /// Fold or unfold the group header under the cursor (Browser only). A no-op
    /// when a key row — not a group header — is selected.
    pub(super) fn toggle_selected_group(&mut self) {
        if self.screen != Screen::Browser {
            return;
        }
        if let Some(conn) = self.active_conn_mut() {
            conn.toggle_selected_group();
        }
    }

    /// Jump to a connection's key browser. Prefers the most recently viewed
    /// browser ([`Self::last_browser`]) so that with several brokers open `b`
    /// lands on the last one browsed; falls back to the active connection. A
    /// no-op (with an explanatory status when a connection exists but can't
    /// browse) otherwise.
    pub(super) fn goto_browser(&mut self) {
        let target = self
            .last_browser
            .filter(|id| self.conn_by_id(*id).is_some_and(|c| c.caps.can_browse))
            .or_else(|| {
                self.active_conn()
                    .filter(|c| c.caps.can_browse)
                    .map(|c| c.id)
            });
        match target {
            Some(id) => {
                self.active = self.connections.iter().position(|c| c.id == id);
                self.last_browser = Some(id);
                self.screen = Screen::Browser;
                // Enter with the keys pane focused; reconcile the panel
                // (mode + focused feed) on entry.
                if let Some(conn) = self.active_conn_mut() {
                    conn.focus = PaneFocus::Keys;
                }
                self.sync_panel_focus();
            }
            // A live but non-browsable broker (e.g. AMQP) earns a hint; with no
            // connection at all there is simply nothing to do.
            None if self.active_conn().is_some() => {
                self.set_status("this broker has no key browser".to_string(), true);
            }
            None => {}
        }
    }

    /// Switch between the two home-area tabs (Connections ↔ Recordings). Cycled
    /// with Tab / Shift-Tab; entering Recordings (re)scans the directory.
    pub(super) fn switch_home_tab(&mut self) {
        match self.screen {
            Screen::Home => self.enter_recordings_tab(),
            Screen::Recordings => {
                self.leave_recordings_tab();
                self.screen = Screen::Home;
            }
            Screen::Browser => {}
        }
    }

    /// Enter the Recordings tab: scan the directory afresh and reset its
    /// transient edit state.
    pub(super) fn enter_recordings_tab(&mut self) {
        self.mode = InputMode::Normal;
        self.recordings_delete_armed = false;
        self.rename_buf.clear();
        self.screen = Screen::Recordings;
        self.scan_recordings();
    }

    /// Leave the Recordings tab: drop any in-progress rename / delete-arm.
    fn leave_recordings_tab(&mut self) {
        self.mode = InputMode::Normal;
        self.recordings_delete_armed = false;
        self.rename_buf.clear();
    }
}
