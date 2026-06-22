//! `App` input handling: key/mouse dispatch, the per-mode key handlers, and
//! list/pane navigation. Part of the `app` module (overview in `app.rs`).

use super::*;

impl App {
    // -- input ---------------------------------------------------------------

    pub(super) fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return; // ignore key-release/repeat (Windows emits these)
        }
        // Keep the panel reconciled with the focused tab on every Browser key:
        // the mode tracks the tab (so the Console / Pub/Sub / Tail prompts are
        // always live) and the focused MONITOR/keyspace feed runs. The text-entry
        // modals (Filter, Form) are excluded so their input is never clobbered.
        if self.screen == Screen::Browser
            && matches!(
                self.mode,
                InputMode::Normal | InputMode::Command | InputMode::Subscribe
            )
        {
            self.sync_panel_focus();
        }
        match self.mode {
            InputMode::Normal => {
                if let Some(action) = action::map_key(&key) {
                    self.apply(action);
                }
            }
            InputMode::Filter => self.handle_filter_key(key),
            InputMode::Form => self.handle_form_key(key),
            InputMode::Subscribe => self.handle_subscribe_key(key),
            InputMode::Command => self.handle_command_key(key),
        }
    }

    pub(super) fn apply(&mut self, action: Action) {
        // Any input other than a repeated Back cancels a pending quit
        // confirmation (see `Action::Back`).
        if action != Action::Back {
            self.quit_armed = false;
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
                } else if self.screen != Screen::Connections {
                    // Leaving the Browser unfocuses the panel: stop the
                    // focus-scoped feeds and drop back to normal navigation.
                    self.stop_focus_feeds();
                    self.mode = InputMode::Normal;
                    self.screen = Screen::Connections;
                    self.quit_armed = false;
                } else if self.quit_armed {
                    self.running = false;
                } else {
                    self.quit_armed = true;
                    self.set_status("Press Esc again to quit".to_string(), false);
                }
            }
            Action::Up => self.nav(-1),
            Action::Down => self.nav(1),
            // In the Browser these page the focused value pane (the key list
            // still has ↑↓ / g / G / n); on every other screen they page the
            // focused list.
            Action::PageUp => {
                if self.screen == Screen::Browser {
                    self.scroll_value(-VALUE_SCROLL_STEP);
                } else {
                    self.nav(-10);
                }
            }
            Action::PageDown => {
                if self.screen == Screen::Browser {
                    self.scroll_value(VALUE_SCROLL_STEP);
                } else {
                    self.nav(10);
                }
            }
            Action::Top => self.nav_edge(true),
            Action::Bottom => self.nav_edge(false),
            Action::Enter => match self.screen {
                Screen::Connections => self.connect_selected_profile(),
                // On a group header, fold/unfold it; on a key, no-op.
                Screen::Browser => self.toggle_selected_group(),
                _ => {}
            },
            Action::AddConnection => {
                self.form = Some(ConnForm::new());
                self.mode = InputMode::Form;
            }
            // Reachable from Connections; a no-op from the Recordings screen,
            // which only steps back to Connections.
            Action::GotoBrowser if self.screen != Screen::Recordings => {
                match self.active_conn().map(|c| c.caps.can_browse) {
                    Some(true) => {
                        self.screen = Screen::Browser;
                        // Reconcile the panel (mode + focused feed) on entry.
                        self.sync_panel_focus();
                    }
                    Some(false) => {
                        self.set_status("this broker has no key browser".to_string(), true)
                    }
                    None => {}
                }
            }
            Action::GotoBrowser => {}
            Action::GotoRecordings => {
                // Leaving the Browser unfocuses the panel.
                self.stop_focus_feeds();
                self.mode = InputMode::Normal;
                self.screen = Screen::Recordings;
                self.scan_recordings();
            }
            Action::StartFilter => {
                if self.screen == Screen::Browser && self.active.is_some() {
                    self.filter.clear();
                    self.mode = InputMode::Filter;
                }
            }
            // Tab / Shift-Tab cycle the Browser's bottom-panel tabs — the only
            // way to move between them. Each cycle also starts/stops the
            // focus-scoped MONITOR/keyspace feeds and sets the focused tab's mode.
            Action::PrevTab => self.cycle_panel(-1),
            Action::NextTab => self.cycle_panel(1),
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
            // `r` on the Browser toggles recording for the focused live feed,
            // and only there — it is a no-op on the Console / Pub/Sub / Tail
            // anchors (no feed to record). Manual key-list refresh was dropped;
            // the keyspace auto-refreshes on its own timer. It does nothing on
            // the other screens — the Recordings list (re)scans on entry.
            Action::Refresh => {
                if self.screen == Screen::Browser {
                    self.toggle_recording();
                }
            }
            Action::ToggleHelp => self.show_help = !self.show_help,
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

    /// Keys that work the same on every text-input anchor (Console / Pub/Sub /
    /// Tail): Tab / Shift-Tab cycle tabs, Esc steps out of the Browser, and the
    /// arrow keys still move the key list so browsing works while a prompt is
    /// focused. Returns whether the key was consumed.
    pub(super) fn browser_input_nav(&mut self, key: &KeyEvent) -> bool {
        match key.code {
            KeyCode::Tab => self.cycle_panel(1),
            KeyCode::BackTab => self.cycle_panel(-1),
            KeyCode::Esc => self.apply(Action::Back),
            KeyCode::Up => self.nav(-1),
            KeyCode::Down => self.nav(1),
            _ => return false,
        }
        true
    }

    pub(super) fn handle_subscribe_key(&mut self, key: KeyEvent) {
        if self.browser_input_nav(&key) {
            return;
        }
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
            KeyCode::Tab | KeyCode::Down => {
                if let Some(form) = &mut self.form {
                    form.focus_next();
                }
            }
            KeyCode::BackTab | KeyCode::Up => {
                if let Some(form) = &mut self.form {
                    form.focus_prev();
                }
            }
            KeyCode::Char(c) => {
                if let Some(form) = &mut self.form {
                    match form.focus {
                        ConnForm::TLS_FOCUS => {
                            if matches!(c, ' ' | 't' | 'f' | 'y' | 'n') {
                                form.tls = !form.tls;
                            }
                        }
                        ConnForm::KIND_FOCUS => {
                            if c == ' ' {
                                form.toggle_kind();
                            }
                        }
                        f if f < ConnForm::FIELD_COUNT => form.fields[f].push(c),
                        _ => {}
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
            Screen::Connections => {
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
                self.load_recording_preview();
            }
        }
    }

    pub(super) fn nav_edge(&mut self, top: bool) {
        match self.screen {
            Screen::Connections => {
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
                self.load_recording_preview();
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
}
