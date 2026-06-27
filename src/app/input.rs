//! `App` input handling: key/mouse dispatch, the per-mode key handlers, and
//! list/pane navigation. Part of the `app` module (overview in `app.rs`).

use super::*;

/// The outcome of feeding one key to a single-line text-entry buffer. The
/// helper only appends to / pops from the buffer; the caller owns the mode
/// change and the cancel/submit actions (which need `&mut self`), so this
/// returns *what happened* rather than acting on `App`.
enum TextEdit {
    /// A character was typed, a backspace applied, or the key was ignored —
    /// stay in the current text mode.
    Editing,
    /// Esc: the caller should leave the text mode (and clear the buffer if it
    /// is a scratch buffer rather than persistent state like the filter).
    Cancelled,
    /// Enter: the caller should run its submit action.
    Submitted,
}

/// Apply one key to a single-line text buffer: append a typed char, pop on
/// backspace, and report Esc/Enter so the caller can cancel or submit. A free
/// function (not a method) so the `&mut String` borrow ends on return, leaving
/// the caller free to touch the rest of `&mut self`.
fn handle_text_input(buf: &mut String, key: KeyEvent) -> TextEdit {
    match key.code {
        KeyCode::Esc => TextEdit::Cancelled,
        KeyCode::Enter => TextEdit::Submitted,
        KeyCode::Char(c) => {
            buf.push(c);
            TextEdit::Editing
        }
        KeyCode::Backspace => {
            buf.pop();
            TextEdit::Editing
        }
        _ => TextEdit::Editing,
    }
}

impl App {
    // -- input ---------------------------------------------------------------

    pub(super) fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return; // ignore key-release/repeat (Windows emits these)
        }
        // The command-palette and settings overlays own the keyboard wholesale
        // until dismissed, so their navigation never leaks to the screen beneath.
        // Settings is checked first: it is reached *from* the palette (which has
        // closed by then), so they are mutually exclusive in practice.
        if self.settings.is_some() {
            return self.handle_settings_key(key);
        }
        if self.palette.is_some() {
            return self.handle_palette_key(key);
        }
        // The full-screen text-entry modals own the keyboard wholesale until they
        // are dismissed, so their input is never treated as a command.
        match self.mode {
            InputMode::Filter => return self.handle_filter_key(key),
            InputMode::Form => return self.handle_form_key(key),
            InputMode::Rename => return self.handle_rename_key(key),
            InputMode::AddDestination => return self.handle_add_destination_key(key),
            InputMode::Publish => return self.handle_publish_key(key),
            InputMode::PeekFilter => return self.handle_peek_filter_key(key),
            // Normal / Command / Subscribe are reconciled from the focused pane.
            InputMode::Normal | InputMode::Command | InputMode::Subscribe => {}
        }
        // The global keys (quit / back / palette / help / mouse) are handled once
        // here, ahead of every screen's dispatch, so they reach any non-text pane
        // without each handler re-implementing them.
        if self.try_global_key(key) {
            return;
        }
        match self.screen {
            Screen::Browser => self.handle_browser_key(key),
            // The Recordings tab is a two-pane browser (list + viewer) with its
            // own pane-focus model (Ctrl-←/→); see `handle_recordings_key`.
            Screen::Recordings => self.handle_recordings_key(key),
            // The Connections home screen has a single navigable list; keys map
            // straight to actions (the pane-focus model is Browser/Recordings-only).
            Screen::Home => {
                if let Some(action) = action::map_key(&key) {
                    self.apply(action);
                }
            }
        }
    }

    /// Keys handled globally, ahead of every screen's own dispatch — the only
    /// place these five live, so they can never drift between panes. Returns
    /// whether the key was consumed.
    ///
    /// Ctrl-C (quit) and Esc (back / leave the screen) fire in any non-modal
    /// mode, including the focused Console and subscribe anchors, so typing can
    /// never trap you. The palette (`:`), help (`?`) and mouse (`m`) toggles fire
    /// only in [`InputMode::Normal`]; while a text subpanel holds the keyboard
    /// they fall through to it as literal input. The full-screen text modals
    /// (filter / form / rename / …) are dispatched before this runs, so their
    /// Esc cancels the modal rather than leaving the screen.
    fn try_global_key(&mut self, key: KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (true, KeyCode::Char('c')) => self.apply(Action::Quit),
            (_, KeyCode::Esc) => self.apply(Action::Back),
            (false, KeyCode::Char(':')) if self.mode == InputMode::Normal => {
                self.apply(Action::OpenPalette)
            }
            (false, KeyCode::Char('?')) if self.mode == InputMode::Normal => {
                self.apply(Action::ToggleHelp)
            }
            (false, KeyCode::Char('m')) if self.mode == InputMode::Normal => {
                self.apply(Action::ToggleMouse)
            }
            _ => return false,
        }
        true
    }

    /// Dispatch a Recordings-tab key by the focused pane. Ctrl-←/→ move the
    /// keyboard between the recordings list (left) and the viewer (right). With
    /// the viewer focused, ↑/↓ and Home/End scroll the loaded recording rather
    /// than moving the list selection; every other key (and all keys while the
    /// list is focused) falls through to the shared home-screen keymap, so Tab,
    /// `r` and `d` work regardless of focus. The global keys (Esc, `:`, `?`, `m`,
    /// Ctrl-C) are handled upstream in [`Self::try_global_key`].
    fn handle_recordings_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            // Ctrl-←/→ move focus between the list and the viewer.
            (true, KeyCode::Left) => return self.set_recordings_focus(RecordingsFocus::List),
            (true, KeyCode::Right) => return self.set_recordings_focus(RecordingsFocus::Viewer),
            _ => {}
        }
        // With the viewer focused, the arrows scroll it; Home/End jump to the
        // ends (clamped against the content height when rendered).
        if self.recordings_focus == RecordingsFocus::Viewer {
            match (ctrl, key.code) {
                (false, KeyCode::Up) => return self.scroll_recording(-1),
                (false, KeyCode::Down) => return self.scroll_recording(1),
                (false, KeyCode::Home) => return self.scroll_recording(i32::MIN),
                (false, KeyCode::End) => return self.scroll_recording(i32::MAX),
                _ => {}
            }
        }
        if let Some(action) = action::map_key(&key) {
            self.apply(action);
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
            // Ctrl-↑/↓ move the keyboard between the keys pane (or AMQP body) and
            // the bottom subpanel — non-printable, so they work even while a text
            // subpanel is capturing input. (Ctrl-C / Esc are handled globally in
            // `try_global_key` before this runs.)
            (true, KeyCode::Up) => {
                self.set_pane_focus(PaneFocus::Keys);
                return;
            }
            (true, KeyCode::Down) => {
                self.set_pane_focus(PaneFocus::Bottom);
                return;
            }
            // Ctrl-←/→ walk the AMQP master-detail focus (destinations ⇄ message
            // list). Redis has no horizontal pane focus, so it ignores them.
            (true, KeyCode::Right) if self.active_is_amqp() => {
                self.focus_messages();
                return;
            }
            (true, KeyCode::Left) if self.active_is_amqp() => {
                self.unfocus_messages();
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
        } else if self.active_is_amqp() {
            // The AMQP left pane is the curated destination list, not a key list.
            self.handle_destination_key(key);
        } else if let Some(action) = action::map_keys_focus(&key) {
            self.apply(action);
        }
    }

    /// Whether the active connection is an AMQP 1.0 broker (a curated destination
    /// browser rather than a Redis key scan).
    pub(super) fn active_is_amqp(&self) -> bool {
        self.active_conn()
            .map(|c| c.caps.r#type == BrokerType::Amqp)
            .unwrap_or(false)
    }

    /// Keys while the AMQP destination list (left pane) is focused: navigate the
    /// list, Enter to open the selection (peek a queue / tail a topic), `a` to add
    /// a destination, `x`/`d` to remove one, `r` to (re)discover destinations from
    /// the broker, `t` to tail it, `P` to publish. `Ctrl-→` steps focus into the
    /// message list (handled in [`Self::handle_browser_key`]).
    ///
    /// When the message pane holds the keyboard the keys drive that pane instead
    /// (see [`Self::handle_message_key`]). The global keys (`:`/`?`/`m`/…) are
    /// handled upstream in [`Self::try_global_key`].
    fn handle_destination_key(&mut self, key: KeyEvent) {
        if self.peek_focused() {
            return self.handle_message_key(key);
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (false, KeyCode::Down) => self.nav(1),
            (false, KeyCode::Up) => self.nav(-1),
            (false, KeyCode::Home) => self.nav_edge(true),
            (false, KeyCode::End) => self.nav_edge(false),
            (false, KeyCode::Enter) => self.open_selected_destination(),
            (false, KeyCode::Char('a')) => self.begin_add_destination(),
            (false, KeyCode::Char('r')) => self.refresh_destinations(),
            (false, KeyCode::Char('x') | KeyCode::Char('d')) => self.delete_selected_destination(),
            (false, KeyCode::Char('t')) => self.tail_selected_destination(),
            (false, KeyCode::Char('P')) => self.begin_publish(),
            _ => {}
        }
    }

    /// Keys while the AMQP message list (right pane) holds the keyboard. ↑/↓ and
    /// Home/End move the message cursor — the selected message's body shows in the
    /// preview below (the analog of the Redis value pane) — `/` filters the list
    /// and `P` publishes. Focus moves (Ctrl-←/→ between destinations and messages)
    /// and the global keys (Esc / `:` / `?` / `m`) are handled upstream.
    fn handle_message_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (false, KeyCode::Down) => self.move_message(1),
            (false, KeyCode::Up) => self.move_message(-1),
            (false, KeyCode::Home) => self.message_to_edge(true),
            (false, KeyCode::End) => self.message_to_edge(false),
            (false, KeyCode::Char('/')) => self.begin_peek_filter(),
            (false, KeyCode::Char('P')) => self.begin_publish(),
            _ => {}
        }
    }

    /// Whether the AMQP message pane (right) currently holds the keyboard.
    fn peek_focused(&self) -> bool {
        self.active_conn().is_some_and(|c| c.peek.focused)
    }

    /// Move the keyboard into the message list, when the selected destination is
    /// a queue with peeked messages to browse. A no-op otherwise (so the key is
    /// harmless on a topic or an empty/unpeeked queue).
    fn focus_messages(&mut self) {
        self.with_active_conn(|conn| {
            if !conn.peek.events.is_empty() {
                conn.peek.focused = true;
                conn.peek.clamp_selection();
                conn.peek.scroll = 0;
            }
        });
    }

    /// Return the keyboard to the destination list.
    fn unfocus_messages(&mut self) {
        self.with_active_conn(|conn| {
            conn.peek.focused = false;
            conn.peek.scroll = 0;
        });
    }

    /// Move the message-list cursor by `delta`, resetting the scroll so the
    /// (single-line) selection stays in view.
    fn move_message(&mut self, delta: i32) {
        self.with_active_conn(|conn| conn.peek.move_selection(delta));
    }

    /// Jump the message-list cursor to the first/last filtered message.
    fn message_to_edge(&mut self, top: bool) {
        self.with_active_conn(|conn| {
            let len = conn.peek.filtered_len();
            conn.peek.selected = if top || len == 0 { 0 } else { len - 1 };
        });
    }

    /// Enter the message-list search-filter prompt (live: the list narrows as you
    /// type). A no-op unless the message pane is focused.
    fn begin_peek_filter(&mut self) {
        if self.peek_focused() {
            self.mode = InputMode::PeekFilter;
        }
    }

    /// Enter the publish prompt for the selected destination. Refused (with a
    /// status) when the connection can't publish or nothing is selected.
    fn begin_publish(&mut self) {
        let ready = self
            .active_conn()
            .is_some_and(|c| c.caps.can_publish && c.selected_destination().is_some());
        if !ready {
            self.set_status("select a destination to publish to".to_string(), true);
            return;
        }
        self.publish_buf.clear();
        self.mode = InputMode::Publish;
    }

    /// Re-discover destinations from the broker's management API for the active
    /// AMQP connection (the `r` key). Announces progress / "not configured",
    /// unlike the silent auto-discovery on connect.
    fn refresh_destinations(&mut self) {
        if let Some(id) = self.active_id() {
            self.discover_destinations(id, true);
        }
    }

    /// Enter the destination-add prompt (AMQP): capture a `topic:name` /
    /// `queue:name` spec, added on submit.
    fn begin_add_destination(&mut self) {
        self.subscribe_buf.clear();
        self.mode = InputMode::AddDestination;
    }

    /// Open the highlighted destination: a queue steps into its message pane when
    /// it already has peeked messages, otherwise it (re)issues the peek; a topic
    /// (which doesn't retain) opens a live tail.
    fn open_selected_destination(&mut self) {
        let Some(id) = self.active_id() else { return };
        match self
            .conn_by_id(id)
            .and_then(|c| c.selected_destination().map(|d| d.kind))
        {
            Some(DestKind::Queue) => {
                let has_messages = self
                    .conn_by_id(id)
                    .is_some_and(|c| !c.peek.events.is_empty());
                if has_messages {
                    self.focus_messages();
                } else {
                    self.request_peek(id);
                }
            }
            Some(DestKind::Topic) => self.tail_selected_destination(),
            None => {}
        }
    }

    /// Open a live tail on the highlighted destination (topic or queue).
    fn tail_selected_destination(&mut self) {
        if let Some(spec) = self
            .active_conn()
            .and_then(|c| c.selected_destination().map(|d| d.spec()))
        {
            self.start_subscribe(spec);
        }
    }

    /// The destination-add prompt's keys: type a spec, Enter adds it, Esc cancels.
    pub(super) fn handle_add_destination_key(&mut self, key: KeyEvent) {
        match handle_text_input(&mut self.subscribe_buf, key) {
            TextEdit::Editing => {}
            TextEdit::Cancelled => {
                self.subscribe_buf.clear();
                self.mode = InputMode::Normal;
            }
            TextEdit::Submitted => {
                let raw = self.subscribe_buf.trim().to_string();
                self.subscribe_buf.clear();
                self.mode = InputMode::Normal;
                if !raw.is_empty() {
                    match SubSpec::parse(&raw, 0) {
                        Ok(spec) => self.add_amqp_destination(spec),
                        Err(e) => self.set_status(e.to_string(), true),
                    }
                }
            }
        }
    }

    /// The publish prompt's keys: type a message body, Enter publishes it to the
    /// selected destination, Esc cancels. An empty body is a valid (empty)
    /// message, so it is not rejected.
    pub(super) fn handle_publish_key(&mut self, key: KeyEvent) {
        match handle_text_input(&mut self.publish_buf, key) {
            TextEdit::Editing => {}
            TextEdit::Cancelled => {
                self.publish_buf.clear();
                self.mode = InputMode::Normal;
            }
            TextEdit::Submitted => {
                let body = std::mem::take(&mut self.publish_buf);
                self.mode = InputMode::Normal;
                self.publish_to_selected(body);
            }
        }
    }

    /// The peek-filter prompt's keys: the filter applies live as you type, Enter
    /// commits and returns to the message list, Esc clears the filter and
    /// cancels. Each edit resets the message-list cursor to the top.
    pub(super) fn handle_peek_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                if let Some(conn) = self.active_conn_mut() {
                    conn.peek.filter.clear();
                    conn.peek.selected = 0;
                }
                self.mode = InputMode::Normal;
            }
            KeyCode::Enter => {
                if let Some(conn) = self.active_conn_mut() {
                    conn.peek.clamp_selection();
                }
                self.mode = InputMode::Normal;
            }
            KeyCode::Char(c) => {
                if let Some(conn) = self.active_conn_mut() {
                    conn.peek.filter.push(c);
                    conn.peek.selected = 0;
                }
            }
            KeyCode::Backspace => {
                if let Some(conn) = self.active_conn_mut() {
                    conn.peek.filter.pop();
                    conn.peek.selected = 0;
                }
            }
            _ => {}
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
    ///
    /// Landing on the bottom skips the passive Server Details tab: the first focus
    /// jumps to the interactive Console, so Ctrl-↓ / Tab puts you somewhere you can
    /// act. Any other remembered tab (`panel_tab` persists across focus changes —
    /// e.g. a tail you were just on) is kept, so this only fires from the default
    /// Server Details landing.
    pub(super) fn set_pane_focus(&mut self, focus: PaneFocus) {
        if focus == PaneFocus::Bottom && !self.active_can_tail() {
            return;
        }
        if let Some(conn) = self.active_conn_mut() {
            conn.focus = focus;
            if focus == PaneFocus::Bottom && conn.active_panel() == PanelTab::ServerDetails {
                if let Some(i) = conn
                    .panel_slots()
                    .iter()
                    .position(|t| *t == PanelTab::Console)
                {
                    conn.panel_tab = i;
                }
            }
        }
        if focus == PaneFocus::Bottom {
            // A fresh prompt on the Pub/Sub and Tail anchors when entering them.
            self.subscribe_buf.clear();
        }
        self.sync_panel_focus();
    }

    /// Tab / Shift-Tab: the first press from the keys pane drops the keyboard onto
    /// the bottom subpanel (landing on Console, not the passive Server Details —
    /// see [`Self::set_pane_focus`]); further presses cycle through the subpanels.
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
    /// stream tail) is focused: the feed always follows the newest event (it is
    /// not scrollable), so the only keys are p/x/r — play-pause / close / record.
    /// Focus moves are handled upstream.
    fn handle_feed_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (false, KeyCode::Char('p')) => self.toggle_play_pause(),
            (false, KeyCode::Char('x')) => self.close_active_tab(),
            (false, KeyCode::Char('r')) => self.toggle_recording(),
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
            (false, KeyCode::Down) => self.scroll_details(1),
            (false, KeyCode::Up) => self.scroll_details(-1),
            (false, KeyCode::Home) => self.scroll_details(i32::MIN),
            (false, KeyCode::End) => self.scroll_details(i32::MAX),
            _ => {}
        }
    }

    pub(super) fn handle_rename_key(&mut self, key: KeyEvent) {
        match handle_text_input(&mut self.rename_buf, key) {
            TextEdit::Editing => {}
            TextEdit::Cancelled => {
                self.mode = InputMode::Normal;
                self.rename_buf.clear();
            }
            TextEdit::Submitted => self.submit_rename(),
        }
    }

    pub(super) fn apply(&mut self, action: Action) {
        // A pending chord confirmation ("Press X again …") is shown as a
        // `Confirm` notification. Any action that isn't the chord's own repeat
        // (Back repeats a pending quit; DeleteRecording repeats a pending delete)
        // breaks the chord, so the prompt vanishes at once — with nothing taking
        // its place (`clear_confirm` leaves any other status alone).
        let is_chord_repeat = match self.confirm {
            ConfirmState::Quit => action == Action::Back,
            ConfirmState::DeleteRecording => action == Action::DeleteRecording,
            ConfirmState::None => true,
        };
        if !is_chord_repeat {
            self.confirm = ConfirmState::None;
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
                    self.confirm = ConfirmState::None;
                } else if self.screen != Screen::Home {
                    // Leaving the Browser unfocuses the panel: stop the
                    // focus-scoped feeds and drop back to normal navigation. The
                    // AMQP body focus returns to the destination list too, so a
                    // single Esc leaves from any drill level and re-entry starts
                    // on the destinations.
                    self.stop_focus_feeds();
                    if let Some(conn) = self.active_conn_mut() {
                        conn.peek.focused = false;
                    }
                    self.mode = InputMode::Normal;
                    self.screen = Screen::Home;
                    self.confirm = ConfirmState::None;
                } else if self.confirm == ConfirmState::Quit {
                    self.running = false;
                } else {
                    self.confirm = ConfirmState::Quit;
                    self.set_confirm("Press Esc again to quit".to_string());
                }
            }
            Action::Up => self.nav(-1),
            Action::Down => self.nav(1),
            Action::Top => self.nav_edge(true),
            Action::Bottom => self.nav_edge(false),
            // Enter is the context action: it connects on the Connections screen
            // and folds/unfolds the cursor's group in the Browser (replacing the
            // former Right binding — `l` still folds via `Action::ToggleGroup`).
            Action::Enter => match self.screen {
                Screen::Home => self.connect_selected_profile(),
                Screen::Browser => {
                    self.toggle_selected_group();
                }
                Screen::Recordings => {}
            },
            // `l`: on a group header (or a key within it), fold/unfold the group.
            Action::ToggleGroup => {
                if self.screen == Screen::Browser {
                    self.toggle_selected_group();
                }
            }
            Action::AddConnection => {
                self.form = Some(ConnForm::new());
                self.mode = InputMode::Form;
            }
            // `e` opens the edit form for the selected saved connection (and from
            // there it can be deleted); only meaningful on the Connections tab.
            Action::EditConnection => {
                if self.screen == Screen::Home {
                    self.edit_selected_profile();
                }
            }
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
            // `x` closes the focused pub/sub or stream tab in the Browser (the
            // fixed tabs stay); on the Connections tab it disconnects the
            // selected profile's live session instead.
            Action::CloseTab => match self.screen {
                Screen::Home => self.disconnect_selected_profile(),
                _ => self.close_active_tab(),
            },
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
            // `r` on the Browser toggles recording for the focused live feed
            // (a no-op on the Console / Pub/Sub / Tail anchors — no feed to
            // record); on the Recordings tab it renames the selected recording.
            Action::Refresh => match self.screen {
                Screen::Browser => self.toggle_recording(),
                Screen::Recordings => self.start_rename(),
                Screen::Home => {}
            },
            Action::ToggleHelp => self.show_help = !self.show_help,
            // `:` opens the command palette (the keys-pane/home discoverable
            // entry point). The overlay then captures input until dismissed.
            Action::OpenPalette => self.open_palette(),
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
        // Esc leaves the filter buffer intact (it is the persistent active
        // filter, not a scratch buffer), unlike the other text prompts.
        match handle_text_input(&mut self.filter, key) {
            TextEdit::Editing => {}
            TextEdit::Cancelled => self.mode = InputMode::Normal,
            TextEdit::Submitted => {
                self.apply_filter();
                self.mode = InputMode::Normal;
            }
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
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        // Ctrl-D requests deletion of the profile being edited; a confirming
        // second consecutive Ctrl-D actually deletes (see `form_delete_request`).
        // Handled before the text-entry arms so it never types a literal 'd'.
        if ctrl && matches!(key.code, KeyCode::Char('d')) {
            self.form_delete_request();
            return;
        }
        // Any other key breaks a pending delete confirmation.
        if let Some(form) = &mut self.form {
            form.confirm_delete = false;
        }

        match key.code {
            KeyCode::Esc => {
                self.form = None;
                self.mode = InputMode::Normal;
            }
            KeyCode::Enter => self.submit_form(),
            // ↑/↓ are the sole field-movement keys: the form is a vertical
            // stack, so Down steps to the next row and Up to the previous,
            // wrapping at the ends (see `ConnForm::step_focus`). The Tab keys
            // are intentionally not bound here.
            KeyCode::Down => {
                if let Some(form) = &mut self.form {
                    form.focus_next();
                }
            }
            KeyCode::Up => {
                if let Some(form) = &mut self.form {
                    form.focus_prev();
                }
            }
            // ←/→ act on the focused boolean/choice field: TLS flips either way,
            // while Type cycles in the arrow's direction (← back, → forward).
            // Space is no longer a toggle — it types a literal space into the
            // text fields, matching every other text-entry surface.
            KeyCode::Left | KeyCode::Right => {
                let forward = matches!(key.code, KeyCode::Right);
                if let Some(form) = &mut self.form {
                    match form.focus {
                        ConnForm::TLS_FOCUS => form.tls = !form.tls,
                        ConnForm::TYPE_FOCUS => form.cycle_type(forward),
                        _ => {}
                    }
                }
            }
            // Plain typing edits the focused text field; control-modified chars
            // (other than the Ctrl-D handled above) are ignored rather than
            // injected as literal letters.
            KeyCode::Char(c) if !ctrl => {
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
                    if conn.caps.uses_key_scan() {
                        // Selection moves through rendered rows (group headers +
                        // keys), so it ranges over the view, not the raw key list.
                        let next = move_selection(
                            conn.browser.table.selected(),
                            conn.browser.view.len(),
                            delta,
                        );
                        conn.browser.table.select(next);
                    } else {
                        // AMQP: move through the curated destination list.
                        let next = move_selection(
                            conn.destinations.table.selected(),
                            conn.destinations.items.len(),
                            delta,
                        );
                        conn.destinations.table.select(next);
                    }
                }
                // Redis loads the selected key's value as the highlight moves
                // (cheap, multiplexed). AMQP does NOT peek on move — each peek
                // opens its own connection — so it peeks only on explicit open.
                if let Some(id) = self.active_id() {
                    if self.conn_by_id(id).is_some_and(|c| c.caps.uses_key_scan()) {
                        self.request_selected_value(id);
                    }
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
                    if conn.caps.uses_key_scan() {
                        let len = conn.browser.view.len();
                        if len > 0 {
                            conn.browser
                                .table
                                .select(Some(if top { 0 } else { len - 1 }));
                        }
                    } else {
                        let len = conn.destinations.items.len();
                        if len > 0 {
                            conn.destinations
                                .table
                                .select(Some(if top { 0 } else { len - 1 }));
                        }
                    }
                }
                if let Some(id) = self.active_id() {
                    if self.conn_by_id(id).is_some_and(|c| c.caps.uses_key_scan()) {
                        self.request_selected_value(id);
                    }
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

    /// Scroll the Server Details client list by `delta` rows (negative = up).
    /// `i32::MIN`/`i32::MAX` jump to the top/bottom. The offset is clamped
    /// against the list height when rendered, so an over-scroll rests at the end.
    pub(super) fn scroll_details(&mut self, delta: i32) {
        if let Some(conn) = self.active_conn_mut() {
            // Saturating: Home/End pass i32::MIN/MAX to jump to the ends, which a
            // plain add would overflow.
            let next = (conn.dashboard.details_scroll as i32).saturating_add(delta);
            conn.dashboard.details_scroll = next.clamp(0, u16::MAX as i32) as u16;
        }
    }

    /// Move the keyboard between the Recordings list (left) and the viewer
    /// (right). Focusing the viewer is a no-op when no recording is loaded (an
    /// empty tab has nothing to scroll), so the key is harmless there.
    pub(super) fn set_recordings_focus(&mut self, focus: RecordingsFocus) {
        if focus == RecordingsFocus::Viewer && self.recording_view.is_none() {
            return;
        }
        self.recordings_focus = focus;
    }

    /// Scroll the recording viewer by `delta` logical lines (negative = up).
    /// `i32::MIN`/`i32::MAX` jump to the top/bottom. The offset is clamped
    /// against the content height when rendered, so an over-scroll rests at the
    /// end (mirroring [`Self::scroll_details`]).
    pub(super) fn scroll_recording(&mut self, delta: i32) {
        // Saturating: Home/End pass i32::MIN/MAX to jump to the ends, which a
        // plain add would overflow.
        let next = (self.recordings_scroll as i32).saturating_add(delta);
        self.recordings_scroll = next.clamp(0, u16::MAX as i32) as u16;
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
        self.confirm = ConfirmState::None;
        self.rename_buf.clear();
        // Always land on the list, so ↑/↓ move the selection on entry.
        self.recordings_focus = RecordingsFocus::List;
        self.screen = Screen::Recordings;
        self.scan_recordings();
    }

    /// Leave the Recordings tab: drop any in-progress rename / delete-arm and
    /// return focus to the list so re-entry starts there.
    fn leave_recordings_tab(&mut self) {
        self.mode = InputMode::Normal;
        self.confirm = ConfirmState::None;
        self.rename_buf.clear();
        self.recordings_focus = RecordingsFocus::List;
    }
}
