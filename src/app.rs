//! Application state machine: owns all UI state and the open connections, turns
//! [`AppEvent`]s into state changes, and drives connection actors. The render
//! loop is the sole owner of this type, so no locking is needed for UI state.

mod action;
mod state;

pub use state::{
    ConnForm, Connection, Console, ConsoleEntry, InputMode, PaletteState, RecordState,
    RecordingFile, ScanStep, Screen, Status, SubState, Subscription, ViewRow,
};

use std::path::PathBuf;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEventKind};
use ratatui::widgets::{ListState, TableState};
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::action::Action;
use crate::broker::actor::{spawn_connection, ConnCommand, ConnHandle};
#[cfg(feature = "amqp")]
use crate::broker::amqp::AmqpConnection;
#[cfg(feature = "rabbitmq")]
use crate::broker::rabbitmq::RabbitmqConnection;
use crate::broker::redis::RedisConnection;
use crate::broker::{
    BrokerConnection, BrokerEvent, BrokerKind, BrowsePage, Capabilities, ConnId, InspectReq,
    ServerStats, SubSpec, ValueType, ValueView,
};
use crate::config::{self, AmqpProfile, Config, ConnectionConfig, RabbitmqProfile, RedisProfile};
use crate::event::AppEvent;
use crate::recording::RecordingStatus;
use crate::theme::Theme;

/// Nominal period of one UI tick, mirroring `crate::TICK_PERIOD`. Used to turn
/// a configured refresh interval (milliseconds) into a tick count.
const TICK_PERIOD_MS: u64 = 250;
/// How many ticks (~250ms each) between automatic dashboard stat refreshes.
const STATS_REFRESH_TICKS: u32 = 8;
/// How many elements of a value to fetch into the inspector at a time.
const VALUE_LIMIT: usize = 200;
/// Lines the Browser value pane scrolls per PageUp/PageDown.
const VALUE_SCROLL_STEP: i32 = 10;
/// Lines the Browser console band scrolls per PageUp/PageDown while focused —
/// roughly one band's worth of output rows (see `CONSOLE_BAND_HEIGHT` in `ui`).
const CONSOLE_SCROLL_STEP: i32 = 4;

/// The whole application as seen by the render loop.
pub struct App {
    pub running: bool,

    // Infrastructure for spawning broker work.
    events: Sender<AppEvent>,
    tracker: TaskTracker,
    cancel: CancellationToken,

    // Config.
    config: Config,
    config_path: PathBuf,
    preview_bytes: usize,
    scan_count: usize,
    tail_scrollback: usize,
    /// Ticks between automatic key-browser refreshes (`0` disables them),
    /// derived from `settings.browse_refresh_ms`.
    browse_refresh_ticks: u32,
    recordings_dir: PathBuf,
    next_id: u32,
    next_sub_id: u32,
    pending_connect: Option<String>,

    // UI state (read by `crate::ui`).
    pub(crate) theme: Theme,
    pub(crate) profiles: Vec<ConnectionConfig>,
    pub(crate) profile_state: TableState,
    pub(crate) connections: Vec<Connection>,
    pub(crate) active: Option<usize>,
    pub(crate) screen: Screen,
    pub(crate) mode: InputMode,
    pub(crate) filter: String,
    pub(crate) subscribe_buf: String,
    pub(crate) form: Option<ConnForm>,
    pub(crate) palette: Option<PaletteState>,
    pub(crate) status: Option<Status>,
    pub(crate) show_help: bool,
    pub(crate) recordings: Vec<RecordingFile>,
    pub(crate) recordings_state: ListState,
    pub(crate) now: OffsetDateTime,
    /// Set when a quit was requested from the home screen but not yet
    /// confirmed: closing the app needs a second consecutive Esc.
    quit_armed: bool,
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        config_path: PathBuf,
        recordings_dir: PathBuf,
        events: Sender<AppEvent>,
        tracker: TaskTracker,
        cancel: CancellationToken,
        connect_on_start: Option<String>,
    ) -> Self {
        let profiles: Vec<ConnectionConfig> = config.connections.clone();
        let preview_bytes = config.settings.value_preview_bytes;
        let scan_count = config.settings.scan_count;
        let tail_scrollback = config.settings.tail_scrollback;
        let browse_refresh_ticks = refresh_ticks(config.settings.browse_refresh_ms);
        let theme = Theme::resolve(&config.theme);
        let mut profile_state = TableState::default();
        if !profiles.is_empty() {
            profile_state.select(Some(0));
        }

        Self {
            running: true,
            events,
            tracker,
            cancel,
            config,
            config_path,
            preview_bytes,
            scan_count,
            tail_scrollback,
            browse_refresh_ticks,
            recordings_dir,
            next_id: 1,
            next_sub_id: 1,
            pending_connect: connect_on_start,
            theme,
            profiles,
            profile_state,
            connections: Vec::new(),
            active: None,
            screen: Screen::Connections,
            mode: InputMode::Normal,
            filter: String::new(),
            subscribe_buf: String::new(),
            form: None,
            palette: None,
            status: None,
            show_help: false,
            recordings: Vec::new(),
            recordings_state: ListState::default(),
            now: OffsetDateTime::now_utc(),
            quit_armed: false,
        }
    }

    /// Kick off an auto-connect requested via `--connect`.
    pub fn on_start(&mut self) {
        if let Some(name) = self.pending_connect.take() {
            match self.profiles.iter().find(|p| p.name() == name).cloned() {
                Some(profile) => self.start_connect(profile, None),
                None => self.set_status(format!("no connection profile named '{name}'"), true),
            }
        }
    }

    // -- accessors for the UI ------------------------------------------------

    pub fn active_conn(&self) -> Option<&Connection> {
        self.active.and_then(|i| self.connections.get(i))
    }

    pub fn active_conn_mut(&mut self) -> Option<&mut Connection> {
        match self.active {
            Some(i) => self.connections.get_mut(i),
            None => None,
        }
    }

    /// True if a profile of this name currently has an open connection.
    pub fn is_connected(&self, name: &str) -> bool {
        self.connections.iter().any(|c| c.name == name)
    }

    /// Labels of the palette items matching the current query (for rendering).
    /// Empty when the palette is closed.
    pub(crate) fn palette_labels(&self) -> Vec<&'static str> {
        match &self.palette {
            Some(p) => action::palette_matches(&p.query)
                .iter()
                .map(|item| item.label)
                .collect(),
            None => Vec::new(),
        }
    }

    // -- event handling ------------------------------------------------------

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
    fn handle_mouse(&mut self, kind: MouseEventKind) {
        if self.mode != InputMode::Normal {
            return;
        }
        match kind {
            MouseEventKind::ScrollDown => self.nav(1),
            MouseEventKind::ScrollUp => self.nav(-1),
            _ => {}
        }
    }

    fn on_tick(&mut self) {
        self.now = OffsetDateTime::now_utc();
        let refresh_ticks = self.browse_refresh_ticks;
        let on_browser = self.screen == Screen::Browser;
        let mut refresh_id = None;
        if let Some(conn) = self.active_conn_mut() {
            conn.stat_ticks += 1;
            if conn.stat_ticks >= STATS_REFRESH_TICKS {
                conn.stat_ticks = 0;
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
            if refresh_ticks > 0 && on_browser && conn.caps.can_browse {
                conn.browse_ticks += 1;
                if conn.browse_ticks >= refresh_ticks {
                    conn.browse_ticks = 0;
                    if !conn.scanning {
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

    fn on_connected(&mut self, handle: ConnHandle) {
        let conn = Connection::new(handle);
        let id = conn.id;
        let name = conn.name.clone();
        let caps = conn.caps.clone();
        self.connections.push(conn);
        self.active = Some(self.connections.len() - 1);
        self.screen = initial_screen(&caps);
        self.set_status(format!("Connected to {name}"), false);
        // Kick off the broker-appropriate first load.
        if caps.can_browse {
            self.start_scan(id, true);
        }
        if caps.can_dashboard {
            self.request_stats(id);
        }
    }

    fn on_disconnected(&mut self, id: ConnId, reason: String) {
        if let Some(idx) = self.connections.iter().position(|c| c.id == id) {
            let name = self.connections[idx].name.clone();
            self.connections[idx].handle.shutdown();
            self.connections.remove(idx);
            self.active = if self.connections.is_empty() {
                None
            } else {
                Some(0)
            };
            if self.connections.is_empty() {
                self.screen = Screen::Connections;
            }
            self.set_status(format!("{name} disconnected: {reason}"), true);
        }
    }

    fn on_keys_page(&mut self, id: ConnId, page: BrowsePage) {
        let page_size = self.scan_count;
        // Fold the page into the scan in progress. A multi-page scan drives
        // itself to completion by issuing the next request here (not on
        // navigation), so the full keyspace loads on its own.
        let next = match self.conn_by_id_mut(id) {
            Some(conn) => match conn.apply_page(page, page_size) {
                ScanStep::Stale => return, // page from a superseded scan; drop it
                ScanStep::Continue(req) => Some(req),
                ScanStep::Done => None,
            },
            None => return,
        };
        // Land the highlight on the first row as soon as there is one to show.
        if let Some(conn) = self.conn_by_id_mut(id) {
            if conn.table.selected().is_none() && !conn.view.is_empty() {
                conn.table.select(Some(0));
            }
        }
        if let Some(req) = next {
            if let Some(conn) = self.conn_by_id(id) {
                conn.handle.send(ConnCommand::Browse(req));
            }
        }
        self.request_selected_value(id);
    }

    fn on_value(&mut self, id: ConnId, key: String, value: ValueView) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if conn.value_key.as_deref() == Some(key.as_str()) {
                conn.value = Some(value);
            }
        }
    }

    fn on_stats(&mut self, id: ConnId, stats: ServerStats) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            conn.stats = Some(stats);
        }
    }

    fn on_conn_error(&mut self, id: ConnId, context: String, error: String) {
        self.set_status(format!("[{}] {context}: {error}", id.0), true);
    }

    // -- realtime events -----------------------------------------------------

    fn on_realtime(&mut self, id: ConnId, sub_id: u32, event: BrokerEvent) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                // First event implicitly confirms the tail is live.
                if sub.state == SubState::Connecting {
                    sub.state = SubState::Active;
                }
                sub.push(event);
            }
        }
    }

    fn on_sub_started(&mut self, id: ConnId, sub_id: u32) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                if sub.state == SubState::Connecting {
                    sub.state = SubState::Active;
                }
            }
        }
    }

    fn on_sub_notice(&mut self, id: ConnId, sub_id: u32, notice: String) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(sub) = conn.sub_by_id_mut(sub_id) {
                sub.notice = Some(notice.clone());
            }
        }
        self.set_status(notice, true);
    }

    fn on_command_result(&mut self, id: ConnId, command: String, result: Result<String, String>) {
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

    fn on_sub_ended(&mut self, id: ConnId, sub_id: u32, reason: Option<String>) {
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

    fn on_recording_update(&mut self, id: ConnId, sub_id: u32, status: RecordingStatus) {
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

    // -- input ---------------------------------------------------------------

    fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return; // ignore key-release/repeat (Windows emits these)
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
            InputMode::Palette => self.handle_palette_key(key),
        }
    }

    fn apply(&mut self, action: Action) {
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
            Action::GotoConnections => self.screen = Screen::Connections,
            Action::GotoBrowser => match self.active_conn().map(|c| c.caps.can_browse) {
                Some(true) => self.screen = Screen::Browser,
                Some(false) => self.set_status("this broker has no key browser".to_string(), true),
                None => {}
            },
            Action::GotoRealtime => {
                if self.active.is_some() {
                    self.screen = Screen::Realtime;
                }
            }
            Action::GotoRecordings => {
                self.screen = Screen::Recordings;
                self.scan_recordings();
            }
            Action::StartFilter => {
                if self.screen == Screen::Browser && self.active.is_some() {
                    self.filter.clear();
                    self.mode = InputMode::Filter;
                }
            }
            Action::Subscribe => self.open_subscribe_prompt(),
            Action::StartMonitor => self.start_special_tail(SubSpec::Monitor),
            Action::StartKeyspace => {
                let db = self.active_conn().map(|c| c.db).unwrap_or(0);
                self.start_special_tail(SubSpec::Keyspace { db });
            }
            // `i` focuses the Browser's console band to type a command (only on
            // the Browser, and only for brokers that have a console).
            Action::ConsoleEdit => {
                let has_console = self
                    .active_conn()
                    .map(|c| c.caps.can_console)
                    .unwrap_or(false);
                if self.screen == Screen::Browser && has_console {
                    self.enter_command_mode();
                }
            }
            Action::OpenPalette => self.open_palette(),
            Action::TailKey => self.tail_selected_key(),
            Action::PrevTab => {
                if self.screen == Screen::Realtime {
                    self.focus_tab(-1);
                }
            }
            Action::NextTab => {
                if self.screen == Screen::Realtime {
                    self.focus_tab(1);
                }
            }
            Action::StopTail => {
                if self.screen == Screen::Realtime {
                    self.stop_active_tail();
                }
            }
            // `[`/`]`: change DB in the Browser, switch tail tabs in Realtime.
            Action::DbPrev => match self.screen {
                Screen::Browser => self.change_db(-1),
                Screen::Realtime => self.focus_tab(-1),
                _ => {}
            },
            Action::DbNext => match self.screen {
                Screen::Browser => self.change_db(1),
                Screen::Realtime => self.focus_tab(1),
                _ => {}
            },
            // Browser key-list ordering and grouping. Each mutates the active
            // connection's view and reports the new state in the status bar.
            Action::CycleSort => self.browser_view(|c| {
                c.cycle_sort();
                format!("sort: {} {}", c.sort.label(), sort_arrow(c.sort_desc))
            }),
            Action::ToggleSortDir => self.browser_view(|c| {
                c.toggle_sort_dir();
                format!("sort: {} {}", c.sort.label(), sort_arrow(c.sort_desc))
            }),
            Action::ToggleAllGroups => self.browser_view(|c| {
                c.toggle_all_groups();
                "toggled all groups".to_string()
            }),
            Action::ToggleCollapse => self.toggle_selected_group(),
            // `r`: refresh data (Browser — keys and the server-stats band),
            // toggle recording (Realtime), rescan files (Recordings).
            Action::Refresh => match self.screen {
                Screen::Browser => {
                    if let Some(id) = self.active_id() {
                        self.start_scan(id, true);
                        self.request_stats(id);
                    }
                }
                Screen::Realtime => self.toggle_recording(),
                Screen::Recordings => {
                    self.scan_recordings();
                    self.set_status("recordings refreshed".to_string(), false);
                }
                Screen::Connections => {}
            },
            Action::ToggleHelp => self.show_help = !self.show_help,
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
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

    fn handle_subscribe_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.mode = InputMode::Normal,
            KeyCode::Enter => {
                self.submit_subscribe();
            }
            KeyCode::Char(c) => self.subscribe_buf.push(c),
            KeyCode::Backspace => {
                self.subscribe_buf.pop();
            }
            _ => {}
        }
    }

    fn handle_form_key(&mut self, key: KeyEvent) {
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

    fn nav(&mut self, delta: i32) {
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
                    let next = move_selection(conn.table.selected(), conn.view.len(), delta);
                    conn.table.select(next);
                }
                // Navigation only moves the highlight; the key list refreshes on
                // its own timer (see `on_tick`), not as a side effect of moving.
                if let Some(id) = self.active_id() {
                    self.request_selected_value(id);
                }
            }
            Screen::Realtime => self.scroll_tail(delta),
            Screen::Recordings => {
                let len = self.recordings.len();
                let next = move_selection(self.recordings_state.selected(), len, delta);
                self.recordings_state.select(next);
            }
        }
    }

    fn nav_edge(&mut self, top: bool) {
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
                    let len = conn.view.len();
                    if len > 0 {
                        conn.table.select(Some(if top { 0 } else { len - 1 }));
                    }
                }
                if let Some(id) = self.active_id() {
                    self.request_selected_value(id);
                }
            }
            Screen::Realtime => {
                if let Some(sub) = self.active_sub_mut() {
                    if top {
                        sub.follow = false;
                        sub.offset = 0;
                    } else {
                        // G: jump to newest and resume following.
                        sub.follow = true;
                        sub.offset = 0;
                    }
                }
            }
            Screen::Recordings => {
                let len = self.recordings.len();
                if len > 0 {
                    self.recordings_state
                        .select(Some(if top { 0 } else { len - 1 }));
                }
            }
        }
    }

    /// Scroll the Browser value pane by `delta` logical lines (negative = up).
    /// The offset is clamped against the value's height when rendered, so an
    /// over-scroll just rests at the bottom.
    fn scroll_value(&mut self, delta: i32) {
        if let Some(conn) = self.active_conn_mut() {
            let next = conn.value_scroll as i32 + delta;
            conn.value_scroll = next.clamp(0, u16::MAX as i32) as u16;
        }
    }

    fn change_db(&mut self, delta: i32) {
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
    }

    /// Apply a view-setting mutation to the active connection while on the
    /// Browser, surfacing the status string `f` returns. No-op off the Browser
    /// or without an active connection.
    fn browser_view<F>(&mut self, f: F)
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
    fn toggle_selected_group(&mut self) {
        if self.screen != Screen::Browser {
            return;
        }
        if let Some(conn) = self.active_conn_mut() {
            conn.toggle_selected_group();
        }
    }

    // -- connection lifecycle ------------------------------------------------

    fn connect_selected_profile(&mut self) {
        let Some(sel) = self.profile_state.selected() else {
            return;
        };
        let Some(profile) = self.profiles.get(sel).cloned() else {
            return;
        };
        if let Some(idx) = self
            .connections
            .iter()
            .position(|c| c.name == profile.name())
        {
            self.active = Some(idx);
            self.screen = initial_screen(&self.connections[idx].caps);
            return;
        }
        self.start_connect(profile, None);
    }

    fn start_connect(&mut self, profile: ConnectionConfig, override_password: Option<String>) {
        let id = ConnId(self.next_id);
        self.next_id += 1;
        let events = self.events.clone();
        let tracker = self.tracker.clone();
        let cancel = self.cancel.clone();
        let preview = self.preview_bytes;
        let recordings_dir = self.recordings_dir.clone();
        let name = profile.name().to_string();
        self.set_status(format!("Connecting to {name}…"), false);

        tokio::spawn(async move {
            // Resolve the secret off the render thread (keyring access can block).
            let (spec, account) = profile.secret_account();
            let password = match override_password {
                Some(pw) => Some(pw),
                None => match resolve_secret(spec, account).await {
                    Ok(pw) => pw,
                    Err(e) => {
                        let _ = events
                            .send(AppEvent::ConnError {
                                id,
                                context: "auth".to_string(),
                                error: e.to_string(),
                            })
                            .await;
                        return;
                    }
                },
            };
            let conn: Box<dyn BrokerConnection> = match profile {
                ConnectionConfig::Redis(p) => Box::new(RedisConnection::new(p, password, preview)),
                #[cfg(feature = "amqp")]
                ConnectionConfig::Amqp(p) => Box::new(AmqpConnection::new(p, password)),
                #[cfg(not(feature = "amqp"))]
                ConnectionConfig::Amqp(_) => {
                    let _ = events
                        .send(AppEvent::ConnError {
                            id,
                            context: "connect".to_string(),
                            error: "AMQP support is not compiled in this build".to_string(),
                        })
                        .await;
                    return;
                }
                #[cfg(feature = "rabbitmq")]
                ConnectionConfig::Rabbitmq(p) => Box::new(RabbitmqConnection::new(p, password)),
                #[cfg(not(feature = "rabbitmq"))]
                ConnectionConfig::Rabbitmq(_) => {
                    let _ = events
                        .send(AppEvent::ConnError {
                            id,
                            context: "connect".to_string(),
                            error: "RabbitMQ support is not compiled in this build".to_string(),
                        })
                        .await;
                    return;
                }
            };
            match spawn_connection(
                id,
                name,
                conn,
                events.clone(),
                &tracker,
                &cancel,
                recordings_dir,
            )
            .await
            {
                Ok(handle) => {
                    let _ = events.send(AppEvent::Connected { handle }).await;
                }
                Err(e) => {
                    let _ = events
                        .send(AppEvent::ConnError {
                            id,
                            context: "connect".to_string(),
                            // `{:#}` surfaces the full cause chain (e.g. a RabbitMQ
                            // connect's context plus the broker's reply detail).
                            error: format!("{e:#}"),
                        })
                        .await;
                }
            }
        });
    }

    fn submit_form(&mut self) {
        let Some(form) = self.form.as_ref() else {
            return;
        };

        let name = form.fields[0].trim().to_string();
        if name.is_empty() {
            self.form_error("name is required");
            return;
        }
        let host = {
            let h = form.fields[1].trim();
            if h.is_empty() {
                "127.0.0.1"
            } else {
                h
            }
        }
        .to_string();
        let port: u16 = match form.fields[2].trim().parse() {
            Ok(p) => p,
            Err(_) => return self.form_error("port must be a number 0-65535"),
        };
        let username = {
            let u = form.fields[4].trim();
            if u.is_empty() {
                None
            } else {
                Some(u.to_string())
            }
        };
        let (saved_spec, session_password) = classify_password(form.fields[5].trim());
        let tls = form.tls;

        let profile = match form.kind {
            BrokerKind::Redis => {
                let db: u32 = match form.fields[3].trim().parse() {
                    Ok(d) => d,
                    Err(_) => return self.form_error("db must be a number"),
                };
                ConnectionConfig::Redis(RedisProfile {
                    name,
                    host,
                    port,
                    db,
                    username,
                    password: saved_spec,
                    tls,
                })
            }
            BrokerKind::Amqp => ConnectionConfig::Amqp(AmqpProfile {
                name,
                host,
                port,
                username,
                password: saved_spec,
                tls,
            }),
            BrokerKind::Rabbitmq => {
                // The DB slot is relabelled "Vhost" for RabbitMQ; empty → default "/".
                let vhost = {
                    let v = form.fields[3].trim();
                    if v.is_empty() {
                        "/".to_string()
                    } else {
                        v.to_string()
                    }
                };
                ConnectionConfig::Rabbitmq(RabbitmqProfile {
                    name,
                    host,
                    port,
                    vhost,
                    username,
                    password: saved_spec,
                    tls,
                })
            }
        };

        // Persist (best effort) and keep the in-memory profile list in sync.
        self.config.connections.push(profile.clone());
        match config::save(&self.config_path, &self.config) {
            Ok(()) => self.profiles.push(profile.clone()),
            Err(e) => {
                self.config.connections.pop();
                self.set_status(format!("could not save config: {e}"), true);
            }
        }

        self.form = None;
        self.mode = InputMode::Normal;
        self.start_connect(profile, session_password);
    }

    fn form_error(&mut self, message: &str) {
        if let Some(form) = &mut self.form {
            form.error = Some(message.to_string());
        }
    }

    // -- broker requests -----------------------------------------------------

    /// Start a fresh keyspace scan for connection `id`, decoupled from
    /// navigation. `live` is forwarded to [`Connection::begin_scan`]: a
    /// foreground scan (initial load, DB/filter change, explicit refresh)
    /// reveals keys as they load and clears the previous result first; a
    /// background scan (the auto-refresh) keeps the current list visible and
    /// swaps the fresh set in atomically once complete. The scan then drives
    /// itself page by page to completion in [`Self::on_keys_page`].
    fn start_scan(&mut self, id: ConnId, live: bool) {
        let page_size = self.scan_count;
        if let Some(conn) = self.conn_by_id_mut(id) {
            let req = conn.begin_scan(live, page_size);
            conn.handle.send(ConnCommand::Browse(req));
        }
    }

    fn request_selected_value(&mut self, id: ConnId) {
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(entry) = conn.selected().cloned() {
                if conn.value_key.as_deref() != Some(entry.key.as_str()) {
                    conn.value = None;
                    conn.value_key = Some(entry.key.clone());
                    conn.value_scroll = 0;
                    conn.handle.send(ConnCommand::Inspect(InspectReq {
                        db: conn.db,
                        key: entry.key,
                        offset: 0,
                        limit: VALUE_LIMIT,
                    }));
                }
            }
        }
    }

    fn request_stats(&mut self, id: ConnId) {
        if let Some(conn) = self.conn_by_id(id) {
            conn.handle.send(ConnCommand::RefreshStats);
        }
    }

    fn apply_filter(&mut self) {
        let raw = self.filter.trim().to_string();
        let pattern = if raw.is_empty() {
            "*".to_string()
        } else if raw.contains(['*', '?', '[']) {
            raw
        } else {
            format!("*{raw}*")
        };
        if let Some(conn) = self.active_conn_mut() {
            conn.pattern = pattern;
        }
        if let Some(id) = self.active_id() {
            self.start_scan(id, true);
        }
    }

    // -- realtime / recordings -----------------------------------------------

    fn active_sub_mut(&mut self) -> Option<&mut Subscription> {
        self.active_conn_mut()
            .and_then(|c| c.active_subscription_mut())
    }

    fn open_subscribe_prompt(&mut self) {
        if self.active.is_none() {
            self.set_status("connect first, then subscribe".to_string(), true);
            return;
        }
        self.subscribe_buf.clear();
        self.mode = InputMode::Subscribe;
    }

    fn submit_subscribe(&mut self) {
        let raw = self.subscribe_buf.trim().to_string();
        self.mode = InputMode::Normal;
        if raw.is_empty() {
            return;
        }
        let default_db = self.active_conn().map(|c| c.db).unwrap_or(0);
        let spec = match SubSpec::parse(&raw, default_db) {
            Ok(spec) => spec,
            Err(e) => return self.set_status(format!("bad spec: {e}"), true),
        };
        // Reject a spec meant for a different broker up front, with a clear
        // message, rather than opening a tail tab that immediately fails.
        if let Some(kind) = self.active_conn().map(|c| c.caps.kind) {
            let want = spec.supported_kind();
            if want != kind {
                return self.set_status(
                    format!(
                        "`{raw}` is a {} spec, but this connection is {}",
                        want.label(),
                        kind.label()
                    ),
                    true,
                );
            }
        }
        self.start_subscribe(spec);
    }

    fn start_subscribe(&mut self, spec: SubSpec) {
        let Some(id) = self.active_id() else {
            self.set_status("no active connection".to_string(), true);
            return;
        };
        let capacity = self.tail_scrollback;
        let label = spec.label();

        // Focus an existing live tail for the same spec rather than duplicating.
        if let Some(conn) = self.conn_by_id_mut(id) {
            if let Some(pos) = conn
                .subs
                .iter()
                .position(|s| s.spec == spec && !matches!(s.state, SubState::Ended(_)))
            {
                conn.active_sub = Some(pos);
                self.screen = Screen::Realtime;
                self.set_status(format!("already tailing {label}"), false);
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
            conn.active_sub = Some(conn.subs.len() - 1);
        }
        self.screen = Screen::Realtime;
        self.set_status(format!("subscribing to {label}…"), false);
    }

    fn tail_selected_key(&mut self) {
        if self.screen != Screen::Browser {
            return;
        }
        let selected = self
            .active_conn()
            .and_then(|c| c.selected().map(|e| (e.key.clone(), e.vtype, c.db)));
        let Some((key, vtype, db)) = selected else {
            self.set_status("no key selected".to_string(), true);
            return;
        };
        if vtype != ValueType::Stream {
            self.set_status(
                format!(
                    "'{key}' is a {} — only streams can be tailed (press s for pub/sub)",
                    vtype.label()
                ),
                true,
            );
            return;
        }
        self.start_subscribe(SubSpec::Stream { key, db });
    }

    fn toggle_recording(&mut self) {
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

    fn stop_active_tail(&mut self) {
        let label = {
            let Some(conn) = self.active_conn_mut() else {
                return;
            };
            let Some(pos) = conn.active_sub else {
                return;
            };
            let sub = conn.subs.remove(pos);
            conn.handle
                .send(ConnCommand::StopSubscription { sub_id: sub.sub_id });
            conn.active_sub = if conn.subs.is_empty() {
                None
            } else {
                Some(pos.min(conn.subs.len() - 1))
            };
            sub.label
        };
        self.set_status(format!("stopped {label}"), false);
    }

    fn focus_tab(&mut self, delta: i32) {
        if let Some(conn) = self.active_conn_mut() {
            let len = conn.subs.len();
            if len == 0 {
                return;
            }
            let cur = conn.active_sub.unwrap_or(0) as i32;
            let next = (cur + delta).rem_euclid(len as i32) as usize;
            conn.active_sub = Some(next);
        }
    }

    fn scroll_tail(&mut self, delta: i32) {
        if let Some(sub) = self.active_sub_mut() {
            let max = sub.events.len().saturating_sub(1) as i32;
            // Up (delta < 0) scrolls back into history (larger offset from newest).
            let next = (sub.offset as i32 - delta).clamp(0, max) as usize;
            sub.offset = next;
            sub.follow = next == 0;
        }
    }

    /// Start a keyspace or MONITOR tail on the active connection. These are just
    /// more [`SubSpec`]s, so they reuse the whole subscribe/record/scrollback
    /// path; `start_subscribe` focuses an existing identical tail rather than
    /// duplicating it.
    fn start_special_tail(&mut self, spec: SubSpec) {
        if self.active.is_none() {
            self.set_status("connect first, then start the tail".to_string(), true);
            return;
        }
        self.start_subscribe(spec);
    }

    // -- command console -----------------------------------------------------

    fn active_console_mut(&mut self) -> Option<&mut Console> {
        self.active_conn_mut().map(|c| &mut c.console)
    }

    /// Begin typing a console command on a fresh prompt.
    fn enter_command_mode(&mut self) {
        if let Some(console) = self.active_console_mut() {
            console.input.clear();
            console.history_pos = None;
        }
        self.mode = InputMode::Command;
    }

    fn clear_console(&mut self) {
        if let Some(console) = self.active_console_mut() {
            console.entries.clear();
            console.scroll = 0;
        }
        self.set_status("console cleared".to_string(), false);
    }

    fn scroll_console(&mut self, delta: i32) {
        if let Some(console) = self.active_console_mut() {
            // Up (delta < 0) scrolls back through output (larger offset from the
            // bottom); the upper bound is clamped against total lines at render.
            let next = console.scroll as i32 - delta;
            console.scroll = next.clamp(0, u16::MAX as i32) as u16;
        }
    }

    fn handle_command_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Esc => self.mode = InputMode::Normal,
            KeyCode::Enter => self.submit_command(),
            KeyCode::Up => {
                if let Some(console) = self.active_console_mut() {
                    console.recall_prev();
                }
            }
            KeyCode::Down => {
                if let Some(console) = self.active_console_mut() {
                    console.recall_next();
                }
            }
            // ↑↓ recall history, so the output band scrolls with PageUp/PageDown.
            KeyCode::PageUp => self.scroll_console(-CONSOLE_SCROLL_STEP),
            KeyCode::PageDown => self.scroll_console(CONSOLE_SCROLL_STEP),
            // Ctrl-L clears the console (the standalone screen's `r`, now gone).
            KeyCode::Char('l') if ctrl => self.clear_console(),
            KeyCode::Char(c) => {
                if let Some(console) = self.active_console_mut() {
                    console.input.push(c);
                    console.history_pos = None;
                }
            }
            KeyCode::Backspace => {
                if let Some(console) = self.active_console_mut() {
                    console.input.pop();
                }
            }
            _ => {}
        }
    }

    /// Submit the typed command for read-only execution. Stays in command mode
    /// (console-style) so commands can be issued back to back; `Esc` leaves.
    fn submit_command(&mut self) {
        let Some(id) = self.active_id() else {
            self.mode = InputMode::Normal;
            return;
        };
        let command = self
            .active_console_mut()
            .map(|c| c.input.trim().to_string())
            .unwrap_or_default();
        if command.is_empty() {
            return;
        }
        if let Some(conn) = self.conn_by_id_mut(id) {
            conn.console.remember(&command);
            conn.console.input.clear();
            conn.console.pending = Some(command.clone());
            conn.console.scroll = 0;
            conn.handle.send(ConnCommand::Exec { command });
        }
    }

    // -- command palette -----------------------------------------------------

    fn open_palette(&mut self) {
        self.palette = Some(PaletteState::default());
        self.mode = InputMode::Palette;
    }

    fn close_palette(&mut self) {
        self.palette = None;
        self.mode = InputMode::Normal;
    }

    fn handle_palette_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.close_palette(),
            KeyCode::Enter => self.submit_palette(),
            KeyCode::Up => self.palette_nav(-1),
            KeyCode::Down => self.palette_nav(1),
            KeyCode::Char(c) => {
                if let Some(p) = &mut self.palette {
                    p.query.push(c);
                    p.selected = 0;
                }
            }
            KeyCode::Backspace => {
                if let Some(p) = &mut self.palette {
                    p.query.pop();
                    p.selected = 0;
                }
            }
            _ => {}
        }
    }

    fn palette_nav(&mut self, delta: i32) {
        if let Some(p) = &mut self.palette {
            let len = action::palette_matches(&p.query).len();
            if len == 0 {
                p.selected = 0;
                return;
            }
            p.selected = (p.selected as i32 + delta).rem_euclid(len as i32) as usize;
        }
    }

    fn submit_palette(&mut self) {
        let action = self.palette.as_ref().and_then(|p| {
            action::palette_matches(&p.query)
                .get(p.selected)
                .map(|item| item.action)
        });
        self.close_palette();
        if let Some(action) = action {
            self.apply(action);
        }
    }

    fn scan_recordings(&mut self) {
        let dir = self.recordings_dir.clone();
        let mut files = Vec::new();
        if let Ok(read) = std::fs::read_dir(&dir) {
            for entry in read.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                let meta = entry.metadata().ok();
                let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
                let modified = meta
                    .as_ref()
                    .and_then(|m| m.modified().ok())
                    .map(OffsetDateTime::from);
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?")
                    .to_string();
                files.push(RecordingFile {
                    name,
                    size,
                    modified,
                });
            }
        }
        // Newest first.
        files.sort_by_key(|f| std::cmp::Reverse(f.modified));
        self.recordings = files;
        let sel = match self.recordings.len() {
            0 => None,
            len => Some(self.recordings_state.selected().unwrap_or(0).min(len - 1)),
        };
        self.recordings_state.select(sel);
    }

    // -- helpers -------------------------------------------------------------

    fn active_id(&self) -> Option<ConnId> {
        self.active_conn().map(|c| c.id)
    }

    fn conn_by_id(&self, id: ConnId) -> Option<&Connection> {
        self.connections.iter().find(|c| c.id == id)
    }

    fn conn_by_id_mut(&mut self, id: ConnId) -> Option<&mut Connection> {
        self.connections.iter_mut().find(|c| c.id == id)
    }

    fn set_status(&mut self, message: String, is_error: bool) {
        if is_error {
            tracing::warn!(%message, "status");
        } else {
            tracing::info!(%message, "status");
        }
        self.status = Some(Status { message, is_error });
    }
}

/// Resolve a secret spec off the render thread (keyring access can block).
async fn resolve_secret(
    spec: config::SecretSpec,
    account: String,
) -> anyhow::Result<Option<String>> {
    tokio::task::spawn_blocking(move || config::resolve_secret(&spec, &account)).await?
}

/// The screen to show a freshly-focused connection: the key browser when the
/// broker has one (Redis), else the realtime tails (AMQP).
fn initial_screen(caps: &Capabilities) -> Screen {
    if caps.can_browse {
        Screen::Browser
    } else {
        Screen::Realtime
    }
}

/// Convert a configured key-browser refresh interval (milliseconds) into a
/// count of UI ticks. `0` stays `0` (auto-refresh disabled); any other value
/// rounds up to at least one tick so a tiny interval still fires.
fn refresh_ticks(interval_ms: u64) -> u32 {
    if interval_ms == 0 {
        return 0;
    }
    interval_ms
        .div_ceil(TICK_PERIOD_MS)
        .max(1)
        .try_into()
        .unwrap_or(u32::MAX)
}

/// Decide whether the form's password field is a *spec* (persisted) or a
/// *literal* (used for this session only, never written to the config file).
fn classify_password(pw: &str) -> (Option<String>, Option<String>) {
    if pw.is_empty() {
        return (None, None);
    }
    let is_spec =
        pw == "keyring" || pw == "prompt" || pw.starts_with("env:") || pw.starts_with("keyring:");
    if is_spec {
        (Some(pw.to_string()), None)
    } else {
        // Literal: persist a `prompt` spec so nothing is stored in plaintext.
        (Some("prompt".to_string()), Some(pw.to_string()))
    }
}

fn move_selection(current: Option<usize>, len: usize, delta: i32) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let cur = current.unwrap_or(0) as i32;
    Some((cur + delta).clamp(0, len as i32 - 1) as usize)
}

/// Direction arrow + word for a status message describing the sort order.
fn sort_arrow(desc: bool) -> &'static str {
    if desc {
        "↓ desc"
    } else {
        "↑ asc"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::actor::mock;
    use crate::broker::{EntryMeta, Payload, Ttl};
    use crossterm::event::{KeyEventState, KeyModifiers};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::mpsc::{self, Receiver};

    // -- harness -------------------------------------------------------------

    fn build_app(
        config: Config,
        config_path: PathBuf,
        connect: Option<String>,
    ) -> (App, Receiver<AppEvent>) {
        let (tx, rx) = mpsc::channel::<AppEvent>(64);
        let app = App::new(
            config,
            config_path,
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            connect,
        );
        (app, rx)
    }

    fn test_app() -> (App, Receiver<AppEvent>) {
        build_app(
            Config::default(),
            PathBuf::from("/nonexistent/keyhole/config.toml"),
            None,
        )
    }

    fn profile(name: &str) -> RedisProfile {
        RedisProfile {
            name: name.into(),
            host: "127.0.0.1".into(),
            port: 6399,
            db: 0,
            username: None,
            password: None,
            tls: false,
        }
    }

    fn config_with(names: &[&str]) -> Config {
        Config {
            connections: names
                .iter()
                .map(|n| ConnectionConfig::Redis(profile(n)))
                .collect(),
            ..Default::default()
        }
    }

    fn unique_config_path() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("keyhole-app-{}-{n}.toml", std::process::id()))
    }

    /// Attach a live mock-backed connection and return its id.
    async fn connect(app: &mut App, id: u32, name: &str, databases: u32) -> ConnId {
        let handle = mock::handle(id, name, databases).await;
        app.handle_event(AppEvent::Connected { handle });
        ConnId(id)
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ch(c: char) -> KeyEvent {
        key(KeyCode::Char(c))
    }

    fn ctrl_ch(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    fn broker_event(body: &str) -> BrokerEvent {
        BrokerEvent {
            ts: OffsetDateTime::UNIX_EPOCH,
            source: "c".into(),
            payload: Payload::Utf8(body.into()),
            meta: Vec::new(),
        }
    }

    fn stream_entry(name: &str, vtype: ValueType) -> EntryMeta {
        EntryMeta {
            key: name.into(),
            vtype,
            ttl: Ttl::NoExpire,
            size: None,
        }
    }

    // -- construction --------------------------------------------------------

    #[test]
    fn new_selects_first_profile_when_present() {
        let (app, _rx) = build_app(config_with(&["a", "b"]), unique_config_path(), None);
        assert_eq!(app.profiles.len(), 2);
        assert_eq!(app.profile_state.selected(), Some(0));
        assert_eq!(app.screen, Screen::Connections);
        assert_eq!(app.mode, InputMode::Normal);
        assert!(app.running);
    }

    #[test]
    fn new_selects_nothing_without_profiles() {
        let (app, _rx) = test_app();
        assert!(app.profiles.is_empty());
        assert_eq!(app.profile_state.selected(), None);
    }

    // -- on_start ------------------------------------------------------------

    #[test]
    fn on_start_unknown_profile_sets_error() {
        let (mut app, _rx) = build_app(
            config_with(&["known"]),
            unique_config_path(),
            Some("missing".into()),
        );
        app.on_start();
        let status = app.status.as_ref().expect("status set");
        assert!(status.is_error);
        assert!(status.message.contains("missing"));
    }

    #[tokio::test]
    async fn on_start_known_profile_starts_connecting() {
        let (mut app, _rx) = build_app(
            config_with(&["known"]),
            unique_config_path(),
            Some("known".into()),
        );
        app.on_start();
        let status = app.status.as_ref().expect("status set");
        assert!(!status.is_error);
        assert!(status.message.contains("Connecting to known"));
        assert_eq!(
            app.next_id, 2,
            "an id was allocated for the connect attempt"
        );
    }

    // -- connection lifecycle ------------------------------------------------

    #[tokio::test]
    async fn on_connected_activates_and_opens_browser() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        assert_eq!(app.connections.len(), 1);
        assert_eq!(app.active, Some(0));
        assert_eq!(app.screen, Screen::Browser);
        assert!(app.is_connected("prod"));
        assert!(!app.is_connected("other"));
        let status = app.status.as_ref().unwrap();
        assert!(!status.is_error);
        assert!(status.message.contains("Connected to prod"));
        assert_eq!(app.active_conn().unwrap().label(), "prod (db0)");
    }

    #[tokio::test]
    async fn page_keys_scroll_value_pane_in_browser() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.screen = Screen::Browser;
        assert_eq!(app.active_conn().unwrap().value_scroll, 0);
        // PageDown scrolls the value pane down; repeated PageUp clamps at the top.
        app.apply(Action::PageDown);
        assert!(
            app.active_conn().unwrap().value_scroll > 0,
            "PageDown scrolls the Browser value pane"
        );
        app.apply(Action::PageUp);
        app.apply(Action::PageUp);
        assert_eq!(
            app.active_conn().unwrap().value_scroll,
            0,
            "PageUp clamps at the top"
        );
    }

    #[test]
    fn page_keys_navigate_list_outside_browser() {
        // On non-Browser screens the page keys still page the focused list.
        let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
        assert_eq!(app.profile_state.selected(), Some(0));
        app.apply(Action::PageDown);
        assert_eq!(
            app.profile_state.selected(),
            Some(2),
            "PageDown pages the connections list (clamped to the last profile)"
        );
    }

    #[tokio::test]
    async fn on_disconnected_removes_and_resets_to_connections() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.handle_event(AppEvent::Disconnected {
            id,
            reason: "bye".into(),
        });
        assert!(app.connections.is_empty());
        assert_eq!(app.active, None);
        assert_eq!(app.screen, Screen::Connections);
        let status = app.status.as_ref().unwrap();
        assert!(status.is_error);
        assert!(status.message.contains("disconnected: bye"));
    }

    #[tokio::test]
    async fn on_disconnected_unknown_id_is_noop() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.handle_event(AppEvent::Disconnected {
            id: ConnId(999),
            reason: "x".into(),
        });
        assert_eq!(app.connections.len(), 1);
    }

    #[tokio::test]
    async fn on_disconnected_keeps_others_when_multiple() {
        let (mut app, _rx) = test_app();
        let first = connect(&mut app, 1, "a", 16).await;
        connect(&mut app, 2, "b", 16).await;
        app.handle_event(AppEvent::Disconnected {
            id: first,
            reason: "x".into(),
        });
        assert_eq!(app.connections.len(), 1);
        assert_eq!(app.connections[0].name, "b");
        assert_eq!(app.active, Some(0));
        assert_ne!(app.screen, Screen::Connections);
    }

    #[tokio::test]
    async fn connect_selected_focuses_existing_connection() {
        let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
        connect(&mut app, 1, "prod", 16).await;
        app.screen = Screen::Connections;
        app.profile_state.select(Some(0));
        app.apply(Action::Enter);
        assert_eq!(app.connections.len(), 1, "no duplicate connection opened");
        assert_eq!(app.active, Some(0));
        assert_eq!(app.screen, Screen::Browser);
    }

    // -- browse / value / stats ----------------------------------------------

    #[tokio::test]
    async fn keys_page_extends_and_tracks_cursor() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        // The initial (foreground) scan was kicked off on connect; its pages
        // carry the current scan epoch.
        let epoch = app.active_conn().unwrap().scan_epoch;
        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0,
                entries: vec![stream_entry("k1", ValueType::String)],
                next_cursor: 5,
                epoch,
            },
        });
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.keys.len(), 1);
        assert_eq!(conn.next_cursor, 5);
        assert!(!conn.complete);
        assert!(conn.scanning, "scan still in progress mid-page");

        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0,
                entries: vec![stream_entry("k2", ValueType::List)],
                next_cursor: 0,
                epoch,
            },
        });
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.keys.len(), 2, "second page appended");
        assert!(conn.complete, "cursor 0 marks the scan complete");
        assert!(!conn.scanning, "scan finished");
    }

    #[tokio::test]
    async fn keys_page_from_stale_db_is_ignored() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        let epoch = app.active_conn().unwrap().scan_epoch;
        app.connections[0].db = 1;
        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0, // stale: connection has since switched to db1
                entries: vec![stream_entry("k", ValueType::String)],
                next_cursor: 0,
                epoch,
            },
        });
        assert!(app.active_conn().unwrap().keys.is_empty());
    }

    #[tokio::test]
    async fn keys_page_from_superseded_scan_is_ignored() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        // A page stamped with an older epoch (e.g. from a scan abandoned when
        // the user changed the filter) must not contaminate the current scan.
        let stale_epoch = app.active_conn().unwrap().scan_epoch.wrapping_sub(1);
        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0,
                entries: vec![stream_entry("ghost", ValueType::String)],
                next_cursor: 0,
                epoch: stale_epoch,
            },
        });
        assert!(
            app.active_conn().unwrap().keys.is_empty(),
            "page from a superseded scan is dropped"
        );
    }

    /// Completes the connection's initial (foreground) scan with `entries`,
    /// leaving the browser idle and showing those keys.
    fn finish_initial_scan(app: &mut App, id: ConnId, entries: Vec<EntryMeta>) {
        let epoch = app.active_conn().unwrap().scan_epoch;
        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0,
                entries,
                next_cursor: 0,
                epoch,
            },
        });
        assert!(app.active_conn().unwrap().complete);
        assert!(!app.active_conn().unwrap().scanning);
    }

    #[test]
    fn refresh_ticks_rounds_up_and_disables_on_zero() {
        assert_eq!(refresh_ticks(0), 0, "zero disables auto-refresh");
        assert_eq!(refresh_ticks(250), 1, "one tick");
        assert_eq!(refresh_ticks(5000), 20, "default 5s at 250ms ticks");
        assert_eq!(
            refresh_ticks(100),
            1,
            "a sub-tick interval still fires once"
        );
        assert_eq!(refresh_ticks(600), 3, "rounds up to whole ticks");
    }

    #[tokio::test]
    async fn navigation_does_not_trigger_a_scan() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        finish_initial_scan(
            &mut app,
            id,
            vec![
                stream_entry("a", ValueType::String),
                stream_entry("b", ValueType::String),
                stream_entry("c", ValueType::String),
            ],
        );
        let epoch_after_load = app.active_conn().unwrap().scan_epoch;

        // Spamming up/down must only move the highlight — the whole point of the
        // change: the key set never re-fetches as a side effect of navigating.
        app.screen = Screen::Browser;
        for _ in 0..20 {
            app.apply(Action::Down);
            app.apply(Action::Up);
        }
        let conn = app.active_conn().unwrap();
        assert_eq!(
            conn.scan_epoch, epoch_after_load,
            "navigation must not start a scan"
        );
        assert!(!conn.scanning, "navigation must not leave a scan running");
    }

    #[tokio::test]
    async fn tick_auto_refreshes_keys_independently_of_navigation() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        finish_initial_scan(&mut app, id, vec![stream_entry("a", ValueType::String)]);
        let epoch_before = app.active_conn().unwrap().scan_epoch;

        // With auto-refresh due every tick and the browser on screen, one tick
        // starts a fresh background scan on its own — no key was pressed.
        app.screen = Screen::Browser;
        app.browse_refresh_ticks = 1;
        app.on_tick();
        let conn = app.active_conn().unwrap();
        assert_eq!(
            conn.scan_epoch,
            epoch_before + 1,
            "the tick started a new scan"
        );
        assert!(conn.scanning, "background scan in progress");
        assert!(
            !conn.scan_live,
            "auto-refresh stages into scan_buf rather than clearing the list"
        );
    }

    #[tokio::test]
    async fn auto_refresh_is_disabled_when_interval_is_zero() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        finish_initial_scan(&mut app, id, vec![]);
        let epoch_before = app.active_conn().unwrap().scan_epoch;

        app.screen = Screen::Browser;
        app.browse_refresh_ticks = 0; // disabled
        for _ in 0..50 {
            app.on_tick();
        }
        assert_eq!(
            app.active_conn().unwrap().scan_epoch,
            epoch_before,
            "no scans when auto-refresh is disabled"
        );
    }

    #[tokio::test]
    async fn auto_refresh_does_not_run_off_the_browser_screen() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        finish_initial_scan(&mut app, id, vec![]);
        let epoch_before = app.active_conn().unwrap().scan_epoch;

        // Tailing on the Realtime screen: re-scanning the keyspace would be
        // pointless work, so the auto-refresh holds off until the browser is up.
        app.screen = Screen::Realtime;
        app.browse_refresh_ticks = 1;
        for _ in 0..10 {
            app.on_tick();
        }
        assert_eq!(
            app.active_conn().unwrap().scan_epoch,
            epoch_before,
            "no background scan while off the Browser screen"
        );
    }

    #[tokio::test]
    async fn background_refresh_swaps_in_atomically_without_flicker() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        finish_initial_scan(
            &mut app,
            id,
            vec![
                stream_entry("old1", ValueType::String),
                stream_entry("old2", ValueType::String),
            ],
        );

        // A background refresh begins, exactly as the tick would start it.
        app.screen = Screen::Browser;
        app.browse_refresh_ticks = 1;
        app.on_tick();
        let refresh_epoch = app.active_conn().unwrap().scan_epoch;
        assert!(app.active_conn().unwrap().scanning);

        // The first page of the refresh arrives, but the scan is not finished:
        // the visible list must stay exactly as it was — no empty frame, no
        // half-populated list flashing on screen.
        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0,
                entries: vec![stream_entry("new1", ValueType::String)],
                next_cursor: 42,
                epoch: refresh_epoch,
            },
        });
        {
            let visible: Vec<&str> = app
                .active_conn()
                .unwrap()
                .keys
                .iter()
                .map(|k| k.key.as_str())
                .collect();
            assert_eq!(
                visible,
                ["old1", "old2"],
                "old keys stay visible mid-refresh"
            );
        }

        // The final page completes the scan: only now does the fresh set swap in.
        app.handle_event(AppEvent::KeysPage {
            id,
            page: BrowsePage {
                db: 0,
                entries: vec![stream_entry("new2", ValueType::String)],
                next_cursor: 0,
                epoch: refresh_epoch,
            },
        });
        let conn = app.active_conn().unwrap();
        let visible: Vec<&str> = conn.keys.iter().map(|k| k.key.as_str()).collect();
        assert_eq!(
            visible,
            ["new1", "new2"],
            "fresh set swapped in atomically on completion"
        );
        assert!(conn.complete);
    }

    #[tokio::test]
    async fn changing_filter_clears_list_and_rescans() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        finish_initial_scan(
            &mut app,
            id,
            vec![stream_entry("user:1", ValueType::String)],
        );
        let epoch_before = app.active_conn().unwrap().scan_epoch;

        // A filter change is a foreground rescan: the stale result is cleared at
        // once (the keys no longer match) and a new scan generation begins.
        app.filter = "session".into();
        app.apply_filter();
        let conn = app.active_conn().unwrap();
        assert!(
            conn.keys.is_empty(),
            "foreground rescan clears the previous result immediately"
        );
        assert!(conn.scanning, "a fresh scan is underway");
        assert!(conn.scan_live, "filter change is a live (foreground) scan");
        assert_eq!(conn.pattern, "*session*");
        assert!(conn.scan_epoch > epoch_before, "new scan generation");
    }

    #[tokio::test]
    async fn value_loaded_only_applies_to_current_key() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.connections[0].value_key = Some("k".into());

        app.handle_event(AppEvent::ValueLoaded {
            id,
            key: "other".into(),
            value: ValueView::Missing,
        });
        assert!(
            app.active_conn().unwrap().value.is_none(),
            "mismatch ignored"
        );

        app.handle_event(AppEvent::ValueLoaded {
            id,
            key: "k".into(),
            value: ValueView::Missing,
        });
        assert!(app.active_conn().unwrap().value.is_some(), "match applied");
    }

    #[tokio::test]
    async fn stats_updated_sets_stats() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.handle_event(AppEvent::StatsUpdated {
            id,
            stats: ServerStats {
                redis_version: Some("7.4".into()),
                ..Default::default()
            },
        });
        assert_eq!(
            app.active_conn()
                .unwrap()
                .stats
                .as_ref()
                .unwrap()
                .redis_version
                .as_deref(),
            Some("7.4")
        );
    }

    #[test]
    fn conn_error_sets_error_status() {
        let (mut app, _rx) = test_app();
        app.handle_event(AppEvent::ConnError {
            id: ConnId(3),
            context: "browse".into(),
            error: "nope".into(),
        });
        let status = app.status.as_ref().unwrap();
        assert!(status.is_error);
        assert!(status.message.contains("[3] browse: nope"));
    }

    // -- screen navigation & help --------------------------------------------

    #[test]
    fn ctrl_c_quits_from_anywhere() {
        // Ctrl-c is the hard quit (the old `q`); it exits from any screen, even
        // a deep one where Esc would only step back.
        let (mut app, _rx) = test_app();
        app.screen = Screen::Browser;
        app.handle_key(ctrl_ch('c'));
        assert!(!app.running, "Ctrl-c is a hard quit from any screen");
    }

    #[test]
    fn esc_steps_back_then_quits_from_connections() {
        // Back (Esc) is global and walks toward Connections, then quits from
        // there: Browser → Connections → close app. Other data screens fall
        // back to Connections the same way.
        let (mut app, _rx) = test_app();

        app.screen = Screen::Browser;
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(
            app.screen,
            Screen::Connections,
            "Browser backs out to Connections"
        );
        assert!(app.running);

        app.screen = Screen::Realtime;
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(
            app.screen,
            Screen::Connections,
            "Realtime backs out to Connections"
        );
        assert!(app.running);

        app.screen = Screen::Recordings;
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(
            app.screen,
            Screen::Connections,
            "Recordings backs out to Connections"
        );
        assert!(app.running);

        // From Connections (home) the first Esc arms a quit confirmation; the
        // app only closes on a second consecutive Esc.
        app.handle_key(key(KeyCode::Esc));
        assert!(app.running, "first Esc on Connections arms, does not quit");
        app.handle_key(key(KeyCode::Esc));
        assert!(!app.running, "second consecutive Esc on Connections quits");
    }

    #[test]
    fn esc_quit_confirmation_resets_on_other_input() {
        // Arming is only consumed by an immediately following Esc: any other
        // input disarms, so a stray Esc can't combine with a later one to quit.
        let (mut app, _rx) = test_app();
        app.handle_key(key(KeyCode::Esc)); // arm
        assert!(app.running);
        app.handle_key(ch('j')); // move selection — disarms
        app.handle_key(key(KeyCode::Esc)); // re-arms rather than quitting
        assert!(
            app.running,
            "intervening input disarms the quit confirmation"
        );
        app.handle_key(key(KeyCode::Esc)); // second consecutive Esc now quits
        assert!(!app.running);
    }

    #[test]
    fn esc_dismisses_help_before_navigating() {
        // The help overlay is the top of the back stack: the first Esc closes
        // it without changing screens.
        let (mut app, _rx) = test_app();
        app.screen = Screen::Browser;
        app.show_help = true;
        app.handle_key(key(KeyCode::Esc));
        assert!(!app.show_help, "first back closes the help overlay");
        assert_eq!(
            app.screen,
            Screen::Browser,
            "help close doesn't change screen"
        );
        // With help closed, the next back steps out of the Browser as usual.
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(app.screen, Screen::Connections);
    }

    #[test]
    fn help_toggles_and_dismisses() {
        let (mut app, _rx) = test_app();
        app.apply(Action::ToggleHelp);
        assert!(app.show_help);
        app.apply(Action::ToggleHelp);
        assert!(!app.show_help);
        // Back (Esc) closes the overlay when it's open, before any navigation.
        app.show_help = true;
        app.apply(Action::Back);
        assert!(!app.show_help);
    }

    #[test]
    fn goto_data_screens_requires_active_connection() {
        let (mut app, _rx) = test_app();
        for action in [Action::GotoBrowser, Action::GotoRealtime] {
            app.apply(action);
            assert_eq!(
                app.screen,
                Screen::Connections,
                "{action:?} needs a connection"
            );
        }
        app.apply(Action::GotoRecordings);
        assert_eq!(
            app.screen,
            Screen::Recordings,
            "recordings is always reachable"
        );
    }

    #[tokio::test]
    async fn goto_screens_switch_with_active_connection() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.apply(Action::GotoRealtime);
        assert_eq!(app.screen, Screen::Realtime);
        app.apply(Action::GotoConnections);
        assert_eq!(app.screen, Screen::Connections);
        app.apply(Action::GotoBrowser);
        assert_eq!(app.screen, Screen::Browser);
    }

    // -- navigation ----------------------------------------------------------

    #[test]
    fn profile_navigation_moves_and_clamps() {
        let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
        app.apply(Action::Down);
        assert_eq!(app.profile_state.selected(), Some(1));
        app.apply(Action::Bottom);
        assert_eq!(app.profile_state.selected(), Some(2));
        app.apply(Action::PageDown);
        assert_eq!(app.profile_state.selected(), Some(2), "clamped at the end");
        app.apply(Action::Top);
        assert_eq!(app.profile_state.selected(), Some(0));
        app.apply(Action::PageUp);
        assert_eq!(
            app.profile_state.selected(),
            Some(0),
            "clamped at the start"
        );
    }

    #[tokio::test]
    async fn browser_navigation_updates_selected_value() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.connections[0].keys = vec![
            stream_entry("k0", ValueType::String),
            stream_entry("k1", ValueType::String),
            stream_entry("k2", ValueType::String),
        ];
        app.connections[0].rebuild_view();
        // Keys are always grouped, so row 0 is the "(no prefix)" header and the
        // keys follow: k0 at row 1, k1 at row 2. From k0, Down lands on k1 and
        // inspects it.
        app.connections[0].table.select(Some(1));
        app.apply(Action::Down);
        assert_eq!(app.connections[0].table.selected(), Some(2));
        assert_eq!(app.connections[0].value_key.as_deref(), Some("k1"));
    }

    /// The key names of the Entry rows in a connection's current view order.
    fn view_keys(conn: &Connection) -> Vec<String> {
        conn.view
            .iter()
            .filter_map(|r| match r {
                ViewRow::Entry(i) => Some(conn.keys[*i].key.clone()),
                ViewRow::Group { .. } => None,
            })
            .collect()
    }

    async fn browser_with_keys(keys: Vec<EntryMeta>) -> (App, Receiver<AppEvent>) {
        let (mut app, rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.screen = Screen::Browser;
        app.connections[0].keys = keys;
        app.connections[0].rebuild_view();
        (app, rx)
    }

    #[tokio::test]
    async fn browser_cycle_sort_changes_order() {
        // Default name-asc order is a, b. Sorting by type puts the string (b)
        // ahead of the hash (a), so the column choice — not the name — drives it.
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("a", ValueType::Hash),
            stream_entry("b", ValueType::String),
        ])
        .await;
        assert_eq!(view_keys(&app.connections[0]), ["a", "b"]);
        app.apply(Action::CycleSort);
        assert_eq!(app.connections[0].sort.label(), "type");
        assert_eq!(view_keys(&app.connections[0]), ["b", "a"]);
    }

    #[tokio::test]
    async fn browser_toggle_sort_direction_reverses_order() {
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("a", ValueType::String),
            stream_entry("b", ValueType::String),
        ])
        .await;
        app.apply(Action::ToggleSortDir);
        assert!(app.connections[0].sort_desc);
        assert_eq!(view_keys(&app.connections[0]), ["b", "a"]);
    }

    #[tokio::test]
    async fn browser_view_is_always_grouped_with_headers() {
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("cache:x", ValueType::String),
            stream_entry("user:2", ValueType::String),
        ])
        .await;
        // Keys are always grouped by prefix — there is no ungrouped mode — so two
        // group headers (cache, user) are present from the start.
        let groups = app.connections[0]
            .view
            .iter()
            .filter(|r| matches!(r, ViewRow::Group { .. }))
            .count();
        assert_eq!(groups, 2);
        // Rows: [cache hdr, cache:x, user hdr, user:1, user:2]. Select user:1.
        app.connections[0].table.select(Some(3));
        assert_eq!(app.connections[0].selected().unwrap().key, "user:1");
        // A re-sort keeps the highlight on the same key (across the rebuild).
        app.apply(Action::CycleSort);
        assert_eq!(app.connections[0].selected().unwrap().key, "user:1");
    }

    #[tokio::test]
    async fn browser_enter_collapses_and_expands_selected_group() {
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("user:2", ValueType::String),
        ])
        .await;
        // Always grouped: the first row is the "user" group header.
        app.connections[0].table.select(Some(0));
        assert_eq!(
            app.connections[0].cursor_group_prefix().as_deref(),
            Some("user")
        );

        app.apply(Action::Enter); // collapse
        assert!(app.connections[0].collapsed.contains("user"));
        assert!(view_keys(&app.connections[0]).is_empty(), "keys hidden");

        app.apply(Action::Enter); // expand
        assert!(!app.connections[0].collapsed.contains("user"));
        assert_eq!(view_keys(&app.connections[0]), ["user:1", "user:2"]);
    }

    #[tokio::test]
    async fn browser_collapse_works_from_a_key_inside_the_group() {
        // Folding should act on the group the cursor is in, even when a key —
        // not the group header — is highlighted. The selection then lands on
        // the (now folded) header so the cursor stays on a visible row.
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("user:2", ValueType::String),
            stream_entry("cache:x", ValueType::String),
        ])
        .await;
        // Always grouped — rows: [cache hdr, cache:x, user hdr, user:1, user:2].
        // Select user:2.
        app.connections[0].table.select(Some(4));
        assert_eq!(app.connections[0].selected().unwrap().key, "user:2");

        app.apply(Action::ToggleCollapse); // Space, from inside the group
        assert!(
            app.connections[0].collapsed.contains("user"),
            "the cursor's group folds even from a key row"
        );
        // The "user" keys are hidden; "cache:x" remains.
        assert_eq!(view_keys(&app.connections[0]), ["cache:x"]);
        // The highlight moved to the folded "user" header (not a stale index).
        assert_eq!(
            app.connections[0].cursor_group_prefix().as_deref(),
            Some("user")
        );

        app.apply(Action::Enter); // expand again from the header
        assert!(!app.connections[0].collapsed.contains("user"));
        assert_eq!(
            view_keys(&app.connections[0]),
            ["cache:x", "user:1", "user:2"]
        );
    }

    #[tokio::test]
    async fn browser_selecting_group_header_requests_no_value() {
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("user:2", ValueType::String),
        ])
        .await;
        // Always grouped: row 0 is the "user" group header.
        app.connections[0].table.select(Some(0)); // the group header
        let id = app.active_id().unwrap();
        app.request_selected_value(id);
        // A group row is not a key, so nothing is inspected.
        assert!(app.connections[0].selected().is_none());
        assert!(app.connections[0].value_key.is_none());
    }

    #[tokio::test]
    async fn browser_toggle_all_groups_folds_and_unfolds() {
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("cache:x", ValueType::String),
        ])
        .await;
        // Always grouped, so groups exist from the start.
        app.apply(Action::ToggleAllGroups); // collapse all
        assert!(view_keys(&app.connections[0]).is_empty());
        app.apply(Action::ToggleAllGroups); // expand all
        assert_eq!(view_keys(&app.connections[0]).len(), 2);
    }

    #[tokio::test]
    async fn browser_resort_keeps_highlight_on_same_key() {
        let (mut app, _rx) = browser_with_keys(vec![
            stream_entry("a", ValueType::Hash),
            stream_entry("b", ValueType::String),
        ])
        .await;
        // Always grouped: "a" and "b" share the empty prefix, so row 0 is the
        // "(no prefix)" header and "a" (name-asc) is row 1.
        app.connections[0].table.select(Some(1));
        assert_eq!(app.connections[0].selected().unwrap().key, "a");
        // Sorting by type reorders the group's keys to [b, a]; the highlight
        // follows "a", which is now the second key (row 2, after the header).
        app.apply(Action::CycleSort);
        assert_eq!(view_keys(&app.connections[0]), ["b", "a"]);
        assert_eq!(app.connections[0].selected().unwrap().key, "a");
        assert_eq!(app.connections[0].table.selected(), Some(2));
    }

    // -- change_db -----------------------------------------------------------

    #[tokio::test]
    async fn change_db_clamps_to_capabilities() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 4).await; // databases 0..=3
        assert_eq!(app.screen, Screen::Browser);
        app.change_db(1);
        assert_eq!(app.connections[0].db, 1);
        app.change_db(100);
        assert_eq!(app.connections[0].db, 3, "clamped to the last database");
        app.change_db(-100);
        assert_eq!(app.connections[0].db, 0, "clamped to the first database");
    }

    #[tokio::test]
    async fn change_db_only_acts_in_browser() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 4).await;
        app.screen = Screen::Realtime;
        app.change_db(1);
        assert_eq!(app.connections[0].db, 0, "no DB change outside the Browser");
    }

    // -- filter --------------------------------------------------------------

    #[tokio::test]
    async fn apply_filter_builds_scan_patterns() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;

        app.filter = "foo".into();
        app.apply_filter();
        assert_eq!(app.connections[0].pattern, "*foo*", "plain text is wrapped");

        app.filter = "a*b".into();
        app.apply_filter();
        assert_eq!(app.connections[0].pattern, "a*b", "globs pass through");

        app.filter = "   ".into();
        app.apply_filter();
        assert_eq!(app.connections[0].pattern, "*", "blank means match-all");
    }

    #[test]
    fn filter_mode_edits_buffer() {
        let (mut app, _rx) = test_app();
        app.mode = InputMode::Filter;
        app.handle_key(ch('a'));
        app.handle_key(ch('b'));
        assert_eq!(app.filter, "ab");
        app.handle_key(key(KeyCode::Backspace));
        assert_eq!(app.filter, "a");
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(app.mode, InputMode::Normal);
    }

    #[test]
    fn start_filter_requires_browser_with_connection() {
        let (mut app, _rx) = test_app();
        app.apply(Action::StartFilter);
        assert_eq!(
            app.mode,
            InputMode::Normal,
            "no filter without a connection"
        );
    }

    #[tokio::test]
    async fn start_filter_enters_filter_mode() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.filter = "stale".into();
        app.apply(Action::StartFilter);
        assert_eq!(app.mode, InputMode::Filter);
        assert!(app.filter.is_empty(), "filter buffer is reset on entry");
    }

    // -- subscribe -----------------------------------------------------------

    #[test]
    fn subscribe_without_connection_errors() {
        let (mut app, _rx) = test_app();
        app.apply(Action::Subscribe);
        assert_eq!(app.mode, InputMode::Normal);
        let status = app.status.as_ref().unwrap();
        assert!(status.is_error);
        assert!(status.message.contains("connect first"));
    }

    #[test]
    fn submit_subscribe_rejects_bad_spec() {
        let (mut app, _rx) = test_app();
        app.mode = InputMode::Subscribe;
        app.subscribe_buf = "garbage".into();
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.mode, InputMode::Normal);
        let status = app.status.as_ref().unwrap();
        assert!(status.is_error);
        assert!(status.message.contains("bad spec"));
    }

    #[test]
    fn submit_subscribe_empty_is_noop() {
        let (mut app, _rx) = test_app();
        app.mode = InputMode::Subscribe;
        app.subscribe_buf = "   ".into();
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.mode, InputMode::Normal);
        assert!(app.status.is_none());
    }

    #[tokio::test]
    async fn start_subscribe_opens_realtime_tail() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        let next_sub = app.next_sub_id;
        app.start_subscribe(SubSpec::Channel("news".into()));
        assert_eq!(app.screen, Screen::Realtime);
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.subs.len(), 1);
        assert_eq!(conn.active_sub, Some(0));
        assert_eq!(conn.subs[0].state, SubState::Connecting);
        assert_eq!(conn.subs[0].label, "pubsub:news");
        assert_eq!(app.next_sub_id, next_sub + 1);
    }

    #[tokio::test]
    async fn duplicate_subscribe_focuses_existing_tail() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("news".into()));
        app.start_subscribe(SubSpec::Channel("news".into()));
        assert_eq!(app.active_conn().unwrap().subs.len(), 1, "no duplicate tab");
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("already tailing"));
    }

    // -- realtime state transitions ------------------------------------------

    #[tokio::test]
    async fn realtime_event_marks_tail_active_and_buffers() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        let sub_id = app.connections[0].subs[0].sub_id;
        app.handle_event(AppEvent::Realtime {
            id,
            sub_id,
            event: broker_event("hi"),
        });
        let sub = &app.connections[0].subs[0];
        assert_eq!(sub.state, SubState::Active);
        assert_eq!(sub.received, 1);
        assert_eq!(sub.events.len(), 1);
    }

    #[tokio::test]
    async fn sub_started_marks_active() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        let sub_id = app.connections[0].subs[0].sub_id;
        app.handle_event(AppEvent::SubscriptionStarted { id, sub_id });
        assert_eq!(app.connections[0].subs[0].state, SubState::Active);
    }

    #[tokio::test]
    async fn sub_ended_marks_ended_and_stops_recording() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        let sub_id = app.connections[0].subs[0].sub_id;
        app.handle_event(AppEvent::SubscriptionEnded {
            id,
            sub_id,
            reason: Some("source closed".into()),
        });
        let sub = &app.connections[0].subs[0];
        assert_eq!(sub.state, SubState::Ended(Some("source closed".into())));
        assert_eq!(sub.recording, RecordState::Off);
        assert!(app.status.as_ref().unwrap().message.contains("tail ended"));
    }

    #[tokio::test]
    async fn recording_update_transitions() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        let sub_id = app.connections[0].subs[0].sub_id;
        let path = PathBuf::from("/tmp/rec.jsonl");

        app.handle_event(AppEvent::RecordingUpdate {
            id,
            sub_id,
            status: RecordingStatus::Started { path: path.clone() },
        });
        assert!(app.connections[0].subs[0].recording.is_on());
        assert!(app.status.as_ref().unwrap().message.contains("recording →"));

        app.handle_event(AppEvent::RecordingUpdate {
            id,
            sub_id,
            status: RecordingStatus::Progress {
                records: 9,
                bytes: 123,
            },
        });
        match &app.connections[0].subs[0].recording {
            RecordState::On { records, bytes, .. } => {
                assert_eq!((*records, *bytes), (9, 123));
            }
            other => panic!("expected On, got {other:?}"),
        }

        app.handle_event(AppEvent::RecordingUpdate {
            id,
            sub_id,
            status: RecordingStatus::Stopped {
                records: 9,
                bytes: 123,
                path,
            },
        });
        assert_eq!(app.connections[0].subs[0].recording, RecordState::Off);
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("recorded 9 events"));

        app.handle_event(AppEvent::RecordingUpdate {
            id,
            sub_id,
            status: RecordingStatus::Failed {
                error: "disk full".into(),
            },
        });
        let status = app.status.as_ref().unwrap();
        assert!(status.is_error);
        assert!(status.message.contains("recording failed: disk full"));
    }

    #[tokio::test]
    async fn toggle_recording_without_tail_errors() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.screen = Screen::Realtime;
        app.toggle_recording();
        let status = app.status.as_ref().unwrap();
        assert!(status.is_error);
        assert!(status.message.contains("no active tail"));
    }

    #[tokio::test]
    async fn toggle_recording_on_ended_tail_errors() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        app.connections[0].subs[0].state = SubState::Ended(None);
        app.toggle_recording();
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("tail has ended"));
    }

    #[tokio::test]
    async fn toggle_recording_requests_start_on_active_tail() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        app.toggle_recording();
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("starting recording"));
    }

    #[tokio::test]
    async fn stop_active_tail_removes_focused_tab() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        app.start_subscribe(SubSpec::Channel("d".into()));
        assert_eq!(app.connections[0].active_sub, Some(1));
        app.stop_active_tail();
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.subs.len(), 1);
        assert_eq!(conn.subs[0].label, "pubsub:c");
        assert_eq!(conn.active_sub, Some(0));
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("stopped pubsub:d"));
    }

    #[tokio::test]
    async fn focus_tab_wraps_around() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        for c in ['a', 'b', 'c'] {
            app.start_subscribe(SubSpec::Channel(c.to_string()));
        }
        assert_eq!(app.connections[0].active_sub, Some(2));
        app.focus_tab(1);
        assert_eq!(app.connections[0].active_sub, Some(0), "wraps past the end");
        app.focus_tab(-1);
        assert_eq!(
            app.connections[0].active_sub,
            Some(2),
            "wraps past the start"
        );
    }

    #[tokio::test]
    async fn scroll_tail_clamps_and_toggles_follow() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Channel("c".into()));
        for i in 0..5 {
            app.connections[0].subs[0].push(broker_event(&format!("m{i}")));
        }
        app.scroll_tail(-1); // up == back into history
        let sub = &app.connections[0].subs[0];
        assert_eq!(sub.offset, 1);
        assert!(!sub.follow);

        app.scroll_tail(-100); // clamp at the oldest event
        assert_eq!(app.connections[0].subs[0].offset, 4);

        app.scroll_tail(100); // back to newest -> following again
        let sub = &app.connections[0].subs[0];
        assert_eq!(sub.offset, 0);
        assert!(sub.follow);
    }

    // -- tail_selected_key ---------------------------------------------------

    #[tokio::test]
    async fn tail_selected_key_starts_stream_tail() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.connections[0].keys = vec![stream_entry("orders", ValueType::Stream)];
        app.connections[0].rebuild_view();
        // Always grouped: row 0 is the "(no prefix)" header, the key is row 1.
        app.connections[0].table.select(Some(1));
        app.tail_selected_key();
        assert_eq!(app.screen, Screen::Realtime);
        assert_eq!(app.active_conn().unwrap().subs.len(), 1);
        assert_eq!(app.active_conn().unwrap().subs[0].label, "stream:orders");
    }

    #[tokio::test]
    async fn tail_selected_key_rejects_non_stream() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.connections[0].keys = vec![stream_entry("greeting", ValueType::String)];
        app.connections[0].rebuild_view();
        // Always grouped: row 0 is the "(no prefix)" header, the key is row 1.
        app.connections[0].table.select(Some(1));
        app.tail_selected_key();
        assert!(app.active_conn().unwrap().subs.is_empty());
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("only streams can be tailed"));
    }

    #[tokio::test]
    async fn tail_selected_key_without_selection_errors() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.connections[0].keys.clear();
        app.tail_selected_key();
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("no key selected"));
    }

    // -- form ----------------------------------------------------------------

    #[test]
    fn add_connection_opens_form() {
        let (mut app, _rx) = test_app();
        app.apply(Action::AddConnection);
        assert!(app.form.is_some());
        assert_eq!(app.mode, InputMode::Form);
    }

    #[test]
    fn form_typing_and_backspace_edit_focused_field() {
        let (mut app, _rx) = test_app();
        app.apply(Action::AddConnection);
        app.form.as_mut().unwrap().fields[0].clear();
        app.handle_key(ch('p'));
        app.handle_key(ch('q'));
        assert_eq!(app.form.as_ref().unwrap().fields[0], "pq");
        app.handle_key(key(KeyCode::Backspace));
        assert_eq!(app.form.as_ref().unwrap().fields[0], "p");
    }

    #[test]
    fn form_tab_moves_focus_and_tls_toggles() {
        let (mut app, _rx) = test_app();
        app.apply(Action::AddConnection);
        app.handle_key(key(KeyCode::Tab));
        assert_eq!(app.form.as_ref().unwrap().focus, 1);
        app.handle_key(key(KeyCode::BackTab));
        assert_eq!(app.form.as_ref().unwrap().focus, 0);

        app.form.as_mut().unwrap().focus = ConnForm::TLS_FOCUS;
        app.handle_key(ch(' '));
        assert!(app.form.as_ref().unwrap().tls);
        app.handle_key(ch(' '));
        assert!(!app.form.as_ref().unwrap().tls);
    }

    #[test]
    fn form_escape_cancels() {
        let (mut app, _rx) = test_app();
        app.apply(Action::AddConnection);
        app.handle_key(key(KeyCode::Esc));
        assert!(app.form.is_none());
        assert_eq!(app.mode, InputMode::Normal);
    }

    #[test]
    fn form_validation_rejects_bad_fields() {
        let (mut app, _rx) = test_app();
        app.apply(Action::AddConnection);
        // Default form has an empty name.
        app.submit_form();
        assert!(app.form.is_some(), "form stays open on error");
        assert_eq!(
            app.form.as_ref().unwrap().error.as_deref(),
            Some("name is required")
        );

        app.form.as_mut().unwrap().fields[0] = "ok".into();
        app.form.as_mut().unwrap().fields[2] = "notaport".into();
        app.submit_form();
        assert!(app
            .form
            .as_ref()
            .unwrap()
            .error
            .as_deref()
            .unwrap()
            .contains("port"));

        app.form.as_mut().unwrap().fields[2] = "6390".into();
        app.form.as_mut().unwrap().fields[3] = "xx".into();
        app.submit_form();
        assert!(app
            .form
            .as_ref()
            .unwrap()
            .error
            .as_deref()
            .unwrap()
            .contains("db"));
    }

    #[tokio::test]
    async fn form_submit_persists_profile_and_connects() {
        let path = unique_config_path();
        let (mut app, _rx) = build_app(Config::default(), path.clone(), None);
        app.apply(Action::AddConnection);
        {
            let form = app.form.as_mut().unwrap();
            form.fields[0] = "c1".into(); // name
            form.fields[1] = "".into(); // host -> defaults to 127.0.0.1
            form.fields[2] = "6399".into(); // port
            form.fields[3] = "2".into(); // db
            form.fields[4] = "".into(); // username
            form.fields[5] = "secret".into(); // password literal
        }
        app.submit_form();

        assert!(app.form.is_none());
        assert_eq!(app.mode, InputMode::Normal);
        assert_eq!(app.profiles.len(), 1);
        let ConnectionConfig::Redis(p) = &app.profiles[0] else {
            panic!("expected a redis profile");
        };
        assert_eq!(p.name, "c1");
        assert_eq!(p.host, "127.0.0.1", "blank host defaults");
        assert_eq!(p.port, 6399);
        assert_eq!(p.db, 2);
        assert_eq!(
            p.password.as_deref(),
            Some("prompt"),
            "a literal password is persisted as a prompt spec, never plaintext"
        );
        assert_eq!(app.next_id, 2, "a connection attempt was kicked off");

        let saved = std::fs::read_to_string(&path).expect("config written");
        assert!(saved.contains("c1"));
        assert!(!saved.contains("secret"), "the literal must not be written");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn form_submit_builds_rabbitmq_profile_with_vhost() {
        let path = unique_config_path();
        let (mut app, _rx) = build_app(Config::default(), path.clone(), None);
        app.apply(Action::AddConnection);
        {
            let form = app.form.as_mut().unwrap();
            form.toggle_kind(); // Redis -> AMQP
            form.toggle_kind(); // AMQP  -> RabbitMQ
            form.fields[0] = "rmq".into(); // name
            form.fields[1] = "rabbit.local".into(); // host
            form.fields[2] = "5672".into(); // port
            form.fields[3] = "staging".into(); // slot 3 == Vhost for RabbitMQ
            form.fields[4] = "app".into(); // username
            form.fields[5] = "".into(); // password
        }
        app.submit_form();

        assert_eq!(app.profiles.len(), 1);
        let ConnectionConfig::Rabbitmq(p) = &app.profiles[0] else {
            panic!("expected a rabbitmq profile");
        };
        assert_eq!(p.name, "rmq");
        assert_eq!(p.host, "rabbit.local");
        assert_eq!(p.port, 5672);
        assert_eq!(p.vhost, "staging", "slot 3 is read as the vhost");
        assert_eq!(p.username.as_deref(), Some("app"));

        let saved = std::fs::read_to_string(&path).expect("config written");
        assert!(saved.contains("type = \"rabbitmq\""));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn form_submit_rabbitmq_blank_vhost_defaults_to_root() {
        let path = unique_config_path();
        let (mut app, _rx) = build_app(Config::default(), path.clone(), None);
        app.apply(Action::AddConnection);
        {
            let form = app.form.as_mut().unwrap();
            form.toggle_kind(); // -> AMQP
            form.toggle_kind(); // -> RabbitMQ
            form.fields[0] = "rmq2".into();
            form.fields[3] = "   ".into(); // whitespace-only vhost
        }
        app.submit_form();

        let ConnectionConfig::Rabbitmq(p) = &app.profiles[0] else {
            panic!("expected a rabbitmq profile");
        };
        assert_eq!(p.vhost, "/", "a blank vhost defaults to /");
        let _ = std::fs::remove_file(&path);
    }

    // -- input mode plumbing -------------------------------------------------

    #[test]
    fn key_release_events_are_ignored() {
        let (mut app, _rx) = test_app();
        // Esc on Connections quits (back) on *press*; a release must be ignored.
        let release = KeyEvent {
            code: KeyCode::Esc,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        };
        app.handle_key(release);
        assert!(app.running, "key releases must not trigger actions");
    }

    // -- recordings ----------------------------------------------------------

    #[test]
    fn scan_recordings_lists_only_jsonl_newest_first() {
        let dir = std::env::temp_dir().join(format!("keyhole-scan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.jsonl"), "x").unwrap();
        std::fs::write(dir.join("b.jsonl"), "y").unwrap();
        std::fs::write(dir.join("notes.txt"), "z").unwrap();

        let (mut app, _rx) = test_app();
        app.recordings_dir = dir.clone();
        app.scan_recordings();
        assert_eq!(app.recordings.len(), 2, "only .jsonl files are listed");
        assert!(app.recordings.iter().all(|f| f.name.ends_with(".jsonl")));
        assert!(
            app.recordings
                .windows(2)
                .all(|w| w[0].modified >= w[1].modified),
            "sorted newest first"
        );
        assert_eq!(app.recordings_state.selected(), Some(0));

        // A stale, out-of-range selection is clamped on rescan.
        app.recordings_state.select(Some(9));
        app.scan_recordings();
        assert_eq!(app.recordings_state.selected(), Some(1));

        // Emptying the directory clears the selection.
        std::fs::remove_file(dir.join("a.jsonl")).unwrap();
        std::fs::remove_file(dir.join("b.jsonl")).unwrap();
        app.scan_recordings();
        assert!(app.recordings.is_empty());
        assert_eq!(app.recordings_state.selected(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- monitor / keyspace tails --------------------------------------------

    #[tokio::test]
    async fn start_monitor_opens_a_monitor_tail() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.apply(Action::StartMonitor);
        assert_eq!(app.screen, Screen::Realtime);
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.subs.len(), 1);
        assert_eq!(conn.subs[0].spec, SubSpec::Monitor);
        assert_eq!(conn.subs[0].label, "monitor");
    }

    #[tokio::test]
    async fn start_keyspace_uses_active_db() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.connections[0].db = 3;
        app.apply(Action::StartKeyspace);
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.subs[0].spec, SubSpec::Keyspace { db: 3 });
        assert_eq!(conn.subs[0].label, "keyspace:db3");
    }

    #[test]
    fn start_monitor_without_connection_errors() {
        let (mut app, _rx) = test_app();
        app.apply(Action::StartMonitor);
        assert!(app.active_conn().is_none());
        assert!(app.status.as_ref().unwrap().is_error);
    }

    #[tokio::test]
    async fn sub_notice_is_stored_on_the_tail() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.start_subscribe(SubSpec::Keyspace { db: 0 });
        let sub_id = app.connections[0].subs[0].sub_id;
        app.handle_event(AppEvent::SubscriptionNotice {
            id,
            sub_id,
            notice: "notifications disabled".into(),
        });
        assert_eq!(
            app.connections[0].subs[0].notice.as_deref(),
            Some("notifications disabled")
        );
        assert!(app.status.as_ref().unwrap().is_error);
    }

    // -- AMQP / capabilities -------------------------------------------------

    /// Attach a live AMQP-capability mock connection.
    async fn connect_amqp(app: &mut App, id: u32, name: &str) -> ConnId {
        let handle = mock::amqp_handle(id, name).await;
        app.handle_event(AppEvent::Connected { handle });
        ConnId(id)
    }

    #[tokio::test]
    async fn amqp_connection_opens_realtime_not_browser() {
        let (mut app, _rx) = test_app();
        connect_amqp(&mut app, 1, "mq").await;
        assert_eq!(
            app.screen,
            Screen::Realtime,
            "AMQP has no browser, so it lands on realtime"
        );
        assert_eq!(app.active_conn().unwrap().label(), "mq [amqp]");
    }

    #[tokio::test]
    async fn amqp_capabilities_gate_redis_only_screens() {
        let (mut app, _rx) = test_app();
        connect_amqp(&mut app, 1, "mq").await;
        // The Browser (and the console band it now carries) is Redis-only.
        app.screen = Screen::Realtime;
        app.apply(Action::GotoBrowser);
        assert_eq!(app.screen, Screen::Realtime, "GotoBrowser must be blocked");
        assert!(app
            .status
            .as_ref()
            .unwrap()
            .message
            .contains("no key browser"));
    }

    #[tokio::test]
    async fn amqp_tails_a_topic() {
        let (mut app, _rx) = test_app();
        connect_amqp(&mut app, 1, "mq").await;
        app.start_subscribe(SubSpec::Topic("events".into()));
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.subs.len(), 1);
        assert_eq!(conn.subs[0].spec, SubSpec::Topic("events".into()));
        assert_eq!(conn.subs[0].label, "topic:events");
    }

    #[tokio::test]
    async fn amqp_tick_skips_stats_refresh() {
        // A non-dashboard broker must not be pinged for stats each tick.
        let (mut app, mut rx) = test_app();
        connect_amqp(&mut app, 1, "mq").await;
        // Drain the connect-time events.
        while rx.try_recv().is_ok() {}
        app.connections[0].stat_ticks = STATS_REFRESH_TICKS - 1;
        app.on_tick();
        // The mock's stats() succeeds, so a RefreshStats would surface as
        // StatsUpdated. Give the actor a moment, then assert none arrived.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let mut saw_stats = false;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, AppEvent::StatsUpdated { .. }) {
                saw_stats = true;
            }
        }
        assert!(!saw_stats, "AMQP tick must not request stats");
    }

    // -- console -------------------------------------------------------------

    #[tokio::test]
    async fn console_edit_requires_browser_with_console() {
        // The console is a band in the Browser, entered with `i` (ConsoleEdit).
        let (mut app, _rx) = test_app();
        // No connection: `i` is inert.
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        assert_eq!(
            app.mode,
            InputMode::Normal,
            "no connection → no command mode"
        );

        connect(&mut app, 1, "prod", 16).await;
        // Console-capable, but not on the Browser screen: still inert.
        app.screen = Screen::Realtime;
        app.apply(Action::ConsoleEdit);
        assert_eq!(
            app.mode,
            InputMode::Normal,
            "the console band only lives in the Browser"
        );
        // On the Browser with a console-capable broker: enters command mode.
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        assert_eq!(app.mode, InputMode::Command);
    }

    #[tokio::test]
    async fn console_edit_inert_without_console_capability() {
        // A broker with no console (AMQP), even forced onto a Browser screen,
        // must not enter command mode — the capability gate, not just the screen.
        let (mut app, _rx) = test_app();
        connect_amqp(&mut app, 1, "mq").await;
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        assert_eq!(app.mode, InputMode::Normal);
    }

    #[tokio::test]
    async fn console_edit_and_submit_records_command() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        assert_eq!(app.mode, InputMode::Command);
        for c in "GET k".chars() {
            app.handle_key(ch(c));
        }
        assert_eq!(app.connections[0].console.input, "GET k");
        app.handle_key(key(KeyCode::Enter));
        let console = &app.connections[0].console;
        assert_eq!(console.pending.as_deref(), Some("GET k"));
        assert_eq!(console.history, vec!["GET k"]);
        assert!(console.input.is_empty(), "input cleared after submit");
        assert_eq!(app.mode, InputMode::Command, "stays in command mode");
        // Esc leaves command mode.
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(app.mode, InputMode::Normal);
    }

    #[tokio::test]
    async fn command_result_appends_entry_and_clears_pending() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.connections[0].console.pending = Some("PING".into());
        app.handle_event(AppEvent::CommandResult {
            id,
            command: "PING".into(),
            result: Ok("PONG".into()),
        });
        let console = &app.connections[0].console;
        assert!(console.pending.is_none());
        assert_eq!(console.entries.len(), 1);
        assert_eq!(console.entries[0].output, "PONG");
        assert!(!console.entries[0].is_error);

        app.handle_event(AppEvent::CommandResult {
            id,
            command: "SET k v".into(),
            result: Err("refused".into()),
        });
        let last = app.connections[0].console.entries.last().unwrap();
        assert!(last.is_error);
        assert_eq!(last.output, "refused");
    }

    #[tokio::test]
    async fn console_empty_submit_is_ignored() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        app.handle_key(key(KeyCode::Enter)); // empty
        assert!(app.connections[0].console.history.is_empty());
        assert!(app.connections[0].console.pending.is_none());
    }

    #[tokio::test]
    async fn clear_console_empties_output() {
        let (mut app, _rx) = test_app();
        let id = connect(&mut app, 1, "prod", 16).await;
        app.handle_event(AppEvent::CommandResult {
            id,
            command: "PING".into(),
            result: Ok("PONG".into()),
        });
        assert_eq!(app.connections[0].console.entries.len(), 1);
        // Ctrl-L clears the console while it is focused (command mode).
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL));
        assert!(app.connections[0].console.entries.is_empty());
    }

    #[tokio::test]
    async fn console_scroll_via_pageup_pagedown() {
        let (mut app, _rx) = test_app();
        connect(&mut app, 1, "prod", 16).await;
        // The console band scrolls with PageUp/PageDown while focused (command
        // mode), since ↑↓ are taken by command history recall.
        app.screen = Screen::Browser;
        app.apply(Action::ConsoleEdit);
        app.handle_key(key(KeyCode::PageUp)); // scroll back a page
        assert_eq!(
            app.connections[0].console.scroll,
            CONSOLE_SCROLL_STEP as u16
        );
        app.handle_key(key(KeyCode::PageDown)); // toward the newest output
        assert_eq!(app.connections[0].console.scroll, 0);
        app.handle_key(key(KeyCode::PageDown)); // clamped at the bottom
        assert_eq!(app.connections[0].console.scroll, 0);
    }

    // -- command palette -----------------------------------------------------

    #[test]
    fn palette_opens_filters_and_dispatches() {
        let (mut app, _rx) = test_app();
        app.apply(Action::OpenPalette);
        assert_eq!(app.mode, InputMode::Palette);
        assert!(app.palette.is_some());
        // Filter down to "Quit" and run it.
        for c in "quit".chars() {
            app.handle_key(ch(c));
        }
        assert_eq!(app.palette.as_ref().unwrap().query, "quit");
        app.handle_key(key(KeyCode::Enter));
        assert!(app.palette.is_none(), "palette closes after dispatch");
        assert_eq!(app.mode, InputMode::Normal);
        assert!(!app.running, "selecting Quit dispatched the action");
    }

    #[test]
    fn palette_escape_closes_without_acting() {
        let (mut app, _rx) = test_app();
        app.apply(Action::OpenPalette);
        app.handle_key(key(KeyCode::Esc));
        assert!(app.palette.is_none());
        assert_eq!(app.mode, InputMode::Normal);
        assert!(app.running);
    }

    #[test]
    fn palette_nav_wraps_within_matches() {
        let (mut app, _rx) = test_app();
        app.apply(Action::OpenPalette);
        // Narrow to the "Go to:" entries so the count is predictable.
        for c in "go to".chars() {
            app.handle_key(ch(c));
        }
        let count = app.palette_labels().len();
        assert!(count >= 2); // Realtime/Recordings "Go to:" entries (Connections/Browser use Enter/Esc)
        app.handle_key(key(KeyCode::Up)); // wrap to the last
        assert_eq!(app.palette.as_ref().unwrap().selected, count - 1);
        app.handle_key(key(KeyCode::Down)); // back to the first
        assert_eq!(app.palette.as_ref().unwrap().selected, 0);
    }

    // -- mouse ---------------------------------------------------------------

    #[test]
    fn mouse_scroll_moves_selection_in_normal_mode() {
        let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
        assert_eq!(app.profile_state.selected(), Some(0));
        app.handle_mouse(MouseEventKind::ScrollDown);
        assert_eq!(app.profile_state.selected(), Some(1));
        app.handle_mouse(MouseEventKind::ScrollUp);
        assert_eq!(app.profile_state.selected(), Some(0));
    }

    #[test]
    fn mouse_scroll_ignored_during_text_entry() {
        let (mut app, _rx) = build_app(config_with(&["a", "b"]), unique_config_path(), None);
        app.mode = InputMode::Palette;
        app.handle_mouse(MouseEventKind::ScrollDown);
        assert_eq!(
            app.profile_state.selected(),
            Some(0),
            "no navigation while typing"
        );
    }

    // -- pure helpers --------------------------------------------------------

    #[test]
    fn move_selection_handles_edges() {
        assert_eq!(move_selection(None, 0, 1), None, "empty list");
        assert_eq!(
            move_selection(None, 3, 1),
            Some(1),
            "from unset starts at 0"
        );
        assert_eq!(move_selection(Some(0), 3, -1), Some(0), "clamped low");
        assert_eq!(move_selection(Some(2), 3, 1), Some(2), "clamped high");
        assert_eq!(move_selection(Some(1), 3, 10), Some(2));
        assert_eq!(move_selection(Some(1), 3, -10), Some(0));
    }

    #[test]
    fn classify_password_distinguishes_specs_from_literals() {
        assert_eq!(classify_password(""), (None, None));
        assert_eq!(
            classify_password("hunter2"),
            (Some("prompt".to_string()), Some("hunter2".to_string())),
            "a literal is never persisted; a prompt spec stands in"
        );
        assert_eq!(
            classify_password("keyring"),
            (Some("keyring".to_string()), None)
        );
        assert_eq!(
            classify_password("prompt"),
            (Some("prompt".to_string()), None)
        );
        assert_eq!(
            classify_password("env:REDIS_PW"),
            (Some("env:REDIS_PW".to_string()), None)
        );
        assert_eq!(
            classify_password("keyring:prod"),
            (Some("keyring:prod".to_string()), None)
        );
    }
}
