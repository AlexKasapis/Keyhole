//! Application state machine: owns all UI state and the open connections, turns
//! [`AppEvent`]s into state changes, and drives connection actors. The render
//! loop is the sole owner of this type, so no locking is needed for UI state.

mod action;
mod state;

pub use state::{ConnForm, Connection, InputMode, Screen, Status};

use std::path::PathBuf;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::widgets::ListState;
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::action::Action;
use crate::broker::actor::{spawn_connection, ConnCommand, ConnHandle};
use crate::broker::redis::RedisConnection;
use crate::broker::{
    BrokerConnection, BrowsePage, BrowseReq, ConnId, InspectReq, ServerStats, ValueView,
};
use crate::config::{self, Config, ConnectionConfig, RedisProfile};
use crate::event::AppEvent;

/// How many ticks (~250ms each) between automatic dashboard stat refreshes.
const STATS_REFRESH_TICKS: u32 = 8;
/// Inspect window / SCAN look-ahead margin for auto load-more.
const VALUE_LIMIT: usize = 200;
const LOAD_MORE_MARGIN: usize = 5;

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
    next_id: u32,
    pending_connect: Option<String>,

    // UI state (read by `crate::ui`).
    pub(crate) profiles: Vec<RedisProfile>,
    pub(crate) profile_state: ListState,
    pub(crate) connections: Vec<Connection>,
    pub(crate) active: Option<usize>,
    pub(crate) screen: Screen,
    pub(crate) mode: InputMode,
    pub(crate) filter: String,
    pub(crate) form: Option<ConnForm>,
    pub(crate) status: Option<Status>,
    pub(crate) show_help: bool,
    pub(crate) now: OffsetDateTime,
}

impl App {
    pub fn new(
        config: Config,
        config_path: PathBuf,
        events: Sender<AppEvent>,
        tracker: TaskTracker,
        cancel: CancellationToken,
        connect_on_start: Option<String>,
    ) -> Self {
        let profiles: Vec<RedisProfile> = config
            .connections
            .iter()
            .map(|c| match c {
                ConnectionConfig::Redis(p) => p.clone(),
            })
            .collect();
        let preview_bytes = config.settings.value_preview_bytes;
        let scan_count = config.settings.scan_count;
        let mut profile_state = ListState::default();
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
            next_id: 1,
            pending_connect: connect_on_start,
            profiles,
            profile_state,
            connections: Vec::new(),
            active: None,
            screen: Screen::Connections,
            mode: InputMode::Normal,
            filter: String::new(),
            form: None,
            status: None,
            show_help: false,
            now: OffsetDateTime::now_utc(),
        }
    }

    /// Kick off an auto-connect requested via `--connect`.
    pub fn on_start(&mut self) {
        if let Some(name) = self.pending_connect.take() {
            match self.profiles.iter().find(|p| p.name == name).cloned() {
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

    // -- event handling ------------------------------------------------------

    pub fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::Input(Event::Key(key)) => self.handle_key(key),
            AppEvent::Input(_) => {}
            AppEvent::Tick => self.on_tick(),
            AppEvent::Connected { handle } => self.on_connected(handle),
            AppEvent::Disconnected { id, reason } => self.on_disconnected(id, reason),
            AppEvent::KeysPage { id, page } => self.on_keys_page(id, page),
            AppEvent::ValueLoaded { id, key, value } => self.on_value(id, key, value),
            AppEvent::StatsUpdated { id, stats } => self.on_stats(id, stats),
            AppEvent::ConnError { id, context, error } => self.on_conn_error(id, context, error),
        }
    }

    fn on_tick(&mut self) {
        self.now = OffsetDateTime::now_utc();
        if let Some(conn) = self.active_conn_mut() {
            conn.stat_ticks += 1;
            if conn.stat_ticks >= STATS_REFRESH_TICKS {
                conn.stat_ticks = 0;
                conn.handle.send(ConnCommand::RefreshStats);
                // Liveness check; a failure surfaces as Disconnected.
                conn.handle.send(ConnCommand::Ping);
            }
        }
    }

    fn on_connected(&mut self, handle: ConnHandle) {
        let conn = Connection::new(handle);
        let id = conn.id;
        let name = conn.name.clone();
        self.connections.push(conn);
        self.active = Some(self.connections.len() - 1);
        self.screen = Screen::Browser;
        self.set_status(format!("Connected to {name}"), false);
        self.start_browse(id, true);
        self.request_stats(id);
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
        if let Some(conn) = self.conn_by_id_mut(id) {
            if page.db != conn.db {
                return; // stale page from a previous DB
            }
            conn.keys.extend(page.entries);
            conn.next_cursor = page.next_cursor;
            conn.complete = page.next_cursor == 0;
            if conn.table.selected().is_none() && !conn.keys.is_empty() {
                conn.table.select(Some(0));
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
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Quit => self.running = false,
            Action::Up => self.nav(-1),
            Action::Down => self.nav(1),
            Action::PageUp => self.nav(-10),
            Action::PageDown => self.nav(10),
            Action::Top => self.nav_edge(true),
            Action::Bottom => self.nav_edge(false),
            Action::Enter => {
                if self.screen == Screen::Connections {
                    self.connect_selected_profile();
                }
            }
            Action::AddConnection => {
                self.form = Some(ConnForm::new());
                self.mode = InputMode::Form;
            }
            Action::GotoConnections => self.screen = Screen::Connections,
            Action::GotoBrowser => {
                if self.active.is_some() {
                    self.screen = Screen::Browser;
                }
            }
            Action::GotoDashboard => {
                if let Some(id) = self.active_id() {
                    self.screen = Screen::Dashboard;
                    self.request_stats(id);
                }
            }
            Action::StartFilter => {
                if self.screen == Screen::Browser && self.active.is_some() {
                    self.filter.clear();
                    self.mode = InputMode::Filter;
                }
            }
            Action::DbPrev => self.change_db(-1),
            Action::DbNext => self.change_db(1),
            Action::LoadMore => {
                if self.screen == Screen::Browser {
                    if let Some(id) = self.active_id() {
                        self.start_browse(id, false);
                    }
                }
            }
            Action::Refresh => {
                if let Some(id) = self.active_id() {
                    self.start_browse(id, true);
                    self.request_stats(id);
                }
            }
            Action::ToggleHelp => self.show_help = !self.show_help,
            Action::Dismiss => self.show_help = false,
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
                    if form.focus == ConnForm::TLS_FOCUS {
                        if matches!(c, ' ' | 't' | 'f' | 'y' | 'n') {
                            form.tls = !form.tls;
                        }
                    } else {
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
                    let next = move_selection(conn.table.selected(), conn.keys.len(), delta);
                    conn.table.select(next);
                }
                if let Some(id) = self.active_id() {
                    self.request_selected_value(id);
                    self.maybe_load_more(id);
                }
            }
            Screen::Dashboard => {}
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
                    let len = conn.keys.len();
                    if len > 0 {
                        conn.table.select(Some(if top { 0 } else { len - 1 }));
                    }
                }
                if let Some(id) = self.active_id() {
                    self.request_selected_value(id);
                    self.maybe_load_more(id);
                }
            }
            Screen::Dashboard => {}
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
        self.start_browse(id, true);
    }

    // -- connection lifecycle ------------------------------------------------

    fn connect_selected_profile(&mut self) {
        let Some(sel) = self.profile_state.selected() else {
            return;
        };
        let Some(profile) = self.profiles.get(sel).cloned() else {
            return;
        };
        if let Some(idx) = self.connections.iter().position(|c| c.name == profile.name) {
            self.active = Some(idx);
            self.screen = Screen::Browser;
            return;
        }
        self.start_connect(profile, None);
    }

    fn start_connect(&mut self, profile: RedisProfile, override_password: Option<String>) {
        let id = ConnId(self.next_id);
        self.next_id += 1;
        let events = self.events.clone();
        let tracker = self.tracker.clone();
        let cancel = self.cancel.clone();
        let preview = self.preview_bytes;
        let name = profile.name.clone();
        self.set_status(format!("Connecting to {name}…"), false);

        tokio::spawn(async move {
            let password = match override_password {
                Some(pw) => Some(pw),
                None => match resolve_password(&profile).await {
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
            let conn: Box<dyn BrokerConnection> =
                Box::new(RedisConnection::new(profile, password, preview));
            match spawn_connection(id, name, conn, events.clone(), &tracker, &cancel).await {
                Ok(handle) => {
                    let _ = events.send(AppEvent::Connected { handle }).await;
                }
                Err(e) => {
                    let _ = events
                        .send(AppEvent::ConnError {
                            id,
                            context: "connect".to_string(),
                            error: e.to_string(),
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
        let db: u32 = match form.fields[3].trim().parse() {
            Ok(d) => d,
            Err(_) => return self.form_error("db must be a number"),
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

        let profile = RedisProfile {
            name,
            host,
            port,
            db,
            username,
            password: saved_spec,
            tls,
        };

        // Persist (best effort) and keep the in-memory profile list in sync.
        self.config
            .connections
            .push(ConnectionConfig::Redis(profile.clone()));
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

    fn start_browse(&mut self, id: ConnId, reset: bool) {
        let page_size = self.scan_count;
        if let Some(conn) = self.conn_by_id_mut(id) {
            if reset {
                conn.keys.clear();
                conn.next_cursor = 0;
                conn.complete = false;
                conn.table.select(Some(0));
                conn.value = None;
                conn.value_key = None;
                conn.value_scroll = 0;
            }
            let req = BrowseReq {
                db: conn.db,
                pattern: conn.pattern.clone(),
                cursor: conn.next_cursor,
                page_size,
            };
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

    fn maybe_load_more(&mut self, id: ConnId) {
        let should = self.conn_by_id(id).is_some_and(|c| {
            !c.complete && c.table.selected().unwrap_or(0) + LOAD_MORE_MARGIN >= c.keys.len()
        });
        if should {
            self.start_browse(id, false);
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
            self.start_browse(id, true);
        }
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

/// Resolve a profile's secret off the render thread (keyring access can block).
async fn resolve_password(profile: &RedisProfile) -> anyhow::Result<Option<String>> {
    let spec = profile.password_spec();
    let account = profile.name.clone();
    tokio::task::spawn_blocking(move || config::resolve_secret(&spec, &account)).await?
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
