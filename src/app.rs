//! Application state machine: owns all UI state and the open connections, turns
//! [`AppEvent`]s into state changes, and drives connection actors. The render
//! loop is the sole owner of this type, so no locking is needed for UI state.

mod action;
mod state;

// `impl App` is split across these files to keep this module a thin spine: the
// struct, its constructor/accessors, the event dispatcher, and small helpers
// live here; each submodule adds an `impl App` block for one area of behaviour.
mod connection;
mod console;
mod events;
mod input;
mod realtime;
mod recordings;
mod settings;

pub use state::{
    ConnForm, ConnHealth, Connection, Console, ConsoleEntry, InputMode, PaletteCommand,
    PaletteState, PaneFocus, PanelTab, RecordState, RecordingFile, ScanStep, Screen, SettingsState,
    Status, StatusKind, SubState, Subscription, ViewRow,
};

use std::path::PathBuf;
use std::time::Duration;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEventKind};
use ratatui::widgets::{ListState, TableState};
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::action::Action;
use crate::broker::actor::{spawn_connection, ConnCommand, ConnHandle};
use crate::broker::factory::connection_for;
use crate::broker::{
    BrokerEvent, BrokerKind, BrowsePage, Capabilities, ConnId, InspectReq, ServerStats, SubSpec,
    ValueType, ValueView,
};
use crate::config::{self, AmqpProfile, Config, ConnectionConfig, RabbitmqProfile, RedisProfile};
use crate::event::AppEvent;
use crate::recording::{self, RecordingStatus, RecordingView};
use crate::theme::Theme;

/// Nominal period of one UI tick, mirroring `crate::TICK_PERIOD`. Used to turn
/// a configured refresh interval (milliseconds) into a tick count.
const TICK_PERIOD_MS: u64 = 250;
/// How many ticks (~250ms each) between automatic dashboard stat refreshes.
const STATS_REFRESH_TICKS: u32 = 8;
/// How long a transient status-bar notification stays before it self-dismisses.
/// Confirmation prompts (e.g. "Press d again …") are exempt — they live and die
/// with their key chord, not this timer (see [`Status`] / [`StatusKind`]).
const STATUS_TTL: time::Duration = time::Duration::seconds(3);
/// How long before its expiry a transient notification begins to fade out.
/// Shorter than [`STATUS_TTL`] so the message reads solid for most of its life
/// and only dissolves over the final stretch. See [`App::status_fade`].
const STATUS_FADE: time::Duration = time::Duration::milliseconds(1000);
/// How many elements of a value to fetch into the inspector at a time.
const VALUE_LIMIT: usize = 200;
/// Minimum wall-clock gap between progressive key-browser view rebuilds while a
/// foreground scan streams in. A large keyspace arrives over many SCAN pages;
/// rebuilding (a full re-sort) on every page is quadratic in the key count, so
/// mid-scan rebuilds are coalesced to ~this cadence. The final page always
/// rebuilds regardless, so the finished list is exact.
const VIEW_REBUILD_INTERVAL: Duration = Duration::from_millis(100);
/// Lines the Browser value pane scrolls per PageUp/PageDown.
const VALUE_SCROLL_STEP: i32 = 10;
/// Lines the Browser console band scrolls per PageUp/PageDown while focused —
/// roughly one band's worth of output rows (see `CONSOLE_BAND_HEIGHT` in `ui`).
const CONSOLE_SCROLL_STEP: i32 = 4;
/// Events a focused live-feed tab scrolls per PageUp/PageDown (or Ctrl-U/D).
const FEED_SCROLL_STEP: i32 = 10;

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
    pub(crate) status: Option<Status>,
    /// Connection health while no connection is active. `Connected` is derived
    /// live from [`Self::active_conn`], so this field only carries the
    /// no-connection sub-state (offline / connecting / error); the connected
    /// state surfaces in the Browser's Server band, and connect/disconnect
    /// outcomes also post a footer status. See [`Self::conn_health`].
    pub(crate) health: ConnHealth,
    pub(crate) show_help: bool,
    /// The command palette overlay (opened with `:`), when showing. While set it
    /// captures all input until dismissed. See [`Self::open_palette`].
    pub(crate) palette: Option<PaletteState>,
    /// The settings page overlay, when showing (reached from the palette). While
    /// set it captures all input until dismissed. See [`Self::handle_settings_key`].
    pub(crate) settings: Option<SettingsState>,
    /// Whether terminal mouse capture is on. While on, the scroll wheel scrolls
    /// lists/panes; while off, the terminal's own text selection (and copy)
    /// works. Toggled with `m`. The render loop reconciles the real terminal
    /// state from this flag, so `App` stays free of terminal I/O.
    pub(crate) mouse_capture: bool,
    pub(crate) recordings: Vec<RecordingFile>,
    pub(crate) recordings_state: ListState,
    /// Loaded view of the selected recording: `(file name, parsed records)`.
    /// Reloaded only when the selection lands on a different file, so it is
    /// cheap to refresh after every navigation step. Unlike a bounded preview,
    /// this holds the whole file so the viewer scrolls every record.
    pub(crate) recording_view: Option<(String, RecordingView)>,
    /// Vertical scroll offset (logical lines from the top) of the recording
    /// viewer pane. Reset to `0` whenever the selected recording changes.
    pub(crate) recordings_scroll: u16,
    /// The recording-name buffer being edited in [`InputMode::Rename`], primed
    /// with the current name when rename starts.
    pub(crate) rename_buf: String,
    /// Set after a first `d` on the Recordings tab: a second consecutive `d`
    /// deletes the selected recording; any other key disarms it.
    pub(crate) recordings_delete_armed: bool,
    pub(crate) now: OffsetDateTime,
    /// Set when a quit was requested from the home screen but not yet
    /// confirmed: closing the app needs a second consecutive Esc.
    quit_armed: bool,
    /// The connection whose key browser was viewed most recently. `b` jumps
    /// back to it (so with several brokers open it lands on the last one
    /// browsed), falling back to the active connection. Cleared when that
    /// connection drops. See [`Self::goto_browser`].
    last_browser: Option<ConnId>,
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
            screen: Screen::Home,
            mode: InputMode::Normal,
            filter: String::new(),
            subscribe_buf: String::new(),
            form: None,
            status: None,
            health: ConnHealth::Offline,
            show_help: false,
            palette: None,
            settings: None,
            // Capture starts on, matching `tui::init`.
            mouse_capture: true,
            recordings: Vec::new(),
            recordings_state: ListState::default(),
            recording_view: None,
            recordings_scroll: 0,
            rename_buf: String::new(),
            recordings_delete_armed: false,
            now: OffsetDateTime::now_utc(),
            quit_armed: false,
            last_browser: None,
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

    /// Whether the app wants terminal mouse capture on. The render loop reads
    /// this to reconcile the real terminal state (see `crate::tui`).
    pub fn mouse_capture(&self) -> bool {
        self.mouse_capture
    }

    /// The configured theme base name (e.g. `"dark"`), if any — read by the
    /// settings overlay to show and step the current theme selection.
    pub(crate) fn theme_base(&self) -> Option<&str> {
        self.config.theme.base.as_deref()
    }

    /// Connection health, surfaced by the Browser's Server band. An active
    /// connection always reads as [`ConnHealth::Connected`]; otherwise the most
    /// recent connection-lifecycle outcome (offline / connecting / error) is
    /// reported.
    pub fn conn_health(&self) -> ConnHealth {
        if self.active_conn().is_some() {
            ConnHealth::Connected
        } else {
            self.health
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

    /// Remember the active connection as the most recently viewed browser when
    /// the Browser screen is showing, so `b` can return to it later (see
    /// [`Self::goto_browser`]). A no-op off the Browser.
    fn note_browser_view(&mut self) {
        if self.screen == Screen::Browser {
            self.last_browser = self.active_id();
            // Each entry starts with the keys pane focused; reconcile the panel
            // mode + focus-scoped feeds for the freshly-entered browser.
            if let Some(conn) = self.active_conn_mut() {
                conn.focus = PaneFocus::Keys;
            }
            self.sync_panel_focus();
        }
    }

    /// Show a transient notification: it self-dismisses after [`STATUS_TTL`] (or
    /// the moment a newer notification replaces it). The everyday status path.
    fn set_status(&mut self, message: String, is_error: bool) {
        self.set_notification(message, is_error, StatusKind::Transient);
    }

    /// Show a confirmation prompt tied to an armed key chord (e.g. "Press d
    /// again to delete"). Unlike a transient notification it does not time out;
    /// it persists until the chord completes or is broken, at which point
    /// [`Self::clear_confirm`] removes it without a replacement.
    fn set_confirm(&mut self, message: String) {
        self.set_notification(message, false, StatusKind::Confirm);
    }

    fn set_notification(&mut self, message: String, is_error: bool, kind: StatusKind) {
        if is_error {
            tracing::warn!(%message, "status");
        } else {
            tracing::info!(%message, "status");
        }
        self.status = Some(Status {
            message,
            is_error,
            kind,
            shown_at: self.now,
        });
    }

    /// Dismiss a confirmation prompt (only). Called when a chord is broken so
    /// the prompt vanishes immediately with no message taking its place; any
    /// other (transient) status that happens to be showing is left untouched.
    fn clear_confirm(&mut self) {
        if matches!(&self.status, Some(s) if s.kind == StatusKind::Confirm) {
            self.status = None;
        }
    }

    /// Opacity for the active status notification while it fades out: `1.0`
    /// fully visible, `0.0` about to vanish. Only a [`StatusKind::Transient`]
    /// fades, and only over the final [`STATUS_FADE`] of its [`STATUS_TTL`]
    /// life; a confirmation prompt (or no status at all) always reads fully
    /// opaque, so the renderer draws it solid. A transient that is replaced by a
    /// newer one never reaches its fade window — the replacement resets the
    /// clock — so it just swaps in. Driven by the tick clock via `now`.
    pub(crate) fn status_fade(&self) -> f32 {
        let Some(status) = &self.status else {
            return 1.0;
        };
        if status.kind != StatusKind::Transient {
            return 1.0;
        }
        let remaining = STATUS_TTL - (self.now - status.shown_at);
        if remaining >= STATUS_FADE {
            return 1.0;
        }
        (remaining.as_seconds_f32() / STATUS_FADE.as_seconds_f32()).clamp(0.0, 1.0)
    }

    /// Drop a transient notification once it has been on screen for
    /// [`STATUS_TTL`]. Confirmation prompts are exempt — they are cleared by
    /// chord resolution, not the clock. Driven by the tick handler.
    fn expire_status(&mut self) {
        if let Some(status) = &self.status {
            if status.kind == StatusKind::Transient && self.now - status.shown_at >= STATUS_TTL {
                self.status = None;
            }
        }
    }
}

/// The screen to show a freshly-focused connection: the key browser when the
/// broker has one (Redis). Brokers without a browser (AMQP/RabbitMQ) have no
/// data screen yet — their realtime tails were removed pending a rework — so
/// they stay on the Connections list, where the row shows them live.
fn initial_screen(caps: &Capabilities) -> Screen {
    if caps.can_browse {
        Screen::Browser
    } else {
        Screen::Home
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

/// Build a pub/sub spec from a raw Pub/Sub-tab entry. An explicit `psub:` or
/// `pubsub:` prefix is honoured; otherwise a glob (`*`, `?`, `[`) makes it a
/// pattern (PSUBSCRIBE) and a plain name a channel (SUBSCRIBE).
fn pubsub_spec(raw: &str) -> SubSpec {
    if let Some(p) = raw.strip_prefix("psub:") {
        return SubSpec::Pattern(p.trim().to_string());
    }
    let s = raw.strip_prefix("pubsub:").unwrap_or(raw).trim();
    if s.contains(['*', '?', '[']) {
        SubSpec::Pattern(s.to_string())
    } else {
        SubSpec::Channel(s.to_string())
    }
}

/// The stream key from a raw Tail-tab entry, tolerating an explicit `stream:`
/// prefix.
fn stream_key(raw: &str) -> String {
    raw.strip_prefix("stream:")
        .unwrap_or(raw)
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests;
