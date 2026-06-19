//! UI-facing application state types owned by [`crate::app::App`].

use ratatui::widgets::TableState;

use crate::broker::actor::ConnHandle;
use crate::broker::{Capabilities, ConnId, EntryMeta, ServerStats, ValueView};

/// Which top-level screen is showing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Connections,
    Browser,
    Dashboard,
}

/// Keyboard input mode (text-entry modes capture raw keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Filter,
    Form,
}

/// A transient status-bar message.
pub struct Status {
    pub message: String,
    pub is_error: bool,
}

/// An open connection plus its per-connection browse/inspect/dashboard state.
pub struct Connection {
    pub id: ConnId,
    pub name: String,
    pub caps: Capabilities,
    pub db: u32,
    pub pattern: String,
    pub keys: Vec<EntryMeta>,
    pub next_cursor: u64,
    pub complete: bool,
    pub table: TableState,
    pub value: Option<ValueView>,
    pub value_key: Option<String>,
    pub value_scroll: u16,
    pub stats: Option<ServerStats>,
    pub stat_ticks: u32,
    pub handle: ConnHandle,
}

impl Connection {
    pub fn new(handle: ConnHandle) -> Self {
        let mut table = TableState::default();
        table.select(Some(0));
        Self {
            id: handle.id,
            name: handle.name.clone(),
            caps: handle.caps.clone(),
            db: 0,
            pattern: "*".to_string(),
            keys: Vec::new(),
            next_cursor: 0,
            complete: false,
            table,
            value: None,
            value_key: None,
            value_scroll: 0,
            stats: None,
            stat_ticks: 0,
            handle,
        }
    }

    /// The currently highlighted key, if any.
    pub fn selected(&self) -> Option<&EntryMeta> {
        self.table.selected().and_then(|i| self.keys.get(i))
    }

    /// A short `name (dbN)` label for the status bar.
    pub fn label(&self) -> String {
        format!("{} (db{})", self.name, self.db)
    }
}

/// The add-connection modal. Fields are plain strings edited in place; the
/// password field accepts a *spec* (`env:VAR`, `keyring`, `prompt`) or a literal
/// (used for the session only, never persisted in plaintext).
pub struct ConnForm {
    pub fields: [String; ConnForm::FIELD_COUNT],
    pub tls: bool,
    pub focus: usize,
    pub error: Option<String>,
}

impl ConnForm {
    pub const FIELD_COUNT: usize = 6;
    /// Index of the synthetic "TLS toggle" focus position.
    pub const TLS_FOCUS: usize = Self::FIELD_COUNT;
    /// Total number of focusable positions (fields + TLS toggle).
    pub const FOCUS_COUNT: usize = Self::FIELD_COUNT + 1;

    pub const LABELS: [&'static str; Self::FIELD_COUNT] =
        ["Name", "Host", "Port", "DB", "Username", "Password"];

    pub fn new() -> Self {
        Self {
            fields: [
                String::new(),
                "127.0.0.1".to_string(),
                "6379".to_string(),
                "0".to_string(),
                String::new(),
                String::new(),
            ],
            tls: false,
            focus: 0,
            error: None,
        }
    }

    pub fn focus_next(&mut self) {
        self.focus = (self.focus + 1) % Self::FOCUS_COUNT;
    }

    pub fn focus_prev(&mut self) {
        self.focus = (self.focus + Self::FOCUS_COUNT - 1) % Self::FOCUS_COUNT;
    }
}

impl Default for ConnForm {
    fn default() -> Self {
        Self::new()
    }
}
