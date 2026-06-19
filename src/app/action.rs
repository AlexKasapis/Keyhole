//! Key bindings as data. Keeping the map here (rather than scattered `match`
//! arms in the UI) lets it grow into a full per-view keymap table without
//! touching rendering code.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// A high-level action produced by a key press.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Quit the application.
    Quit,
}

/// Translate a key event into an [`Action`], if it is bound.
pub fn map_key(key: &KeyEvent) -> Option<Action> {
    match (key.modifiers, key.code) {
        (KeyModifiers::CONTROL, KeyCode::Char('c')) => Some(Action::Quit),
        (_, KeyCode::Char('q')) => Some(Action::Quit),
        _ => None,
    }
}
