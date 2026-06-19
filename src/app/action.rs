//! Normal-mode key bindings as data. Text-entry modes (filter, connection form)
//! handle keys directly in [`crate::app::App`] rather than through this map.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// A high-level action produced by a key press in normal mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Quit,
    Up,
    Down,
    PageUp,
    PageDown,
    Top,
    Bottom,
    /// Context action: connect (connections screen) / no-op elsewhere.
    Enter,
    /// Open the add-connection form.
    AddConnection,
    GotoConnections,
    GotoBrowser,
    GotoDashboard,
    StartFilter,
    DbPrev,
    DbNext,
    LoadMore,
    Refresh,
    ToggleHelp,
    Dismiss,
}

/// Translate a key event into an [`Action`], if bound.
pub fn map_key(key: &KeyEvent) -> Option<Action> {
    use KeyCode::*;
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match (ctrl, key.code) {
        (true, Char('c')) => Some(Action::Quit),
        (false, Char('q')) => Some(Action::Quit),
        (false, Down | Char('j')) => Some(Action::Down),
        (false, Up | Char('k')) => Some(Action::Up),
        (true, Char('d')) => Some(Action::PageDown),
        (true, Char('u')) => Some(Action::PageUp),
        (false, Char('g') | Home) => Some(Action::Top),
        (false, Char('G') | End) => Some(Action::Bottom),
        (false, Enter) => Some(Action::Enter),
        (false, Char('a')) => Some(Action::AddConnection),
        (false, Char('c')) => Some(Action::GotoConnections),
        (false, Char('b')) => Some(Action::GotoBrowser),
        (false, Char('d')) => Some(Action::GotoDashboard),
        (false, Char('/')) => Some(Action::StartFilter),
        (false, Char('[')) => Some(Action::DbPrev),
        (false, Char(']')) => Some(Action::DbNext),
        (false, Char('n')) => Some(Action::LoadMore),
        (false, Char('r')) => Some(Action::Refresh),
        (false, Char('?')) => Some(Action::ToggleHelp),
        (false, Esc) => Some(Action::Dismiss),
        _ => None,
    }
}
