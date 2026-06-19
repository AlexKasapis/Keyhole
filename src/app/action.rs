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
    GotoRealtime,
    GotoRecordings,
    StartFilter,
    /// Open the subscribe prompt.
    Subscribe,
    /// Tail the selected key (Browser) as a stream.
    TailKey,
    /// Focus the previous / next tail tab (Realtime).
    PrevTab,
    NextTab,
    /// Stop the focused tail (Realtime).
    StopTail,
    DbPrev,
    DbNext,
    LoadMore,
    /// Context refresh: browse/stats, toggle recording (Realtime), rescan (Recordings).
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
        (false, Char('w')) => Some(Action::GotoRealtime),
        (false, Char('R')) => Some(Action::GotoRecordings),
        (false, Char('s')) => Some(Action::Subscribe),
        (false, Char('t')) => Some(Action::TailKey),
        (false, Char('x')) => Some(Action::StopTail),
        (false, Tab) => Some(Action::NextTab),
        (false, BackTab) => Some(Action::PrevTab),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyCode::*;

    fn plain(code: KeyCode) -> Option<Action> {
        map_key(&KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn ctrl(code: KeyCode) -> Option<Action> {
        map_key(&KeyEvent::new(code, KeyModifiers::CONTROL))
    }

    #[test]
    fn every_normal_binding_maps() {
        assert_eq!(plain(Char('q')), Some(Action::Quit));
        assert_eq!(ctrl(Char('c')), Some(Action::Quit));
        assert_eq!(plain(Char('j')), Some(Action::Down));
        assert_eq!(plain(Down), Some(Action::Down));
        assert_eq!(plain(Char('k')), Some(Action::Up));
        assert_eq!(plain(Up), Some(Action::Up));
        assert_eq!(plain(Char('g')), Some(Action::Top));
        assert_eq!(plain(Home), Some(Action::Top));
        assert_eq!(plain(Char('G')), Some(Action::Bottom));
        assert_eq!(plain(End), Some(Action::Bottom));
        assert_eq!(plain(Enter), Some(Action::Enter));
        assert_eq!(plain(Char('a')), Some(Action::AddConnection));
        assert_eq!(plain(Char('c')), Some(Action::GotoConnections));
        assert_eq!(plain(Char('b')), Some(Action::GotoBrowser));
        assert_eq!(plain(Char('d')), Some(Action::GotoDashboard));
        assert_eq!(plain(Char('w')), Some(Action::GotoRealtime));
        assert_eq!(plain(Char('R')), Some(Action::GotoRecordings));
        assert_eq!(plain(Char('s')), Some(Action::Subscribe));
        assert_eq!(plain(Char('t')), Some(Action::TailKey));
        assert_eq!(plain(Char('x')), Some(Action::StopTail));
        assert_eq!(plain(Tab), Some(Action::NextTab));
        assert_eq!(plain(BackTab), Some(Action::PrevTab));
        assert_eq!(plain(Char('/')), Some(Action::StartFilter));
        assert_eq!(plain(Char('[')), Some(Action::DbPrev));
        assert_eq!(plain(Char(']')), Some(Action::DbNext));
        assert_eq!(plain(Char('n')), Some(Action::LoadMore));
        assert_eq!(plain(Char('r')), Some(Action::Refresh));
        assert_eq!(plain(Char('?')), Some(Action::ToggleHelp));
        assert_eq!(plain(Esc), Some(Action::Dismiss));
    }

    #[test]
    fn ctrl_paging_is_distinct_from_plain_letters() {
        assert_eq!(ctrl(Char('d')), Some(Action::PageDown));
        assert_eq!(plain(Char('d')), Some(Action::GotoDashboard));
        assert_eq!(ctrl(Char('u')), Some(Action::PageUp));
        assert_eq!(plain(Char('u')), None, "plain 'u' is unbound");
    }

    #[test]
    fn unbound_keys_return_none() {
        assert_eq!(plain(Char('z')), None);
        assert_eq!(ctrl(Char('q')), None, "Ctrl-q is not a binding");
        assert_eq!(ctrl(Char('a')), None);
        assert_eq!(plain(F(1)), None);
    }
}
