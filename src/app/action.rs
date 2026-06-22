//! Normal-mode key bindings as data. Text-entry modes (filter, connection form)
//! handle keys directly in [`crate::app::App`] rather than through this map.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// A high-level action produced by a key press in normal mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Quit,
    /// Global "back" (Esc): step out of the current screen toward Connections,
    /// and quit from Connections (Browser → Connections → close app). Also
    /// dismisses the help overlay first when it is showing.
    Back,
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
    GotoBrowser,
    GotoRecordings,
    StartFilter,
    /// Cycle the Browser's bottom panel to the previous / next tab. The tabs are
    /// a fixed set (Console, Monitor, Keyspace, Pub/Sub, Tail) plus one tab per
    /// live pub/sub or stream tail. Bound to Shift-Tab / Tab — the *only* way to
    /// move between tabs.
    PrevTab,
    NextTab,
    /// Play/pause the focused live feed (freeze or resume its view). Only acts on
    /// a live-feed subpanel (Monitor / Keyspace / a pub-sub or tail tab).
    PlayPause,
    /// Close the focused pub/sub or stream tab. The fixed tabs cannot be closed.
    CloseTab,
    DbPrev,
    DbNext,
    /// Cycle the key-list sort column (Browser).
    CycleSort,
    /// Flip the key-list sort direction (Browser).
    ToggleSortDir,
    /// Collapse/expand the selected group header (Browser).
    ToggleCollapse,
    /// Collapse or expand every group at once (Browser).
    ToggleAllGroups,
    /// Toggle recording on the focused live-feed subpanel (Browser); rescan on
    /// the Recordings screen. No longer refreshes the key list.
    Refresh,
    ToggleHelp,
}

/// Translate a key event into an [`Action`], if bound.
pub fn map_key(key: &KeyEvent) -> Option<Action> {
    use KeyCode::*;
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match (ctrl, key.code) {
        (true, Char('c')) => Some(Action::Quit),
        (false, Down | Char('j')) => Some(Action::Down),
        (false, Up | Char('k')) => Some(Action::Up),
        (true, Char('d')) => Some(Action::PageDown),
        (true, Char('u')) => Some(Action::PageUp),
        (_, PageDown) => Some(Action::PageDown),
        (_, PageUp) => Some(Action::PageUp),
        (false, Char('g') | Home) => Some(Action::Top),
        (false, Char('G') | End) => Some(Action::Bottom),
        (false, Enter) => Some(Action::Enter),
        (false, Char('a')) => Some(Action::AddConnection),
        (false, Char('b')) => Some(Action::GotoBrowser),
        (false, Char('R')) => Some(Action::GotoRecordings),
        (false, Char('p')) => Some(Action::PlayPause),
        (false, Char('x')) => Some(Action::CloseTab),
        (false, Tab) => Some(Action::NextTab),
        (false, BackTab) => Some(Action::PrevTab),
        (false, Char('/')) => Some(Action::StartFilter),
        (false, Char('[')) => Some(Action::DbPrev),
        (false, Char(']')) => Some(Action::DbNext),
        (false, Char('o')) => Some(Action::CycleSort),
        (false, Char('O')) => Some(Action::ToggleSortDir),
        (false, Char('z')) => Some(Action::ToggleAllGroups),
        (false, Char(' ')) => Some(Action::ToggleCollapse),
        (false, Char('r')) => Some(Action::Refresh),
        (false, Char('?')) => Some(Action::ToggleHelp),
        (false, Esc) => Some(Action::Back),
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
        assert_eq!(plain(Esc), Some(Action::Back));
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
        assert_eq!(plain(Char('b')), Some(Action::GotoBrowser));
        assert_eq!(plain(Char('R')), Some(Action::GotoRecordings));
        assert_eq!(plain(Char('p')), Some(Action::PlayPause));
        assert_eq!(plain(Char('x')), Some(Action::CloseTab));
        assert_eq!(plain(Tab), Some(Action::NextTab));
        assert_eq!(plain(BackTab), Some(Action::PrevTab));
        assert_eq!(plain(Char('/')), Some(Action::StartFilter));
        assert_eq!(plain(Char('[')), Some(Action::DbPrev));
        assert_eq!(plain(Char(']')), Some(Action::DbNext));
        assert_eq!(plain(Char('o')), Some(Action::CycleSort));
        assert_eq!(plain(Char('O')), Some(Action::ToggleSortDir));
        assert_eq!(plain(Char('z')), Some(Action::ToggleAllGroups));
        assert_eq!(plain(Char(' ')), Some(Action::ToggleCollapse));
        assert_eq!(plain(Char('r')), Some(Action::Refresh));
        assert_eq!(plain(Char('?')), Some(Action::ToggleHelp));
    }

    #[test]
    fn ctrl_paging_is_distinct_from_plain_letters() {
        assert_eq!(ctrl(Char('d')), Some(Action::PageDown));
        assert_eq!(
            plain(Char('d')),
            None,
            "plain 'd' is unbound: the Dashboard merged into the Browser"
        );
        assert_eq!(ctrl(Char('u')), Some(Action::PageUp));
        assert_eq!(plain(Char('u')), None, "plain 'u' is unbound");
        // The physical Page keys page too (regardless of modifiers).
        assert_eq!(plain(PageDown), Some(Action::PageDown));
        assert_eq!(plain(PageUp), Some(Action::PageUp));
    }

    #[test]
    fn unbound_keys_return_none() {
        assert_eq!(plain(Char('y')), None);
        assert_eq!(ctrl(Char('q')), None, "Ctrl-q is not a binding");
        assert_eq!(ctrl(Char('a')), None);
        assert_eq!(plain(F(1)), None);
        // `e` opened the standalone Console screen, which is gone: the console is
        // now an always-visible band in the Browser, entered with `i`.
        assert_eq!(plain(Char('e')), None, "'e' is unbound: no Console screen");
        // `:` opened the command palette, which has been removed: every action
        // is now reached directly by its own key.
        assert_eq!(plain(Char(':')), None, "':' is unbound: no command palette");
        // `w` opened the standalone Realtime screen, which is gone: tails now
        // live in the Browser's bottom panel, cycled with Tab / Shift-Tab.
        assert_eq!(plain(Char('w')), None, "'w' is unbound: no Realtime screen");
        // The former tail-management keys are gone: the panel's tabs are a fixed
        // set, each driven from within its own subpanel and reached only by
        // Tab / Shift-Tab. `s`/`m`/`K` started tails, `i` entered the console,
        // and `t` tailed the selected key — all now unbound.
        for c in ['s', 'm', 'K', 'i', 't'] {
            assert_eq!(
                plain(Char(c)),
                None,
                "'{c}' is unbound: panel tabs are fixed"
            );
        }
    }
}
