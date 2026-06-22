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
    GotoRealtime,
    GotoRecordings,
    StartFilter,
    /// Open the subscribe prompt.
    Subscribe,
    /// Start a MONITOR tail on the active connection.
    StartMonitor,
    /// Start a keyspace-notification tail on the active connection's db.
    StartKeyspace,
    /// Begin typing a command in the Browser's console band.
    ConsoleEdit,
    /// Open the command palette overlay.
    OpenPalette,
    /// Tail the selected key (Browser) as a stream.
    TailKey,
    /// Focus the previous / next tail tab (Realtime).
    PrevTab,
    NextTab,
    /// Stop the focused tail (Realtime).
    StopTail,
    DbPrev,
    DbNext,
    /// Cycle the key-list sort column (Browser).
    CycleSort,
    /// Flip the key-list sort direction (Browser).
    ToggleSortDir,
    /// Toggle namespace-prefix grouping (Browser).
    ToggleGroup,
    /// Collapse/expand the selected group header (Browser).
    ToggleCollapse,
    /// Collapse or expand every group at once (Browser).
    ToggleAllGroups,
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
        (_, PageDown) => Some(Action::PageDown),
        (_, PageUp) => Some(Action::PageUp),
        (false, Char('g') | Home) => Some(Action::Top),
        (false, Char('G') | End) => Some(Action::Bottom),
        (false, Enter) => Some(Action::Enter),
        (false, Char('a')) => Some(Action::AddConnection),
        (false, Char('c')) => Some(Action::GotoConnections),
        (false, Char('b')) => Some(Action::GotoBrowser),
        (false, Char('w')) => Some(Action::GotoRealtime),
        (false, Char('R')) => Some(Action::GotoRecordings),
        (false, Char('s')) => Some(Action::Subscribe),
        (false, Char('m')) => Some(Action::StartMonitor),
        (false, Char('K')) => Some(Action::StartKeyspace),
        (false, Char('i')) => Some(Action::ConsoleEdit),
        (false, Char(':')) => Some(Action::OpenPalette),
        (false, Char('t')) => Some(Action::TailKey),
        (false, Char('x')) => Some(Action::StopTail),
        (false, Tab) => Some(Action::NextTab),
        (false, BackTab) => Some(Action::PrevTab),
        (false, Char('/')) => Some(Action::StartFilter),
        (false, Char('[')) => Some(Action::DbPrev),
        (false, Char(']')) => Some(Action::DbNext),
        (false, Char('o')) => Some(Action::CycleSort),
        (false, Char('O')) => Some(Action::ToggleSortDir),
        (false, Char('p')) => Some(Action::ToggleGroup),
        (false, Char('z')) => Some(Action::ToggleAllGroups),
        (false, Char(' ')) => Some(Action::ToggleCollapse),
        (false, Char('r')) => Some(Action::Refresh),
        (false, Char('?')) => Some(Action::ToggleHelp),
        (false, Esc) => Some(Action::Dismiss),
        _ => None,
    }
}

/// One entry in the command palette: a human label and the action it runs.
pub struct PaletteItem {
    pub label: &'static str,
    pub action: Action,
}

/// The actions offered by the command palette (`:`), in display order.
pub const PALETTE_ITEMS: &[PaletteItem] = &[
    pal("Go to: Connections", Action::GotoConnections),
    pal("Go to: Browser", Action::GotoBrowser),
    pal("Go to: Realtime tails", Action::GotoRealtime),
    pal("Go to: Recordings", Action::GotoRecordings),
    pal("Add connection", Action::AddConnection),
    pal("Subscribe (pub/sub or stream)…", Action::Subscribe),
    pal("Monitor commands (MONITOR)", Action::StartMonitor),
    pal("Keyspace events (current db)", Action::StartKeyspace),
    pal("Browser: cycle sort column", Action::CycleSort),
    pal("Browser: toggle sort direction", Action::ToggleSortDir),
    pal("Browser: group by prefix (toggle)", Action::ToggleGroup),
    pal(
        "Browser: collapse/expand all groups",
        Action::ToggleAllGroups,
    ),
    pal("Refresh / toggle recording", Action::Refresh),
    pal("Toggle help", Action::ToggleHelp),
    pal("Quit", Action::Quit),
];

/// `const fn` constructor so the palette list can be a `const`.
const fn pal(label: &'static str, action: Action) -> PaletteItem {
    PaletteItem { label, action }
}

/// The palette items whose label contains `query` (case-insensitive substring).
pub fn palette_matches(query: &str) -> Vec<&'static PaletteItem> {
    let q = query.trim().to_ascii_lowercase();
    PALETTE_ITEMS
        .iter()
        .filter(|item| q.is_empty() || item.label.to_ascii_lowercase().contains(&q))
        .collect()
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
        assert_eq!(plain(Char('w')), Some(Action::GotoRealtime));
        assert_eq!(plain(Char('R')), Some(Action::GotoRecordings));
        assert_eq!(plain(Char('s')), Some(Action::Subscribe));
        assert_eq!(plain(Char('m')), Some(Action::StartMonitor));
        assert_eq!(plain(Char('K')), Some(Action::StartKeyspace));
        assert_eq!(plain(Char('i')), Some(Action::ConsoleEdit));
        assert_eq!(plain(Char(':')), Some(Action::OpenPalette));
        assert_eq!(plain(Char('t')), Some(Action::TailKey));
        assert_eq!(plain(Char('x')), Some(Action::StopTail));
        assert_eq!(plain(Tab), Some(Action::NextTab));
        assert_eq!(plain(BackTab), Some(Action::PrevTab));
        assert_eq!(plain(Char('/')), Some(Action::StartFilter));
        assert_eq!(plain(Char('[')), Some(Action::DbPrev));
        assert_eq!(plain(Char(']')), Some(Action::DbNext));
        assert_eq!(plain(Char('o')), Some(Action::CycleSort));
        assert_eq!(plain(Char('O')), Some(Action::ToggleSortDir));
        assert_eq!(plain(Char('p')), Some(Action::ToggleGroup));
        assert_eq!(plain(Char('z')), Some(Action::ToggleAllGroups));
        assert_eq!(plain(Char(' ')), Some(Action::ToggleCollapse));
        assert_eq!(plain(Char('r')), Some(Action::Refresh));
        assert_eq!(plain(Char('?')), Some(Action::ToggleHelp));
        assert_eq!(plain(Esc), Some(Action::Dismiss));
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
    }

    #[test]
    fn palette_filters_by_substring_case_insensitively() {
        // Empty query returns everything.
        assert_eq!(palette_matches("").len(), PALETTE_ITEMS.len());
        // Substring match is case-insensitive.
        let monitor = palette_matches("monitor");
        assert_eq!(monitor.len(), 1);
        assert_eq!(monitor[0].action, Action::StartMonitor);
        assert!(
            palette_matches("GO TO").len() >= 4,
            "all the Go to: entries (Command console was removed)"
        );
        assert!(palette_matches("zzzz").is_empty(), "no matches");
    }

    #[test]
    fn every_palette_action_is_distinct() {
        // A typo'd duplicate would make two palette rows do the same thing.
        let mut actions: Vec<Action> = PALETTE_ITEMS.iter().map(|i| i.action).collect();
        let before = actions.len();
        actions.sort_by_key(|a| format!("{a:?}"));
        actions.dedup();
        assert_eq!(actions.len(), before, "palette actions must be unique");
    }
}
