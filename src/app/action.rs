//! Normal-mode key bindings as data. Text-entry modes (filter, connection form)
//! handle keys directly in [`crate::app::App`] rather than through this map.
//!
//! [`map_key`] is the base map used on the home screens (Connections /
//! Recordings). On the Browser, keybinds follow the focused pane: the key list
//! uses [`map_keys_focus`] (the base map minus the feed-only controls), while a
//! focused live-feed tab is driven directly (see `App::handle_feed_key`). The
//! pane-focus and tab-cycling keys (Tab / Shift-Tab / Ctrl-↑ / Ctrl-↓ / Esc) are
//! intercepted in `App::handle_browser_key` before either map is consulted.

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
    Top,
    Bottom,
    /// Context action: connect (connections screen) / no-op elsewhere.
    Enter,
    /// Open the add-connection form.
    AddConnection,
    /// Open the edit form for the selected saved connection (Connections tab),
    /// pre-filled from its profile; the form can also delete it. A no-op
    /// elsewhere.
    EditConnection,
    /// Jump to the key browser of the most recently viewed connection (falling
    /// back to the active one). Reachable from the home area's tabs.
    GotoBrowser,
    /// Delete the selected recording (Recordings tab). Needs a second
    /// consecutive press to confirm (`dd`); any other key disarms it.
    DeleteRecording,
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
    /// In the Browser, close the focused pub/sub or stream tab (the fixed tabs
    /// cannot be closed); on the Connections tab, disconnect the selected
    /// profile's live session. Both are "close the focused thing".
    CloseTab,
    /// Cycle the key-list sort column (Browser).
    CycleSort,
    /// Flip the key-list sort direction (Browser).
    ToggleSortDir,
    /// Collapse or expand every group at once (Browser).
    ToggleAllGroups,
    /// Collapse or expand the group at the cursor (Browser). Bound to Right.
    ToggleGroup,
    /// Toggle recording on the focused live-feed subpanel (Browser); rename the
    /// selected recording on the Recordings tab. No longer refreshes the key list.
    Refresh,
    ToggleHelp,
    /// Toggle terminal mouse capture. With capture on the scroll wheel scrolls;
    /// with it off the terminal's own text selection (and copy) works again.
    ToggleMouse,
    /// Open the command palette (`:`) — a small list of commands, the
    /// discoverable entry point for actions without a dedicated key.
    OpenPalette,
}

/// Translate a key event into an [`Action`], if bound.
pub fn map_key(key: &KeyEvent) -> Option<Action> {
    use KeyCode::*;
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match (ctrl, key.code) {
        (true, Char('c')) => Some(Action::Quit),
        (false, Down) => Some(Action::Down),
        (false, Up) => Some(Action::Up),
        (false, Home) => Some(Action::Top),
        (false, End) => Some(Action::Bottom),
        (false, Enter) => Some(Action::Enter),
        (false, Right | Char('l')) => Some(Action::ToggleGroup),
        (false, Char('a')) => Some(Action::AddConnection),
        (false, Char('e')) => Some(Action::EditConnection),
        (false, Char('b')) => Some(Action::GotoBrowser),
        (false, Char('d')) => Some(Action::DeleteRecording),
        (false, Char('p')) => Some(Action::PlayPause),
        (false, Char('x')) => Some(Action::CloseTab),
        (false, Tab) => Some(Action::NextTab),
        (false, BackTab) => Some(Action::PrevTab),
        (false, Char('/')) => Some(Action::StartFilter),
        (false, Char('o')) => Some(Action::CycleSort),
        (false, Char('O')) => Some(Action::ToggleSortDir),
        (false, Char('z')) => Some(Action::ToggleAllGroups),
        (false, Char('r')) => Some(Action::Refresh),
        (false, Char('m')) => Some(Action::ToggleMouse),
        (false, Char('?')) => Some(Action::ToggleHelp),
        (false, Char(':')) => Some(Action::OpenPalette),
        (false, Esc) => Some(Action::Back),
        _ => None,
    }
}

/// Key bindings while the Browser's key list is focused: the base [`map_key`]
/// minus the feed-only controls (`p` play/pause, `x` close, `r` record), which
/// act on a focused live-feed tab instead (see `App::handle_feed_key`). Keeping
/// one source of truth — the base map, filtered — avoids drift between the two.
pub fn map_keys_focus(key: &KeyEvent) -> Option<Action> {
    match map_key(key) {
        Some(Action::PlayPause | Action::CloseTab | Action::Refresh) => None,
        other => other,
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

    fn keys_focus(code: KeyCode) -> Option<Action> {
        map_keys_focus(&KeyEvent::new(code, KeyModifiers::NONE))
    }

    #[test]
    fn keys_focus_drops_feed_controls_but_keeps_list_bindings() {
        // The feed-only controls belong to a focused feed tab, not the key list.
        assert_eq!(keys_focus(Char('p')), None, "p is a feed control");
        assert_eq!(keys_focus(Char('x')), None, "x is a feed control");
        assert_eq!(keys_focus(Char('r')), None, "r records a feed");
        // Space is unbound (group folding is Right/`l`), so it never reaches
        // the console either. The other list bindings stay.
        assert_eq!(keys_focus(Char(' ')), None, "Space is unbound");
        assert_eq!(keys_focus(Char('z')), Some(Action::ToggleAllGroups));
        assert_eq!(keys_focus(Char('/')), Some(Action::StartFilter));
        assert_eq!(keys_focus(Down), Some(Action::Down));
        assert_eq!(keys_focus(Char('j')), None, "vim movement is gone");
        // Right / `l` fold the cursor's group and stay live in the keys pane.
        assert_eq!(keys_focus(Right), Some(Action::ToggleGroup));
        assert_eq!(keys_focus(Char('l')), Some(Action::ToggleGroup));
    }

    #[test]
    fn every_normal_binding_maps() {
        assert_eq!(plain(Esc), Some(Action::Back));
        assert_eq!(ctrl(Char('c')), Some(Action::Quit));
        assert_eq!(plain(Down), Some(Action::Down));
        assert_eq!(plain(Up), Some(Action::Up));
        assert_eq!(plain(Home), Some(Action::Top));
        assert_eq!(plain(End), Some(Action::Bottom));
        assert_eq!(plain(Enter), Some(Action::Enter));
        assert_eq!(plain(Right), Some(Action::ToggleGroup));
        assert_eq!(plain(Char('l')), Some(Action::ToggleGroup));
        assert_eq!(plain(Char('a')), Some(Action::AddConnection));
        assert_eq!(plain(Char('e')), Some(Action::EditConnection));
        assert_eq!(plain(Char('b')), Some(Action::GotoBrowser));
        assert_eq!(plain(Char('d')), Some(Action::DeleteRecording));
        assert_eq!(plain(Char('p')), Some(Action::PlayPause));
        assert_eq!(plain(Char('x')), Some(Action::CloseTab));
        assert_eq!(plain(Tab), Some(Action::NextTab));
        assert_eq!(plain(BackTab), Some(Action::PrevTab));
        assert_eq!(plain(Char('/')), Some(Action::StartFilter));
        assert_eq!(plain(Char('o')), Some(Action::CycleSort));
        assert_eq!(plain(Char('O')), Some(Action::ToggleSortDir));
        assert_eq!(plain(Char('z')), Some(Action::ToggleAllGroups));
        assert_eq!(plain(Char('r')), Some(Action::Refresh));
        assert_eq!(plain(Char('m')), Some(Action::ToggleMouse));
        assert_eq!(plain(Char('?')), Some(Action::ToggleHelp));
        assert_eq!(plain(Char(':')), Some(Action::OpenPalette));
    }

    #[test]
    fn colon_opens_palette_from_home_and_the_keys_pane() {
        // `:` is the command-palette key on the home screens and, via the
        // filtered keys-pane map, in the Browser's key list too.
        assert_eq!(plain(Char(':')), Some(Action::OpenPalette));
        assert_eq!(keys_focus(Char(':')), Some(Action::OpenPalette));
    }

    #[test]
    fn page_keys_and_ctrl_d_u_are_unbound() {
        // Paging is gone — no pane scrolls — so the Page keys and Ctrl-d/Ctrl-u
        // no longer map to anything, while plain 'd' still deletes a recording.
        assert_eq!(ctrl(Char('d')), None, "Ctrl-d no longer pages");
        assert_eq!(
            plain(Char('d')),
            Some(Action::DeleteRecording),
            "plain 'd' deletes a recording"
        );
        assert_eq!(ctrl(Char('u')), None, "Ctrl-u no longer pages");
        assert_eq!(plain(Char('u')), None, "plain 'u' is unbound");
        // The physical Page keys are unbound now too.
        assert_eq!(plain(PageDown), None, "PageDown no longer pages");
        assert_eq!(plain(PageUp), None, "PageUp no longer pages");
    }

    #[test]
    fn unbound_keys_return_none() {
        assert_eq!(plain(Char('y')), None);
        assert_eq!(ctrl(Char('q')), None, "Ctrl-q is not a binding");
        assert_eq!(ctrl(Char('a')), None);
        assert_eq!(plain(F(1)), None);
        // `[` / `]` once stepped the Redis database; that switcher is gone.
        assert_eq!(plain(Char('[')), None, "'[' is unbound: no DB switcher");
        assert_eq!(plain(Char(']')), None, "']' is unbound: no DB switcher");
        // Space once folded the selected group; folding is now Right / `l`.
        assert_eq!(plain(Char(' ')), None, "Space is unbound: fold with Right");
        // The vim-style movement keys are gone: arrows / Home / End are the only
        // navigation now (the Page keys no longer page — nothing scrolls).
        for c in ['j', 'k', 'g', 'G'] {
            assert_eq!(plain(Char(c)), None, "'{c}' is unbound: no vim movement");
        }
        // `:` opens the command palette (see `colon_opens_palette_*`), so it is
        // deliberately *not* unbound here.
        // `w` opened the standalone Realtime screen, which is gone: tails now
        // live in the Browser's bottom panel, cycled with Tab / Shift-Tab.
        assert_eq!(plain(Char('w')), None, "'w' is unbound: no Realtime screen");
        // The former tail-management keys are gone: the panel's tabs are a fixed
        // set, each driven from within its own subpanel and reached only by
        // Tab / Shift-Tab. `s`/`K` started tails, `i` entered the console, and
        // `t` tailed the selected key — all now unbound. (`m`, once a tail key,
        // is now the mouse-capture toggle — see `every_normal_binding_maps`.)
        for c in ['s', 'K', 'i', 't'] {
            assert_eq!(
                plain(Char(c)),
                None,
                "'{c}' is unbound: panel tabs are fixed"
            );
        }
    }
}
