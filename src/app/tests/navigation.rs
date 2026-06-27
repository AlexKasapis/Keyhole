use super::*;

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
    assert_eq!(app.screen, Screen::Home, "Browser backs out to Connections");
    assert!(app.running);

    app.screen = Screen::Recordings;
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(
        app.screen,
        Screen::Home,
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
    app.handle_key(key(KeyCode::Down)); // move selection — disarms
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
    assert_eq!(app.screen, Screen::Home);
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
fn enter_without_a_connection_is_a_noop_and_tab_reaches_recordings() {
    let (mut app, _rx) = test_app();
    // The browser is reached only by Enter on a connection (the `b` jump key was
    // removed); with no connection, Enter does nothing.
    app.apply(Action::Enter);
    assert_eq!(
        app.screen,
        Screen::Home,
        "Enter needs a connection to open a browser"
    );
    // Tab still switches to the Recordings tab even with no connection.
    app.apply(Action::NextTab);
    assert_eq!(
        app.screen,
        Screen::Recordings,
        "the Recordings tab is always reachable"
    );
}

#[tokio::test]
async fn tab_cycles_the_home_tabs() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    // Connecting Redis lands on the Browser; Esc steps back to the home area.
    app.apply(Action::Back);
    assert_eq!(app.screen, Screen::Home);
    // Tab cycles the home tabs: Connections ↔ Recordings.
    app.apply(Action::NextTab);
    assert_eq!(app.screen, Screen::Recordings);
    app.apply(Action::NextTab);
    assert_eq!(app.screen, Screen::Home);
}

#[tokio::test]
async fn enter_reopens_an_already_live_connection_browser() {
    // With the `b` jump key removed, Enter on a connection is the sole way (back)
    // into its browser — including when the connection is already live, where it
    // jumps straight in (no reconnect) with the keys pane focused.
    let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
    connect(&mut app, 1, "prod").await;
    app.apply(Action::Back); // Browser -> Connections
    assert_eq!(app.screen, Screen::Home);
    assert_eq!(
        app.profile_state.selected(),
        Some(0),
        "the live profile is selected"
    );
    app.apply(Action::Enter);
    assert_eq!(
        app.screen,
        Screen::Browser,
        "Enter re-opens the live browser"
    );
    assert!(
        !app.bottom_focused(),
        "re-enters with the keys pane focused"
    );
}

#[test]
fn profile_navigation_moves_and_clamps() {
    let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
    app.apply(Action::Down);
    assert_eq!(app.profile_state.selected(), Some(1));
    app.apply(Action::Bottom);
    assert_eq!(app.profile_state.selected(), Some(2), "clamped at the end");
    app.apply(Action::Top);
    assert_eq!(
        app.profile_state.selected(),
        Some(0),
        "clamped at the start"
    );
}

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

#[tokio::test]
async fn browser_opens_with_keys_focused_so_enter_folds_groups() {
    // Regression: the Browser used to open in command mode (Console is tab 0),
    // so a fold keystroke went to the console instead of the group. It now opens
    // with the keys pane focused, where Enter folds the selected group.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    finish_initial_scan(
        &mut app,
        id,
        vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("user:2", ValueType::String),
        ],
    );
    assert_eq!(app.screen, Screen::Browser);
    assert!(!app.bottom_focused(), "opens with the keys pane focused");
    assert_eq!(app.mode, InputMode::Normal);

    // The group starts folded; Enter on the keys pane expands it, and runs
    // nothing in the console.
    app.connections[0].browser.table.select(Some(0));
    let folded = app.connections[0].browser.collapsed.len();
    assert!(folded > 0, "groups start folded");
    app.handle_key(key(KeyCode::Enter));
    assert!(
        app.connections[0].browser.collapsed.len() < folded,
        "Enter folds/unfolds the selected group"
    );
    assert!(
        app.connections[0].console.input.is_empty(),
        "Enter did not leak into the console"
    );

    // Right no longer folds — it is unbound in the keys pane now.
    let after_enter = app.connections[0].browser.collapsed.len();
    app.handle_key(key(KeyCode::Right));
    assert_eq!(
        app.connections[0].browser.collapsed.len(),
        after_enter,
        "Right is inert: Enter is the fold key"
    );
}

#[tokio::test]
async fn tab_focuses_bottom_then_cycles_subpanels() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    assert!(!app.bottom_focused());

    // The first Tab drops the keyboard onto the bottom, landing on the Console
    // (the interactive tab) rather than the passive Server Details — so it opens
    // somewhere you can act, in command mode.
    app.handle_key(key(KeyCode::Tab));
    assert!(app.bottom_focused());
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Console);
    assert_eq!(app.mode, InputMode::Command);

    // Further Tabs cycle the subpanels; a feed tab is normal mode; Shift-Tab
    // steps back to the Console.
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Monitor);
    assert_eq!(
        app.mode,
        InputMode::Normal,
        "a feed tab is not a text prompt"
    );
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::Keyspace
    );
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Monitor);
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::Console,
        "Shift-Tab steps back to the Console"
    );
}

#[tokio::test]
async fn ctrl_down_lands_on_console_not_server_details() {
    // Regression for the landing rule: focusing the bottom from the keys pane
    // skips the passive Server Details and lands on the interactive Console.
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    assert_eq!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::ServerDetails,
        "the bottom shows Server Details while the keys pane is focused"
    );
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(app.bottom_focused());
    assert_eq!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::Console,
        "Ctrl-↓ lands on the Console, not Server Details"
    );
    // The landing is remembered: cycle to Monitor, leave, and re-enter — it
    // returns to Monitor rather than jumping back to the Console.
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Monitor);
    app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::CONTROL));
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert_eq!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::Monitor,
        "the last-used bottom tab is remembered across focus changes"
    );
}

#[tokio::test]
async fn ctrl_arrows_move_focus_between_panes() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(app.bottom_focused(), "Ctrl-↓ focuses the bottom panel");
    app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::CONTROL));
    assert!(!app.bottom_focused(), "Ctrl-↑ focuses the keys pane");
}

#[tokio::test]
async fn global_keys_reach_every_non_text_browser_pane() {
    // The point of the global layer: `:` / `?` / `m` work from every non-text
    // pane, including the feed and Server Details tabs that used to lack `:`.
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;

    // Keys pane: `:` opens the palette; Esc closes it.
    app.handle_key(ch(':'));
    assert!(
        app.palette.is_some(),
        "`:` opens the palette from the keys pane"
    );
    app.handle_key(key(KeyCode::Esc));
    assert!(app.palette.is_none());

    // A focused live-feed tab (Monitor): `:` reaches it, and `m` toggles mouse.
    focus_panel(&mut app, PanelTab::Monitor);
    app.handle_key(ch(':'));
    assert!(app.palette.is_some(), "`:` reaches a focused feed tab");
    app.handle_key(key(KeyCode::Esc));
    let mouse = app.mouse_capture();
    app.handle_key(ch('m'));
    assert_ne!(
        app.mouse_capture(),
        mouse,
        "`m` toggles mouse from a feed tab"
    );

    // The passive Server Details tab: `?` toggles help.
    focus_panel(&mut app, PanelTab::ServerDetails);
    app.handle_key(ch('?'));
    assert!(app.show_help, "`?` toggles help from Server Details");
}

#[tokio::test]
async fn global_keys_are_literal_while_typing_in_the_console() {
    // The mode gate: while the console holds the keyboard, `:` / `?` / `m` are
    // literal text, not global commands.
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    assert_eq!(app.mode, InputMode::Command);
    for c in ":?m".chars() {
        app.handle_key(ch(c));
    }
    assert!(
        app.palette.is_none(),
        "the palette did not open while typing"
    );
    assert!(!app.show_help, "help did not toggle while typing");
    assert_eq!(
        app.connections[0].console.input, ":?m",
        "the keys were typed into the console literally"
    );
}

#[tokio::test]
async fn esc_from_the_bottom_panel_leaves_the_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(app.bottom_focused());
    // Esc is the global back from any focus: a single press leaves the Browser
    // straight to the home area, without an intermediate step to the keys pane.
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(
        app.screen,
        Screen::Home,
        "Esc from the bottom panel leaves the Browser in one press"
    );
    assert_eq!(app.mode, InputMode::Normal);
}

#[tokio::test]
async fn console_focus_captures_space_without_folding_groups() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    finish_initial_scan(
        &mut app,
        id,
        vec![stream_entry("user:1", ValueType::String)],
    );
    let folded = app.connections[0].browser.collapsed.clone();
    focus_panel(&mut app, PanelTab::Console);
    for c in "a b".chars() {
        app.handle_key(ch(c));
    }
    assert_eq!(app.connections[0].console.input, "a b", "Space is typed");
    assert_eq!(
        app.connections[0].browser.collapsed, folded,
        "no group toggled while the console is focused"
    );
}

#[tokio::test]
async fn feed_focus_controls_the_feed_not_the_key_list() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    finish_initial_scan(
        &mut app,
        id,
        vec![stream_entry("user:1", ValueType::String)],
    );
    // A live tail creates and selects its own Sub tab; focus the bottom pane.
    app.start_subscribe(SubSpec::Channel("c".into()));
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(matches!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::Sub(0)
    ));

    // `p` pauses the focused feed; Space is inert on the key list.
    assert!(
        !app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .paused
    );
    app.handle_key(ch('p'));
    assert!(
        app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .paused
    );
    let folded = app.connections[0].browser.collapsed.clone();
    app.handle_key(ch(' '));
    assert_eq!(
        app.connections[0].browser.collapsed, folded,
        "Space does not fold a group from a focused feed"
    );
}

#[tokio::test]
async fn server_details_tab_scrolls_its_client_list_without_touching_feeds() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    // The leftmost tab is Server Details; focusing the bottom lands on it and
    // stays in normal mode (no text prompt, no feed controls).
    focus_panel(&mut app, PanelTab::ServerDetails);
    assert_eq!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::ServerDetails
    );
    assert_eq!(app.mode, InputMode::Normal);

    // Navigation scrolls the client list; Home/End jump to the ends.
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Down));
    assert_eq!(app.connections[0].dashboard.details_scroll, 2);
    app.handle_key(key(KeyCode::Up));
    assert_eq!(app.connections[0].dashboard.details_scroll, 1);
    app.handle_key(key(KeyCode::Home));
    assert_eq!(
        app.connections[0].dashboard.details_scroll, 0,
        "Home jumps to top"
    );
    app.handle_key(key(KeyCode::End));
    assert_eq!(
        app.connections[0].dashboard.details_scroll,
        u16::MAX,
        "End jumps to the bottom (render clamps to the list height)"
    );

    // The feed controls are inert here: there is no subscription to pause, and
    // `x`/`p` must not create or disturb one.
    app.handle_key(ch('p'));
    app.handle_key(ch('x'));
    assert!(app.connections[0].subs.is_empty(), "no feed touched");
}

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
    app.mode = InputMode::Filter;
    app.handle_mouse(MouseEventKind::ScrollDown);
    assert_eq!(
        app.profile_state.selected(),
        Some(0),
        "no navigation while typing"
    );
}

#[test]
fn m_toggles_mouse_capture() {
    let (mut app, _rx) = build_app(config_with(&["a"]), unique_config_path(), None);
    // Capture starts on (matching `tui::init`).
    assert!(app.mouse_capture());

    app.handle_key(key(KeyCode::Char('m')));
    assert!(!app.mouse_capture(), "first 'm' turns capture off");
    let status = app.status.as_ref().expect("status set");
    assert!(!status.is_error);
    assert!(
        status.message.contains("off"),
        "status reports capture is off: {}",
        status.message
    );

    app.handle_key(key(KeyCode::Char('m')));
    assert!(app.mouse_capture(), "second 'm' turns capture back on");
}

#[test]
fn m_is_literal_text_during_entry_not_a_toggle() {
    let (mut app, _rx) = build_app(config_with(&["a"]), unique_config_path(), None);
    app.mode = InputMode::Filter;
    app.handle_key(key(KeyCode::Char('m')));
    assert!(
        app.mouse_capture(),
        "'m' while typing must not toggle capture"
    );
    assert_eq!(app.filter, "m", "'m' is typed into the filter instead");
}
