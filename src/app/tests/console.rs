use super::*;

#[tokio::test]
async fn console_tab_drives_command_mode() {
    // The Console tab's prompt is always live: focusing it (on a console-
    // capable Browser) puts the app in command mode with no extra keypress.
    let (mut app, _rx) = test_app();
    // No connection on the Browser: the panel reconcile is inert.
    app.screen = Screen::Browser;
    app.sync_panel_focus();
    assert_eq!(
        app.mode,
        InputMode::Normal,
        "no connection → no command mode"
    );

    connect(&mut app, 1, "prod").await;
    // Console-capable, but not on the Browser screen: still inert.
    app.screen = Screen::Home;
    app.sync_panel_focus();
    assert_eq!(
        app.mode,
        InputMode::Normal,
        "the console tab only lives in the Browser"
    );
    // On the Browser with the Console tab focused: command mode.
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    assert_eq!(app.mode, InputMode::Command);
}

#[tokio::test]
async fn console_mode_inert_without_console_capability() {
    // A broker with no console (AMQP), even forced onto a Browser screen,
    // must not enter command mode — the capability gate, not just the screen.
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    app.screen = Screen::Browser;
    app.sync_panel_focus();
    assert_eq!(app.mode, InputMode::Normal);
}

#[tokio::test]
async fn console_typing_and_submit_records_command() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    assert_eq!(app.mode, InputMode::Command);
    for c in "GET k".chars() {
        app.handle_key(ch(c));
    }
    assert_eq!(app.connections[0].console.input, "GET k");
    app.handle_key(key(KeyCode::Enter));
    let console = &app.connections[0].console;
    assert_eq!(console.pending.as_deref(), Some("GET k"));
    assert_eq!(console.history, vec!["GET k"]);
    assert!(console.input.is_empty(), "input cleared after submit");
    assert_eq!(app.mode, InputMode::Command, "stays in command mode");
    // Esc is the global back even with the console focused: it leaves the
    // Browser straight to the home area (no intermediate step to the keys pane).
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(
        app.screen,
        Screen::Home,
        "Esc from the console leaves the Browser"
    );
    assert_eq!(app.mode, InputMode::Normal);
}

#[tokio::test]
async fn command_result_appends_entry_and_clears_pending() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.connections[0].console.pending = Some("PING".into());
    app.handle_event(AppEvent::CommandResult {
        id,
        command: "PING".into(),
        result: Ok("PONG".into()),
    });
    let console = &app.connections[0].console;
    assert!(console.pending.is_none());
    assert_eq!(console.entries.len(), 1);
    assert_eq!(console.entries[0].output, "PONG");
    assert!(!console.entries[0].is_error);

    app.handle_event(AppEvent::CommandResult {
        id,
        command: "SET k v".into(),
        result: Err("refused".into()),
    });
    let last = app.connections[0].console.entries.last().unwrap();
    assert!(last.is_error);
    assert_eq!(last.output, "refused");
}

#[tokio::test]
async fn console_empty_submit_is_ignored() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    app.handle_key(key(KeyCode::Enter)); // empty
    assert!(app.connections[0].console.history.is_empty());
    assert!(app.connections[0].console.pending.is_none());
}

#[tokio::test]
async fn clear_console_empties_output() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.handle_event(AppEvent::CommandResult {
        id,
        command: "PING".into(),
        result: Ok("PONG".into()),
    });
    assert_eq!(app.connections[0].console.entries.len(), 1);
    // Ctrl-L clears the console while it is focused (command mode).
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL));
    assert!(app.connections[0].console.entries.is_empty());
}

#[tokio::test]
async fn console_is_not_scrollable() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    // The console band always shows the newest output — it no longer scrolls,
    // so the Page keys are inert while it is focused (command mode). ↑↓ and
    // Ctrl-P/N still recall history.
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    app.handle_key(key(KeyCode::PageUp));
    app.handle_key(key(KeyCode::PageDown));
    assert_eq!(
        app.connections[0].console.scroll, 0,
        "the console output band does not scroll"
    );
}
