use super::*;

#[test]
fn new_selects_first_profile_when_present() {
    let (app, _rx) = build_app(config_with(&["a", "b"]), unique_config_path(), None);
    assert_eq!(app.profiles.len(), 2);
    assert_eq!(app.profile_state.selected(), Some(0));
    assert_eq!(app.screen, Screen::Home);
    assert_eq!(app.mode, InputMode::Normal);
    assert!(app.running);
}

#[test]
fn new_selects_nothing_without_profiles() {
    let (app, _rx) = test_app();
    assert!(app.profiles.is_empty());
    assert_eq!(app.profile_state.selected(), None);
}

#[test]
fn on_start_unknown_profile_sets_error() {
    let (mut app, _rx) = build_app(
        config_with(&["known"]),
        unique_config_path(),
        Some("missing".into()),
    );
    app.on_start();
    let status = app.status.as_ref().expect("status set");
    assert!(status.is_error);
    assert!(status.message.contains("missing"));
}

#[tokio::test]
async fn on_start_known_profile_starts_connecting() {
    let (mut app, _rx) = build_app(
        config_with(&["known"]),
        unique_config_path(),
        Some("known".into()),
    );
    app.on_start();
    let status = app.status.as_ref().expect("status set");
    assert!(!status.is_error);
    assert!(status.message.contains("Connecting to known"));
    assert_eq!(
        app.next_id, 2,
        "an id was allocated for the connect attempt"
    );
}

#[tokio::test]
async fn on_connected_activates_and_opens_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    assert_eq!(app.connections.len(), 1);
    assert_eq!(app.active, Some(0));
    assert_eq!(app.screen, Screen::Browser);
    assert!(app.is_connected("prod"));
    assert!(!app.is_connected("other"));
    // The connection-health indicator (now in the Browser's Server band) signals
    // success rather than a transient footer message.
    assert_eq!(app.conn_health(), ConnHealth::Connected);
}

#[tokio::test]
async fn conn_health_tracks_the_connection_lifecycle() {
    // Idle on a fresh app, before anything is attempted.
    let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
    assert_eq!(app.conn_health(), ConnHealth::Offline);

    // A connect in flight reads as connecting (Enter connects the selected
    // profile on the Connections screen) …
    app.profile_state.select(Some(0));
    app.apply(Action::Enter);
    assert_eq!(app.conn_health(), ConnHealth::Connecting);

    // … flips to connected once the handle lands …
    let id = connect(&mut app, 1, "prod").await;
    assert_eq!(app.conn_health(), ConnHealth::Connected);

    // … and turns to an error when the last connection drops.
    app.handle_event(AppEvent::Disconnected {
        id,
        reason: "bye".into(),
    });
    assert_eq!(app.conn_health(), ConnHealth::Error);
}

#[tokio::test]
async fn conn_health_stays_connected_on_a_non_fatal_error() {
    // An error raised while a connection is live (e.g. a rejected command)
    // must not flip the health indicator away from connected.
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.handle_event(AppEvent::ConnError {
        id: ConnId(1),
        context: "command".into(),
        error: "nope".into(),
    });
    assert_eq!(app.conn_health(), ConnHealth::Connected);
}

#[tokio::test]
async fn conn_health_errors_when_a_connect_attempt_fails() {
    // A connect error with nothing connected (failed dial/auth) is fatal to
    // the attempt, so the dot goes red.
    let (mut app, _rx) = test_app();
    app.health = ConnHealth::Connecting;
    app.handle_event(AppEvent::ConnError {
        id: ConnId(1),
        context: "connect".into(),
        error: "refused".into(),
    });
    assert_eq!(app.conn_health(), ConnHealth::Error);
}

#[tokio::test]
async fn conn_health_keeps_green_when_one_of_several_drops() {
    // Dropping a non-active connection while others remain leaves the
    // indicator green — there is still a live, active connection.
    let (mut app, _rx) = test_app();
    let first = connect(&mut app, 1, "a").await;
    connect(&mut app, 2, "b").await;
    app.handle_event(AppEvent::Disconnected {
        id: first,
        reason: "x".into(),
    });
    assert_eq!(app.conn_health(), ConnHealth::Connected);
}

#[tokio::test]
async fn value_pane_is_not_scrollable() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Browser;
    assert_eq!(app.active_conn().unwrap().inspector.value_scroll, 0);
    // The value pane no longer scrolls: the Page keys are unbound on the keys
    // pane, so the offset never leaves the top.
    app.handle_key(key(KeyCode::PageDown));
    app.handle_key(key(KeyCode::PageUp));
    assert_eq!(
        app.active_conn().unwrap().inspector.value_scroll,
        0,
        "the Browser value pane does not scroll"
    );
}

#[test]
fn page_keys_are_inert_on_the_connections_list() {
    // The Page keys no longer page any list — only the arrows move the cursor.
    let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
    assert_eq!(app.profile_state.selected(), Some(0));
    app.handle_key(key(KeyCode::PageDown));
    assert_eq!(
        app.profile_state.selected(),
        Some(0),
        "PageDown does not move the connections selection"
    );
}

#[tokio::test]
async fn on_disconnected_removes_and_resets_to_connections() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.handle_event(AppEvent::Disconnected {
        id,
        reason: "bye".into(),
    });
    assert!(app.connections.is_empty());
    assert_eq!(app.active, None);
    assert_eq!(app.screen, Screen::Home);
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("disconnected: bye"));
}

#[tokio::test]
async fn on_disconnected_unknown_id_is_noop() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.handle_event(AppEvent::Disconnected {
        id: ConnId(999),
        reason: "x".into(),
    });
    assert_eq!(app.connections.len(), 1);
}

#[tokio::test]
async fn on_disconnected_keeps_others_when_multiple() {
    let (mut app, _rx) = test_app();
    let first = connect(&mut app, 1, "a").await;
    connect(&mut app, 2, "b").await;
    app.handle_event(AppEvent::Disconnected {
        id: first,
        reason: "x".into(),
    });
    assert_eq!(app.connections.len(), 1);
    assert_eq!(app.connections[0].name, "b");
    assert_eq!(app.active, Some(0));
    assert_ne!(app.screen, Screen::Home);
}

#[tokio::test]
async fn connect_selected_focuses_existing_connection() {
    let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.apply(Action::Enter);
    assert_eq!(app.connections.len(), 1, "no duplicate connection opened");
    assert_eq!(app.active, Some(0));
    assert_eq!(app.screen, Screen::Browser);
}
