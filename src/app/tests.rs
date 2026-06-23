use super::*;
use crate::broker::actor::mock;
use crate::broker::{EntryMeta, Payload, Ttl};
use crossterm::event::{KeyEventState, KeyModifiers};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{self, Receiver};

// -- harness -------------------------------------------------------------

fn build_app(
    config: Config,
    config_path: PathBuf,
    connect: Option<String>,
) -> (App, Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel::<AppEvent>(64);
    let app = App::new(
        config,
        config_path,
        std::env::temp_dir(),
        tx,
        TaskTracker::new(),
        CancellationToken::new(),
        connect,
    );
    (app, rx)
}

fn test_app() -> (App, Receiver<AppEvent>) {
    build_app(
        Config::default(),
        PathBuf::from("/nonexistent/keyhole/config.toml"),
        None,
    )
}

fn profile(name: &str) -> RedisProfile {
    RedisProfile {
        name: name.into(),
        host: "127.0.0.1".into(),
        port: 6399,
        db: 0,
        username: None,
        password: None,
        tls: false,
    }
}

fn config_with(names: &[&str]) -> Config {
    Config {
        connections: names
            .iter()
            .map(|n| ConnectionConfig::Redis(profile(n)))
            .collect(),
        ..Default::default()
    }
}

fn unique_config_path() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("keyhole-app-{}-{n}.toml", std::process::id()))
}

/// Attach a live mock-backed connection and return its id.
async fn connect(app: &mut App, id: u32, name: &str, databases: u32) -> ConnId {
    let handle = mock::handle(id, name, databases).await;
    app.handle_event(AppEvent::Connected { handle });
    ConnId(id)
}

/// Give the keyboard to a fixed anchor tab in the bottom subpanel and reconcile
/// the panel (mode + focus-scoped feeds), mirroring what Tab-cycling there does:
/// the bottom pane takes focus and lands on the given tab.
fn focus_panel(app: &mut App, tab: PanelTab) {
    let pos = app
        .active_conn()
        .unwrap()
        .panel_slots()
        .iter()
        .position(|t| *t == tab)
        .expect("panel tab present");
    let conn = app.active_conn_mut().unwrap();
    conn.panel_tab = pos;
    conn.focus = PaneFocus::Bottom;
    app.sync_panel_focus();
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn ch(c: char) -> KeyEvent {
    key(KeyCode::Char(c))
}

fn ctrl_ch(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

fn broker_event(body: &str) -> BrokerEvent {
    BrokerEvent {
        ts: OffsetDateTime::UNIX_EPOCH,
        source: "c".into(),
        payload: Payload::Utf8(body.into()),
        meta: Vec::new(),
    }
}

fn stream_entry(name: &str, vtype: ValueType) -> EntryMeta {
    EntryMeta {
        key: name.into(),
        vtype,
        ttl: Ttl::NoExpire,
        size: None,
    }
}

// -- construction --------------------------------------------------------

#[test]
fn new_selects_first_profile_when_present() {
    let (app, _rx) = build_app(config_with(&["a", "b"]), unique_config_path(), None);
    assert_eq!(app.profiles.len(), 2);
    assert_eq!(app.profile_state.selected(), Some(0));
    assert_eq!(app.screen, Screen::Connections);
    assert_eq!(app.mode, InputMode::Normal);
    assert!(app.running);
}

#[test]
fn new_selects_nothing_without_profiles() {
    let (app, _rx) = test_app();
    assert!(app.profiles.is_empty());
    assert_eq!(app.profile_state.selected(), None);
}

// -- on_start ------------------------------------------------------------

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

// -- connection lifecycle ------------------------------------------------

#[tokio::test]
async fn on_connected_activates_and_opens_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    assert_eq!(app.connections.len(), 1);
    assert_eq!(app.active, Some(0));
    assert_eq!(app.screen, Screen::Browser);
    assert!(app.is_connected("prod"));
    assert!(!app.is_connected("other"));
    // The header dot — not a transient footer message — now signals success.
    assert_eq!(app.conn_health(), ConnHealth::Connected);
    assert_eq!(app.active_conn().unwrap().label(), "prod (db0)");
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
    let id = connect(&mut app, 1, "prod", 16).await;
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
    // must not flip the header dot away from green.
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
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
    let first = connect(&mut app, 1, "a", 16).await;
    connect(&mut app, 2, "b", 16).await;
    app.handle_event(AppEvent::Disconnected {
        id: first,
        reason: "x".into(),
    });
    assert_eq!(app.conn_health(), ConnHealth::Connected);
}

#[tokio::test]
async fn page_keys_scroll_value_pane_in_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Browser;
    assert_eq!(app.active_conn().unwrap().inspector.value_scroll, 0);
    // PageDown scrolls the value pane down; repeated PageUp clamps at the top.
    app.apply(Action::PageDown);
    assert!(
        app.active_conn().unwrap().inspector.value_scroll > 0,
        "PageDown scrolls the Browser value pane"
    );
    app.apply(Action::PageUp);
    app.apply(Action::PageUp);
    assert_eq!(
        app.active_conn().unwrap().inspector.value_scroll,
        0,
        "PageUp clamps at the top"
    );
}

#[test]
fn page_keys_navigate_list_outside_browser() {
    // On non-Browser screens the page keys still page the focused list.
    let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
    assert_eq!(app.profile_state.selected(), Some(0));
    app.apply(Action::PageDown);
    assert_eq!(
        app.profile_state.selected(),
        Some(2),
        "PageDown pages the connections list (clamped to the last profile)"
    );
}

#[tokio::test]
async fn on_disconnected_removes_and_resets_to_connections() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.handle_event(AppEvent::Disconnected {
        id,
        reason: "bye".into(),
    });
    assert!(app.connections.is_empty());
    assert_eq!(app.active, None);
    assert_eq!(app.screen, Screen::Connections);
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("disconnected: bye"));
}

#[tokio::test]
async fn on_disconnected_unknown_id_is_noop() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.handle_event(AppEvent::Disconnected {
        id: ConnId(999),
        reason: "x".into(),
    });
    assert_eq!(app.connections.len(), 1);
}

#[tokio::test]
async fn on_disconnected_keeps_others_when_multiple() {
    let (mut app, _rx) = test_app();
    let first = connect(&mut app, 1, "a", 16).await;
    connect(&mut app, 2, "b", 16).await;
    app.handle_event(AppEvent::Disconnected {
        id: first,
        reason: "x".into(),
    });
    assert_eq!(app.connections.len(), 1);
    assert_eq!(app.connections[0].name, "b");
    assert_eq!(app.active, Some(0));
    assert_ne!(app.screen, Screen::Connections);
}

#[tokio::test]
async fn connect_selected_focuses_existing_connection() {
    let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Connections;
    app.profile_state.select(Some(0));
    app.apply(Action::Enter);
    assert_eq!(app.connections.len(), 1, "no duplicate connection opened");
    assert_eq!(app.active, Some(0));
    assert_eq!(app.screen, Screen::Browser);
}

// -- browse / value / stats ----------------------------------------------

#[tokio::test]
async fn keys_page_extends_and_tracks_cursor() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    // The initial (foreground) scan was kicked off on connect; its pages
    // carry the current scan epoch.
    let epoch = app.active_conn().unwrap().browser.scan_epoch;
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries: vec![stream_entry("k1", ValueType::String)],
            next_cursor: 5,
            epoch,
        },
    });
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.browser.keys.len(), 1);
    assert_eq!(conn.browser.next_cursor, 5);
    assert!(!conn.browser.complete);
    assert!(conn.browser.scanning, "scan still in progress mid-page");

    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries: vec![stream_entry("k2", ValueType::List)],
            next_cursor: 0,
            epoch,
        },
    });
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.browser.keys.len(), 2, "second page appended");
    assert!(conn.browser.complete, "cursor 0 marks the scan complete");
    assert!(!conn.browser.scanning, "scan finished");
}

#[tokio::test]
async fn keys_page_from_stale_db_is_ignored() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    let epoch = app.active_conn().unwrap().browser.scan_epoch;
    app.connections[0].db = 1;
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0, // stale: connection has since switched to db1
            entries: vec![stream_entry("k", ValueType::String)],
            next_cursor: 0,
            epoch,
        },
    });
    assert!(app.active_conn().unwrap().browser.keys.is_empty());
}

#[tokio::test]
async fn keys_page_from_superseded_scan_is_ignored() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    // A page stamped with an older epoch (e.g. from a scan abandoned when
    // the user changed the filter) must not contaminate the current scan.
    let stale_epoch = app
        .active_conn()
        .unwrap()
        .browser
        .scan_epoch
        .wrapping_sub(1);
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries: vec![stream_entry("ghost", ValueType::String)],
            next_cursor: 0,
            epoch: stale_epoch,
        },
    });
    assert!(
        app.active_conn().unwrap().browser.keys.is_empty(),
        "page from a superseded scan is dropped"
    );
}

/// Completes the connection's initial (foreground) scan with `entries`,
/// leaving the browser idle and showing those keys.
fn finish_initial_scan(app: &mut App, id: ConnId, entries: Vec<EntryMeta>) {
    let epoch = app.active_conn().unwrap().browser.scan_epoch;
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries,
            next_cursor: 0,
            epoch,
        },
    });
    assert!(app.active_conn().unwrap().browser.complete);
    assert!(!app.active_conn().unwrap().browser.scanning);
}

#[tokio::test]
async fn keys_page_builds_the_view_so_render_need_not_rebuild() {
    // The render path no longer rebuilds the view defensively; it relies on the
    // update phase keeping it current. Pin that: applying a SCAN page must leave
    // a non-empty view whenever keys were loaded.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "local", 16).await;
    finish_initial_scan(
        &mut app,
        id,
        vec![
            stream_entry("alpha", ValueType::String),
            stream_entry("beta", ValueType::String),
        ],
    );
    let conn = app.active_conn().unwrap();
    assert!(!conn.browser.keys.is_empty(), "keys loaded");
    assert!(
        !conn.browser.view.is_empty(),
        "on_keys_page rebuilds the view, so render never has to"
    );
}

#[tokio::test]
async fn first_scan_starts_with_groups_collapsed() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(
        &mut app,
        id,
        vec![
            stream_entry("user:1", ValueType::String),
            stream_entry("user:2", ValueType::String),
            stream_entry("cache:x", ValueType::String),
        ],
    );
    // Entering the browser shows only the top-level group headers; the keys are
    // folded away until a group is expanded.
    assert!(
        view_keys(&app.connections[0]).is_empty(),
        "every group starts collapsed on first load"
    );
    let groups = app.connections[0]
        .browser
        .view
        .iter()
        .filter(|r| matches!(r, ViewRow::Group { .. }))
        .count();
    assert_eq!(groups, 2, "both namespaces show as headers");

    // The user expands a group; a later (background) refresh must not re-fold it.
    app.connections[0].browser.collapsed.remove("user");
    app.connections[0].rebuild_view();
    assert_eq!(view_keys(&app.connections[0]), ["user:1", "user:2"]);
    app.start_scan(id, false);
    let epoch = app.active_conn().unwrap().browser.scan_epoch;
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries: vec![
                stream_entry("user:1", ValueType::String),
                stream_entry("user:2", ValueType::String),
                stream_entry("cache:x", ValueType::String),
            ],
            next_cursor: 0,
            epoch,
        },
    });
    assert_eq!(
        view_keys(&app.connections[0]),
        ["user:1", "user:2"],
        "a refresh leaves the user's expanded group expanded"
    );
}

#[test]
fn refresh_ticks_rounds_up_and_disables_on_zero() {
    assert_eq!(refresh_ticks(0), 0, "zero disables auto-refresh");
    assert_eq!(refresh_ticks(250), 1, "one tick");
    assert_eq!(refresh_ticks(5000), 20, "default 5s at 250ms ticks");
    assert_eq!(
        refresh_ticks(100),
        1,
        "a sub-tick interval still fires once"
    );
    assert_eq!(refresh_ticks(600), 3, "rounds up to whole ticks");
}

#[tokio::test]
async fn navigation_does_not_trigger_a_scan() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(
        &mut app,
        id,
        vec![
            stream_entry("a", ValueType::String),
            stream_entry("b", ValueType::String),
            stream_entry("c", ValueType::String),
        ],
    );
    let epoch_after_load = app.active_conn().unwrap().browser.scan_epoch;

    // Spamming up/down must only move the highlight — the whole point of the
    // change: the key set never re-fetches as a side effect of navigating.
    app.screen = Screen::Browser;
    for _ in 0..20 {
        app.apply(Action::Down);
        app.apply(Action::Up);
    }
    let conn = app.active_conn().unwrap();
    assert_eq!(
        conn.browser.scan_epoch, epoch_after_load,
        "navigation must not start a scan"
    );
    assert!(
        !conn.browser.scanning,
        "navigation must not leave a scan running"
    );
}

#[tokio::test]
async fn tick_auto_refreshes_keys_independently_of_navigation() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(&mut app, id, vec![stream_entry("a", ValueType::String)]);
    let epoch_before = app.active_conn().unwrap().browser.scan_epoch;

    // With auto-refresh due every tick and the browser on screen, one tick
    // starts a fresh background scan on its own — no key was pressed.
    app.screen = Screen::Browser;
    app.browse_refresh_ticks = 1;
    app.on_tick();
    let conn = app.active_conn().unwrap();
    assert_eq!(
        conn.browser.scan_epoch,
        epoch_before + 1,
        "the tick started a new scan"
    );
    assert!(conn.browser.scanning, "background scan in progress");
    assert!(
        !conn.browser.scan_live,
        "auto-refresh stages into scan_buf rather than clearing the list"
    );
}

#[tokio::test]
async fn auto_refresh_is_disabled_when_interval_is_zero() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(&mut app, id, vec![]);
    let epoch_before = app.active_conn().unwrap().browser.scan_epoch;

    app.screen = Screen::Browser;
    app.browse_refresh_ticks = 0; // disabled
    for _ in 0..50 {
        app.on_tick();
    }
    assert_eq!(
        app.active_conn().unwrap().browser.scan_epoch,
        epoch_before,
        "no scans when auto-refresh is disabled"
    );
}

#[tokio::test]
async fn auto_refresh_does_not_run_off_the_browser_screen() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(&mut app, id, vec![]);
    let epoch_before = app.active_conn().unwrap().browser.scan_epoch;

    // Off the Browser (e.g. on the Recordings screen): re-scanning the
    // keyspace would be pointless work, so the auto-refresh holds off until
    // the browser is up.
    app.screen = Screen::Recordings;
    app.browse_refresh_ticks = 1;
    for _ in 0..10 {
        app.on_tick();
    }
    assert_eq!(
        app.active_conn().unwrap().browser.scan_epoch,
        epoch_before,
        "no background scan while off the Browser screen"
    );
}

#[tokio::test]
async fn background_refresh_swaps_in_atomically_without_flicker() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(
        &mut app,
        id,
        vec![
            stream_entry("old1", ValueType::String),
            stream_entry("old2", ValueType::String),
        ],
    );

    // A background refresh begins, exactly as the tick would start it.
    app.screen = Screen::Browser;
    app.browse_refresh_ticks = 1;
    app.on_tick();
    let refresh_epoch = app.active_conn().unwrap().browser.scan_epoch;
    assert!(app.active_conn().unwrap().browser.scanning);

    // The first page of the refresh arrives, but the scan is not finished:
    // the visible list must stay exactly as it was — no empty frame, no
    // half-populated list flashing on screen.
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries: vec![stream_entry("new1", ValueType::String)],
            next_cursor: 42,
            epoch: refresh_epoch,
        },
    });
    {
        let visible: Vec<&str> = app
            .active_conn()
            .unwrap()
            .browser
            .keys
            .iter()
            .map(|k| k.key.as_str())
            .collect();
        assert_eq!(
            visible,
            ["old1", "old2"],
            "old keys stay visible mid-refresh"
        );
    }

    // The final page completes the scan: only now does the fresh set swap in.
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0,
            entries: vec![stream_entry("new2", ValueType::String)],
            next_cursor: 0,
            epoch: refresh_epoch,
        },
    });
    let conn = app.active_conn().unwrap();
    let visible: Vec<&str> = conn.browser.keys.iter().map(|k| k.key.as_str()).collect();
    assert_eq!(
        visible,
        ["new1", "new2"],
        "fresh set swapped in atomically on completion"
    );
    assert!(conn.browser.complete);
}

#[tokio::test]
async fn changing_filter_clears_list_and_rescans() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    finish_initial_scan(
        &mut app,
        id,
        vec![stream_entry("user:1", ValueType::String)],
    );
    let epoch_before = app.active_conn().unwrap().browser.scan_epoch;

    // A filter change is a foreground rescan: the stale result is cleared at
    // once (the keys no longer match) and a new scan generation begins.
    app.filter = "session".into();
    app.apply_filter();
    let conn = app.active_conn().unwrap();
    assert!(
        conn.browser.keys.is_empty(),
        "foreground rescan clears the previous result immediately"
    );
    assert!(conn.browser.scanning, "a fresh scan is underway");
    assert!(
        conn.browser.scan_live,
        "filter change is a live (foreground) scan"
    );
    assert_eq!(conn.browser.pattern, "*session*");
    assert!(
        conn.browser.scan_epoch > epoch_before,
        "new scan generation"
    );
}

#[tokio::test]
async fn value_loaded_only_applies_to_current_key() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.connections[0].inspector.value_key = Some("k".into());

    app.handle_event(AppEvent::ValueLoaded {
        id,
        key: "other".into(),
        value: ValueView::Missing,
    });
    assert!(
        app.active_conn().unwrap().inspector.value.is_none(),
        "mismatch ignored"
    );

    app.handle_event(AppEvent::ValueLoaded {
        id,
        key: "k".into(),
        value: ValueView::Missing,
    });
    assert!(
        app.active_conn().unwrap().inspector.value.is_some(),
        "match applied"
    );
}

#[tokio::test]
async fn stats_updated_sets_stats() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.handle_event(AppEvent::StatsUpdated {
        id,
        stats: ServerStats {
            redis_version: Some("7.4".into()),
            ..Default::default()
        },
    });
    assert_eq!(
        app.active_conn()
            .unwrap()
            .dashboard
            .stats
            .as_ref()
            .unwrap()
            .redis_version
            .as_deref(),
        Some("7.4")
    );
}

#[test]
fn conn_error_sets_error_status() {
    let (mut app, _rx) = test_app();
    app.handle_event(AppEvent::ConnError {
        id: ConnId(3),
        context: "browse".into(),
        error: "nope".into(),
    });
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("[3] browse: nope"));
}

// -- screen navigation & help --------------------------------------------

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
    assert_eq!(
        app.screen,
        Screen::Connections,
        "Browser backs out to Connections"
    );
    assert!(app.running);

    app.screen = Screen::Recordings;
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(
        app.screen,
        Screen::Connections,
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
    app.handle_key(ch('j')); // move selection — disarms
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
    assert_eq!(app.screen, Screen::Connections);
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
fn goto_browser_requires_active_connection() {
    let (mut app, _rx) = test_app();
    app.apply(Action::GotoBrowser);
    assert_eq!(
        app.screen,
        Screen::Connections,
        "GotoBrowser needs a connection"
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
async fn goto_screens_switch_with_active_connection() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    // Connecting Redis lands on the Browser; Esc steps back to the home area.
    app.apply(Action::Back);
    assert_eq!(app.screen, Screen::Connections);
    // Tab cycles the home tabs: Connections ↔ Recordings.
    app.apply(Action::NextTab);
    assert_eq!(app.screen, Screen::Recordings);
    app.apply(Action::NextTab);
    assert_eq!(app.screen, Screen::Connections);
    // `b` jumps back into the browser of the last-viewed connection.
    app.apply(Action::GotoBrowser);
    assert_eq!(app.screen, Screen::Browser);
}

// -- navigation ----------------------------------------------------------

#[test]
fn profile_navigation_moves_and_clamps() {
    let (mut app, _rx) = build_app(config_with(&["a", "b", "c"]), unique_config_path(), None);
    app.apply(Action::Down);
    assert_eq!(app.profile_state.selected(), Some(1));
    app.apply(Action::Bottom);
    assert_eq!(app.profile_state.selected(), Some(2));
    app.apply(Action::PageDown);
    assert_eq!(app.profile_state.selected(), Some(2), "clamped at the end");
    app.apply(Action::Top);
    assert_eq!(app.profile_state.selected(), Some(0));
    app.apply(Action::PageUp);
    assert_eq!(
        app.profile_state.selected(),
        Some(0),
        "clamped at the start"
    );
}

#[tokio::test]
async fn browser_navigation_updates_selected_value() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].browser.keys = vec![
        stream_entry("k0", ValueType::String),
        stream_entry("k1", ValueType::String),
        stream_entry("k2", ValueType::String),
    ];
    app.connections[0].rebuild_view();
    // Keys are always grouped, so row 0 is the "(no prefix)" header and the
    // keys follow: k0 at row 1, k1 at row 2. From k0, Down lands on k1 and
    // inspects it.
    app.connections[0].browser.table.select(Some(1));
    app.apply(Action::Down);
    assert_eq!(app.connections[0].browser.table.selected(), Some(2));
    assert_eq!(
        app.connections[0].inspector.value_key.as_deref(),
        Some("k1")
    );
}

/// The key names of the Entry rows in a connection's current view order.
fn view_keys(conn: &Connection) -> Vec<String> {
    conn.browser
        .view
        .iter()
        .filter_map(|r| match r {
            ViewRow::Entry { idx, .. } => Some(conn.browser.keys[*idx].key.clone()),
            ViewRow::Group { .. } => None,
        })
        .collect()
}

async fn browser_with_keys(keys: Vec<EntryMeta>) -> (App, Receiver<AppEvent>) {
    let (mut app, rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Browser;
    app.connections[0].browser.keys = keys;
    app.connections[0].rebuild_view();
    (app, rx)
}

#[tokio::test]
async fn browser_cycle_sort_changes_order() {
    // Default name-asc order is a, b. Sorting by type puts the string (b)
    // ahead of the hash (a), so the column choice — not the name — drives it.
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("a", ValueType::Hash),
        stream_entry("b", ValueType::String),
    ])
    .await;
    assert_eq!(view_keys(&app.connections[0]), ["a", "b"]);
    app.apply(Action::CycleSort);
    assert_eq!(app.connections[0].browser.sort.label(), "type");
    assert_eq!(view_keys(&app.connections[0]), ["b", "a"]);
}

#[tokio::test]
async fn browser_toggle_sort_direction_reverses_order() {
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("a", ValueType::String),
        stream_entry("b", ValueType::String),
    ])
    .await;
    app.apply(Action::ToggleSortDir);
    assert!(app.connections[0].browser.sort_desc);
    assert_eq!(view_keys(&app.connections[0]), ["b", "a"]);
}

#[tokio::test]
async fn browser_view_is_always_grouped_with_headers() {
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("user:1", ValueType::String),
        stream_entry("cache:x", ValueType::String),
        stream_entry("user:2", ValueType::String),
    ])
    .await;
    // Keys are always grouped by prefix — there is no ungrouped mode — so two
    // group headers (cache, user) are present from the start.
    let groups = app.connections[0]
        .browser
        .view
        .iter()
        .filter(|r| matches!(r, ViewRow::Group { .. }))
        .count();
    assert_eq!(groups, 2);
    // Rows: [cache hdr, cache:x, user hdr, user:1, user:2]. Select user:1.
    app.connections[0].browser.table.select(Some(3));
    assert_eq!(app.connections[0].selected().unwrap().key, "user:1");
    // A re-sort keeps the highlight on the same key (across the rebuild).
    app.apply(Action::CycleSort);
    assert_eq!(app.connections[0].selected().unwrap().key, "user:1");
}

#[tokio::test]
async fn browser_enter_collapses_and_expands_selected_group() {
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("user:1", ValueType::String),
        stream_entry("user:2", ValueType::String),
    ])
    .await;
    // Always grouped: the first row is the "user" group header.
    app.connections[0].browser.table.select(Some(0));
    assert_eq!(
        app.connections[0].cursor_group_prefix().as_deref(),
        Some("user")
    );

    app.apply(Action::Enter); // collapse
    assert!(app.connections[0].browser.collapsed.contains("user"));
    assert!(view_keys(&app.connections[0]).is_empty(), "keys hidden");

    app.apply(Action::Enter); // expand
    assert!(!app.connections[0].browser.collapsed.contains("user"));
    assert_eq!(view_keys(&app.connections[0]), ["user:1", "user:2"]);
}

#[tokio::test]
async fn browser_collapse_works_from_a_key_inside_the_group() {
    // Folding should act on the group the cursor is in, even when a key —
    // not the group header — is highlighted. The selection then lands on
    // the (now folded) header so the cursor stays on a visible row.
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("user:1", ValueType::String),
        stream_entry("user:2", ValueType::String),
        stream_entry("cache:x", ValueType::String),
    ])
    .await;
    // Always grouped — rows: [cache hdr, cache:x, user hdr, user:1, user:2].
    // Select user:2.
    app.connections[0].browser.table.select(Some(4));
    assert_eq!(app.connections[0].selected().unwrap().key, "user:2");

    app.apply(Action::ToggleCollapse); // Space, from inside the group
    assert!(
        app.connections[0].browser.collapsed.contains("user"),
        "the cursor's group folds even from a key row"
    );
    // The "user" keys are hidden; "cache:x" remains.
    assert_eq!(view_keys(&app.connections[0]), ["cache:x"]);
    // The highlight moved to the folded "user" header (not a stale index).
    assert_eq!(
        app.connections[0].cursor_group_prefix().as_deref(),
        Some("user")
    );

    app.apply(Action::Enter); // expand again from the header
    assert!(!app.connections[0].browser.collapsed.contains("user"));
    assert_eq!(
        view_keys(&app.connections[0]),
        ["cache:x", "user:1", "user:2"]
    );
}

#[tokio::test]
async fn browser_selecting_group_header_requests_no_value() {
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("user:1", ValueType::String),
        stream_entry("user:2", ValueType::String),
    ])
    .await;
    // Always grouped: row 0 is the "user" group header.
    app.connections[0].browser.table.select(Some(0)); // the group header
    let id = app.active_id().unwrap();
    app.request_selected_value(id);
    // A group row is not a key, so nothing is inspected.
    assert!(app.connections[0].selected().is_none());
    assert!(app.connections[0].inspector.value_key.is_none());
}

#[tokio::test]
async fn browser_toggle_all_groups_folds_and_unfolds() {
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("user:1", ValueType::String),
        stream_entry("cache:x", ValueType::String),
    ])
    .await;
    // Always grouped, so groups exist from the start.
    app.apply(Action::ToggleAllGroups); // collapse all
    assert!(view_keys(&app.connections[0]).is_empty());
    app.apply(Action::ToggleAllGroups); // expand all
    assert_eq!(view_keys(&app.connections[0]).len(), 2);
}

#[tokio::test]
async fn browser_resort_keeps_highlight_on_same_key() {
    let (mut app, _rx) = browser_with_keys(vec![
        stream_entry("a", ValueType::Hash),
        stream_entry("b", ValueType::String),
    ])
    .await;
    // Always grouped: "a" and "b" share the empty prefix, so row 0 is the
    // "(no prefix)" header and "a" (name-asc) is row 1.
    app.connections[0].browser.table.select(Some(1));
    assert_eq!(app.connections[0].selected().unwrap().key, "a");
    // Sorting by type reorders the group's keys to [b, a]; the highlight
    // follows "a", which is now the second key (row 2, after the header).
    app.apply(Action::CycleSort);
    assert_eq!(view_keys(&app.connections[0]), ["b", "a"]);
    assert_eq!(app.connections[0].selected().unwrap().key, "a");
    assert_eq!(app.connections[0].browser.table.selected(), Some(2));
}

// -- change_db -----------------------------------------------------------

#[tokio::test]
async fn change_db_clamps_to_capabilities() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 4).await; // databases 0..=3
    assert_eq!(app.screen, Screen::Browser);
    app.change_db(1);
    assert_eq!(app.connections[0].db, 1);
    app.change_db(100);
    assert_eq!(app.connections[0].db, 3, "clamped to the last database");
    app.change_db(-100);
    assert_eq!(app.connections[0].db, 0, "clamped to the first database");
}

#[tokio::test]
async fn change_db_only_acts_in_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 4).await;
    app.screen = Screen::Connections;
    app.change_db(1);
    assert_eq!(app.connections[0].db, 0, "no DB change outside the Browser");
}

// -- filter --------------------------------------------------------------

#[tokio::test]
async fn apply_filter_builds_scan_patterns() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;

    app.filter = "foo".into();
    app.apply_filter();
    assert_eq!(
        app.connections[0].browser.pattern, "*foo*",
        "plain text is wrapped"
    );

    app.filter = "a*b".into();
    app.apply_filter();
    assert_eq!(
        app.connections[0].browser.pattern, "a*b",
        "globs pass through"
    );

    app.filter = "   ".into();
    app.apply_filter();
    assert_eq!(
        app.connections[0].browser.pattern, "*",
        "blank means match-all"
    );
}

#[test]
fn filter_mode_edits_buffer() {
    let (mut app, _rx) = test_app();
    app.mode = InputMode::Filter;
    app.handle_key(ch('a'));
    app.handle_key(ch('b'));
    assert_eq!(app.filter, "ab");
    app.handle_key(key(KeyCode::Backspace));
    assert_eq!(app.filter, "a");
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(app.mode, InputMode::Normal);
}

#[test]
fn start_filter_requires_browser_with_connection() {
    let (mut app, _rx) = test_app();
    app.apply(Action::StartFilter);
    assert_eq!(
        app.mode,
        InputMode::Normal,
        "no filter without a connection"
    );
}

#[tokio::test]
async fn start_filter_enters_filter_mode() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.filter = "stale".into();
    app.apply(Action::StartFilter);
    assert_eq!(app.mode, InputMode::Filter);
    assert!(app.filter.is_empty(), "filter buffer is reset on entry");
}

// -- subscribe -----------------------------------------------------------

#[test]
fn pubsub_spec_infers_channel_or_pattern() {
    assert_eq!(pubsub_spec("news"), SubSpec::Channel("news".into()));
    assert_eq!(pubsub_spec("news.*"), SubSpec::Pattern("news.*".into()));
    assert_eq!(pubsub_spec("a?b"), SubSpec::Pattern("a?b".into()));
    // Explicit prefixes win over glob inference.
    assert_eq!(
        pubsub_spec("pubsub:plain"),
        SubSpec::Channel("plain".into())
    );
    assert_eq!(pubsub_spec("psub:foo"), SubSpec::Pattern("foo".into()));
}

#[test]
fn stream_key_strips_optional_prefix() {
    assert_eq!(stream_key("orders"), "orders");
    assert_eq!(stream_key("stream:orders"), "orders");
    assert_eq!(stream_key("  spaced  "), "spaced");
}

#[test]
fn submit_subscribe_without_connection_is_noop() {
    let (mut app, _rx) = test_app();
    app.mode = InputMode::Subscribe;
    app.subscribe_buf = "news".into();
    app.submit_subscribe();
    assert!(app.active.is_none());
    assert!(app.status.is_none());
}

#[tokio::test]
async fn pubsub_anchor_subscribes_to_typed_channel() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    focus_panel(&mut app, PanelTab::PubSub);
    // Type a channel into the always-shown prompt, then Enter.
    for c in "news".chars() {
        app.handle_key(ch(c));
    }
    assert_eq!(app.subscribe_buf, "news");
    app.handle_key(key(KeyCode::Enter));
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.subs[0].spec, SubSpec::Channel("news".into()));
    // Focus jumps to the new pub/sub tail's tab; the buffer is cleared.
    assert_eq!(conn.active_panel(), PanelTab::Sub(0));
    assert!(app.subscribe_buf.is_empty());
}

#[tokio::test]
async fn pubsub_anchor_empty_submit_is_noop() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    focus_panel(&mut app, PanelTab::PubSub);
    app.submit_subscribe();
    assert!(app.active_conn().unwrap().subs.is_empty());
}

#[tokio::test]
async fn tail_anchor_typed_key_opens_stream_tail() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].db = 2;
    focus_panel(&mut app, PanelTab::Tail);
    app.subscribe_buf = "orders".into();
    app.submit_subscribe();
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(
        conn.subs[0].spec,
        SubSpec::Stream {
            key: "orders".into(),
            db: 2
        }
    );
    assert_eq!(conn.active_panel(), PanelTab::Sub(0));
}

#[tokio::test]
async fn tail_anchor_empty_submit_tails_selected_key() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].browser.keys = vec![stream_entry("events", ValueType::Stream)];
    app.connections[0].rebuild_view();
    app.connections[0].browser.table.select(Some(1)); // row 0 is the group header
    focus_panel(&mut app, PanelTab::Tail);
    app.submit_subscribe(); // empty prompt → tail the selected stream
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.subs[0].label, "stream:events");
}

#[tokio::test]
async fn play_pause_toggles_focused_feed_view() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    assert!(
        app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .follow
    );
    app.toggle_play_pause(); // pause: freeze the view
    assert!(
        !app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .follow
    );
    app.toggle_play_pause(); // resume: follow the newest again
    assert!(
        app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .follow
    );
}

#[tokio::test]
async fn play_pause_on_an_anchor_reports_no_feed() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    focus_panel(&mut app, PanelTab::PubSub); // an input anchor, no feed
    app.toggle_play_pause();
    assert!(app.status.as_ref().unwrap().is_error);
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("no live feed"));
}

#[tokio::test]
async fn close_stream_tab_returns_focus_to_tail_anchor() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Stream {
        key: "s".into(),
        db: 0,
    });
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Sub(0));
    app.close_active_tab();
    let conn = app.active_conn().unwrap();
    assert!(conn.subs.is_empty());
    assert_eq!(conn.active_panel(), PanelTab::Tail);
}

#[tokio::test]
async fn start_subscribe_opens_tail_in_browser_panel() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    let next_sub = app.next_sub_id;
    app.start_subscribe(SubSpec::Channel("news".into()));
    // Pub/sub tails sit after the four leading anchors (Console, Monitor,
    // Keyspace, Pub/Sub), so the first one's tab is panel index 4.
    assert_eq!(app.screen, Screen::Browser);
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.active_panel(), PanelTab::Sub(0));
    assert_eq!(conn.panel_tab, 4);
    assert_eq!(
        conn.panel_subscription().map(|s| s.sub_id),
        Some(conn.subs[0].sub_id)
    );
    assert_eq!(conn.subs[0].state, SubState::Connecting);
    assert_eq!(conn.subs[0].label, "pubsub:news");
    assert_eq!(app.next_sub_id, next_sub + 1);
}

#[tokio::test]
async fn duplicate_subscribe_focuses_existing_tail() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("news".into()));
    app.start_subscribe(SubSpec::Channel("news".into()));
    assert_eq!(app.active_conn().unwrap().subs.len(), 1, "no duplicate tab");
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("already tailing"));
}

// -- realtime state transitions ------------------------------------------

#[tokio::test]
async fn realtime_event_marks_tail_active_and_buffers() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;
    app.handle_event(AppEvent::Realtime {
        id,
        sub_id,
        event: broker_event("hi"),
    });
    let sub = &app.connections[0].subs[0];
    assert_eq!(sub.state, SubState::Active);
    assert_eq!(sub.received, 1);
    assert_eq!(sub.events.len(), 1);
}

#[tokio::test]
async fn sub_started_marks_active() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;
    app.handle_event(AppEvent::SubscriptionStarted { id, sub_id });
    assert_eq!(app.connections[0].subs[0].state, SubState::Active);
}

#[tokio::test]
async fn sub_ended_marks_ended_and_stops_recording() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;
    app.handle_event(AppEvent::SubscriptionEnded {
        id,
        sub_id,
        reason: Some("source closed".into()),
    });
    let sub = &app.connections[0].subs[0];
    assert_eq!(sub.state, SubState::Ended(Some("source closed".into())));
    assert_eq!(sub.recording, RecordState::Off);
    assert!(app.status.as_ref().unwrap().message.contains("tail ended"));
}

#[tokio::test]
async fn recording_update_transitions() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;
    let path = PathBuf::from("/tmp/rec.jsonl");

    app.handle_event(AppEvent::RecordingUpdate {
        id,
        sub_id,
        status: RecordingStatus::Started { path: path.clone() },
    });
    assert!(app.connections[0].subs[0].recording.is_on());
    assert!(app.status.as_ref().unwrap().message.contains("recording →"));

    app.handle_event(AppEvent::RecordingUpdate {
        id,
        sub_id,
        status: RecordingStatus::Progress {
            records: 9,
            bytes: 123,
        },
    });
    match &app.connections[0].subs[0].recording {
        RecordState::On { records, bytes, .. } => {
            assert_eq!((*records, *bytes), (9, 123));
        }
        other => panic!("expected On, got {other:?}"),
    }

    app.handle_event(AppEvent::RecordingUpdate {
        id,
        sub_id,
        status: RecordingStatus::Stopped {
            records: 9,
            bytes: 123,
            path,
        },
    });
    assert_eq!(app.connections[0].subs[0].recording, RecordState::Off);
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("recorded 9 events"));

    app.handle_event(AppEvent::RecordingUpdate {
        id,
        sub_id,
        status: RecordingStatus::Failed {
            error: "disk full".into(),
        },
    });
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("recording failed: disk full"));
}

#[tokio::test]
async fn toggle_recording_without_tail_errors() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    // No tail open and the Console tab is active, so there is nothing to record.
    app.toggle_recording();
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("no active tail"));
}

#[tokio::test]
async fn toggle_recording_on_ended_tail_errors() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    app.connections[0].subs[0].state = SubState::Ended(None);
    app.toggle_recording();
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("tail has ended"));
}

#[tokio::test]
async fn toggle_recording_requests_start_on_active_tail() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    app.toggle_recording();
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("starting recording"));
}

#[tokio::test]
async fn close_active_tab_removes_focused_pubsub_tab() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    app.start_subscribe(SubSpec::Channel("d".into()));
    // "d" is the focused pub/sub tab (the last subscribe focused it).
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Sub(1));
    app.close_active_tab();
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.subs[0].label, "pubsub:c");
    // Focus lands back on the Pub/Sub anchor the closed tail belonged to.
    assert_eq!(conn.active_panel(), PanelTab::PubSub);
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("closed pubsub:d"));
}

#[tokio::test]
async fn close_active_tab_on_an_anchor_is_a_noop() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    // The Console anchor (and the other fixed anchors) cannot be closed.
    focus_panel(&mut app, PanelTab::Console);
    app.close_active_tab();
    assert_eq!(app.connections[0].subs.len(), 1, "tail is left untouched");
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("only pub/sub and tail tabs"));
}

#[tokio::test]
async fn tab_cycles_through_fixed_anchors_and_tails() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("a".into()));
    // Slots: 0 Console, 1 Monitor, 2 Keyspace, 3 Pub/Sub, 4 Sub(a), 5 Tail.
    assert_eq!(app.screen, Screen::Browser);
    assert_eq!(app.active_conn().unwrap().panel_tab_count(), 6);
    // The subscribe focused the pub/sub tail at slot 4.
    assert_eq!(app.connections[0].panel_tab, 4);
    app.apply(Action::NextTab); // 5 Tail
    assert_eq!(app.connections[0].panel_tab, 5);
    app.apply(Action::NextTab); // wraps to 0 Console
    assert_eq!(app.connections[0].panel_tab, 0, "wraps to the first tab");
    app.apply(Action::PrevTab); // wraps back past Console to 5 Tail
    assert_eq!(app.connections[0].panel_tab, 5, "wraps past the first tab");
}

#[tokio::test]
async fn tab_does_not_cycle_off_the_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.start_subscribe(SubSpec::Channel("a".into()));
    let before = app.connections[0].panel_tab;
    app.screen = Screen::Connections;
    app.apply(Action::NextTab);
    assert_eq!(
        app.connections[0].panel_tab, before,
        "Tab is inert off the Browser"
    );
}

// -- tail_selected_key ---------------------------------------------------

#[tokio::test]
async fn tail_selected_key_starts_stream_tail() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].browser.keys = vec![stream_entry("orders", ValueType::Stream)];
    app.connections[0].rebuild_view();
    // Always grouped: row 0 is the "(no prefix)" header, the key is row 1.
    app.connections[0].browser.table.select(Some(1));
    app.tail_selected_key();
    assert_eq!(app.screen, Screen::Browser);
    assert_eq!(app.active_conn().unwrap().subs.len(), 1);
    assert_eq!(app.active_conn().unwrap().subs[0].label, "stream:orders");
    // Stream tails sit after the Tail anchor; the new one's tab is focused.
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Sub(0));
    assert_eq!(app.active_conn().unwrap().panel_tab, 5);
}

#[tokio::test]
async fn tail_selected_key_rejects_non_stream() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].browser.keys = vec![stream_entry("greeting", ValueType::String)];
    app.connections[0].rebuild_view();
    // Always grouped: row 0 is the "(no prefix)" header, the key is row 1.
    app.connections[0].browser.table.select(Some(1));
    app.tail_selected_key();
    assert!(app.active_conn().unwrap().subs.is_empty());
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("only streams can be tailed"));
}

#[tokio::test]
async fn tail_selected_key_without_selection_errors() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].browser.keys.clear();
    app.tail_selected_key();
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("no stream key selected"));
}

// -- form ----------------------------------------------------------------

#[test]
fn add_connection_opens_form() {
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    assert!(app.form.is_some());
    assert_eq!(app.mode, InputMode::Form);
}

#[test]
fn form_typing_and_backspace_edit_focused_field() {
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    app.form.as_mut().unwrap().fields[0].clear();
    app.handle_key(ch('p'));
    app.handle_key(ch('q'));
    assert_eq!(app.form.as_ref().unwrap().fields[0], "pq");
    app.handle_key(key(KeyCode::Backspace));
    assert_eq!(app.form.as_ref().unwrap().fields[0], "p");
}

#[test]
fn form_tab_moves_focus_and_arrows_toggle_tls() {
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(app.form.as_ref().unwrap().focus, 1);
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(app.form.as_ref().unwrap().focus, 0);

    // ←/→ flip the TLS toggle when it is focused (Space no longer does — see
    // `form_space_types_into_fields_and_no_longer_toggles`).
    app.form.as_mut().unwrap().focus = ConnForm::TLS_FOCUS;
    app.handle_key(key(KeyCode::Right));
    assert!(app.form.as_ref().unwrap().tls);
    app.handle_key(key(KeyCode::Left));
    assert!(!app.form.as_ref().unwrap().tls);
}

#[test]
fn form_arrow_keys_no_longer_navigate() {
    // Up/Down duplicated Tab/Shift-Tab and were removed: only Tab moves focus.
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    assert_eq!(app.form.as_ref().unwrap().focus, 0);
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Up));
    assert_eq!(
        app.form.as_ref().unwrap().focus,
        0,
        "arrow keys are no longer bound in the form"
    );
}

#[test]
fn form_space_types_into_fields_and_no_longer_toggles() {
    // Regression: Space used to flip the TLS/Kind toggles even while typing a
    // text field. Now it types a literal space into the focused text field, and
    // the booleans flip with ←/→ only.
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);

    // On the TLS toggle, Space (and the old t/f/y/n aliases) do nothing.
    app.form.as_mut().unwrap().focus = ConnForm::TLS_FOCUS;
    for c in [' ', 't', 'f', 'y', 'n'] {
        app.handle_key(ch(c));
        assert!(!app.form.as_ref().unwrap().tls, "'{c}' must not toggle TLS");
    }
    app.handle_key(key(KeyCode::Right));
    assert!(app.form.as_ref().unwrap().tls, "←/→ toggles TLS");

    // In a text field, Space types a literal space.
    let form = app.form.as_mut().unwrap();
    form.focus = 0; // Name
    form.fields[0].clear();
    for c in "a b".chars() {
        app.handle_key(ch(c));
    }
    assert_eq!(app.form.as_ref().unwrap().fields[0], "a b");
}

#[test]
fn form_escape_cancels() {
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    app.handle_key(key(KeyCode::Esc));
    assert!(app.form.is_none());
    assert_eq!(app.mode, InputMode::Normal);
}

#[test]
fn form_validation_rejects_bad_fields() {
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    // Default form has an empty name.
    app.submit_form();
    assert!(app.form.is_some(), "form stays open on error");
    assert_eq!(
        app.form.as_ref().unwrap().error.as_deref(),
        Some("name is required")
    );

    app.form.as_mut().unwrap().fields[0] = "ok".into();
    app.form.as_mut().unwrap().fields[2] = "notaport".into();
    app.submit_form();
    assert!(app
        .form
        .as_ref()
        .unwrap()
        .error
        .as_deref()
        .unwrap()
        .contains("port"));

    app.form.as_mut().unwrap().fields[2] = "6390".into();
    app.form.as_mut().unwrap().fields[3] = "xx".into();
    app.submit_form();
    assert!(app
        .form
        .as_ref()
        .unwrap()
        .error
        .as_deref()
        .unwrap()
        .contains("db"));
}

#[tokio::test]
async fn form_submit_persists_profile_and_connects() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(Config::default(), path.clone(), None);
    app.apply(Action::AddConnection);
    {
        let form = app.form.as_mut().unwrap();
        form.fields[0] = "c1".into(); // name
        form.fields[1] = "".into(); // host -> defaults to 127.0.0.1
        form.fields[2] = "6399".into(); // port
        form.fields[3] = "2".into(); // db
        form.fields[4] = "".into(); // username
        form.fields[5] = "secret".into(); // password literal
    }
    app.submit_form();

    assert!(app.form.is_none());
    assert_eq!(app.mode, InputMode::Normal);
    assert_eq!(app.profiles.len(), 1);
    let ConnectionConfig::Redis(p) = &app.profiles[0] else {
        panic!("expected a redis profile");
    };
    assert_eq!(p.name, "c1");
    assert_eq!(p.host, "127.0.0.1", "blank host defaults");
    assert_eq!(p.port, 6399);
    assert_eq!(p.db, 2);
    assert_eq!(
        p.password.as_deref(),
        Some("prompt"),
        "a literal password is persisted as a prompt spec, never plaintext"
    );
    assert_eq!(app.next_id, 2, "a connection attempt was kicked off");

    let saved = std::fs::read_to_string(&path).expect("config written");
    assert!(saved.contains("c1"));
    assert!(!saved.contains("secret"), "the literal must not be written");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn form_submit_builds_rabbitmq_profile_with_vhost() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(Config::default(), path.clone(), None);
    app.apply(Action::AddConnection);
    {
        let form = app.form.as_mut().unwrap();
        form.toggle_kind(); // Redis -> AMQP
        form.toggle_kind(); // AMQP  -> RabbitMQ
        form.fields[0] = "rmq".into(); // name
        form.fields[1] = "rabbit.local".into(); // host
        form.fields[2] = "5672".into(); // port
        form.fields[3] = "staging".into(); // slot 3 == Vhost for RabbitMQ
        form.fields[4] = "app".into(); // username
        form.fields[5] = "".into(); // password
    }
    app.submit_form();

    assert_eq!(app.profiles.len(), 1);
    let ConnectionConfig::Rabbitmq(p) = &app.profiles[0] else {
        panic!("expected a rabbitmq profile");
    };
    assert_eq!(p.name, "rmq");
    assert_eq!(p.host, "rabbit.local");
    assert_eq!(p.port, 5672);
    assert_eq!(p.vhost, "staging", "slot 3 is read as the vhost");
    assert_eq!(p.username.as_deref(), Some("app"));

    let saved = std::fs::read_to_string(&path).expect("config written");
    assert!(saved.contains("type = \"rabbitmq\""));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn form_submit_rabbitmq_blank_vhost_defaults_to_root() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(Config::default(), path.clone(), None);
    app.apply(Action::AddConnection);
    {
        let form = app.form.as_mut().unwrap();
        form.toggle_kind(); // -> AMQP
        form.toggle_kind(); // -> RabbitMQ
        form.fields[0] = "rmq2".into();
        form.fields[3] = "   ".into(); // whitespace-only vhost
    }
    app.submit_form();

    let ConnectionConfig::Rabbitmq(p) = &app.profiles[0] else {
        panic!("expected a rabbitmq profile");
    };
    assert_eq!(p.vhost, "/", "a blank vhost defaults to /");
    let _ = std::fs::remove_file(&path);
}

// -- input mode plumbing -------------------------------------------------

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

// -- recordings ----------------------------------------------------------

#[test]
fn scan_recordings_lists_only_jsonl_newest_first() {
    let dir = std::env::temp_dir().join(format!("keyhole-scan-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.jsonl"), "x").unwrap();
    std::fs::write(dir.join("b.jsonl"), "y").unwrap();
    std::fs::write(dir.join("notes.txt"), "z").unwrap();

    let (mut app, _rx) = test_app();
    app.recordings_dir = dir.clone();
    app.scan_recordings();
    assert_eq!(app.recordings.len(), 2, "only .jsonl files are listed");
    assert!(app.recordings.iter().all(|f| f.name.ends_with(".jsonl")));
    assert!(
        app.recordings
            .windows(2)
            .all(|w| w[0].modified >= w[1].modified),
        "sorted newest first"
    );
    assert_eq!(app.recordings_state.selected(), Some(0));

    // A stale, out-of-range selection is clamped on rescan.
    app.recordings_state.select(Some(9));
    app.scan_recordings();
    assert_eq!(app.recordings_state.selected(), Some(1));

    // Emptying the directory clears the selection.
    std::fs::remove_file(dir.join("a.jsonl")).unwrap();
    std::fs::remove_file(dir.join("b.jsonl")).unwrap();
    app.scan_recordings();
    assert!(app.recordings.is_empty());
    assert_eq!(app.recordings_state.selected(), None);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A valid recording line for a record at a fixed timestamp.
fn recording_line(seq: u64, connection: &str, source: &str, payload: &str) -> String {
    format!(
        r#"{{"seq":{seq},"ts":"2026-06-19T09:08:07Z","connection":"{connection}","source":"{source}","source_type":"pubsub","encoding":"utf8","payload":"{payload}","meta":[]}}"#
    )
}

/// Switch to the Recordings tab pointed at a fresh temp dir holding `files`
/// (each a `(name, body)`), returning the dir for cleanup. Built on the real
/// Tab key path so the test exercises the home-tab switch and the scan.
fn open_recordings(app: &mut App, tag: &str, files: &[(&str, String)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("keyhole-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (name, body) in files {
        std::fs::write(dir.join(name), body).unwrap();
    }
    app.recordings_dir = dir.clone();
    app.apply(Action::NextTab); // Connections -> Recordings, scans on entry
    assert_eq!(app.screen, Screen::Recordings);
    dir
}

#[test]
fn tab_switches_between_the_connections_and_recordings_tabs() {
    let dir = std::env::temp_dir().join(format!("keyhole-rec-tab-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("a.jsonl"),
        format!("{}\n", recording_line(0, "c", "s", "x")),
    )
    .unwrap();

    let (mut app, _rx) = test_app();
    app.recordings_dir = dir.clone();
    assert_eq!(app.screen, Screen::Connections);

    // Tab moves to the Recordings tab and scans the directory; Shift-Tab back.
    app.apply(Action::NextTab);
    assert_eq!(app.screen, Screen::Recordings);
    assert_eq!(app.recordings.len(), 1, "entering the tab scans");
    app.apply(Action::PrevTab);
    assert_eq!(app.screen, Screen::Connections);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn r_renames_the_selected_recording() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-rename",
        &[(
            "old.jsonl",
            format!("{}\n", recording_line(0, "c", "s", "x")),
        )],
    );

    // `r` opens the rename editor primed with the current name.
    app.apply(Action::Refresh);
    assert_eq!(app.mode, InputMode::Rename);
    assert_eq!(app.rename_buf, "old.jsonl");

    // Replace the buffer with a new name and submit.
    app.rename_buf = "new".into();
    app.submit_rename();
    assert_eq!(app.mode, InputMode::Normal);
    // The `.jsonl` extension is appended automatically and the file moved.
    assert!(dir.join("new.jsonl").exists(), "renamed file exists");
    assert!(!dir.join("old.jsonl").exists(), "old name is gone");
    assert_eq!(app.recordings.len(), 1);
    assert_eq!(app.recordings[0].name, "new.jsonl");
    // The highlight follows the renamed file.
    assert_eq!(
        app.recording_view.as_ref().unwrap().0,
        "new.jsonl",
        "the viewer tracks the renamed file"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rename_rejects_path_separators_and_collisions() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-rename-bad",
        &[
            ("a.jsonl", format!("{}\n", recording_line(0, "c", "s", "x"))),
            ("b.jsonl", format!("{}\n", recording_line(0, "c", "s", "y"))),
        ],
    );
    // Newest-first ordering is by mtime; select a known file by name instead.
    let a_idx = app
        .recordings
        .iter()
        .position(|f| f.name == "a.jsonl")
        .unwrap();
    app.recordings_state.select(Some(a_idx));
    app.load_recording_view();

    // A path separator is refused; the file is untouched.
    app.start_rename();
    app.rename_buf = "../escape".into();
    app.submit_rename();
    assert!(
        dir.join("a.jsonl").exists(),
        "rename with a separator is refused"
    );

    // Renaming onto an existing name is refused.
    app.start_rename();
    app.rename_buf = "b".into();
    app.submit_rename();
    assert!(dir.join("a.jsonl").exists(), "a.jsonl still present");
    assert!(dir.join("b.jsonl").exists(), "b.jsonl not clobbered");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn double_d_deletes_the_selected_recording() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-delete",
        &[
            ("a.jsonl", format!("{}\n", recording_line(0, "c", "s", "x"))),
            ("b.jsonl", format!("{}\n", recording_line(0, "c", "s", "y"))),
        ],
    );
    let target = app.recordings[0].name.clone();

    // A single `d` only arms the confirmation — nothing is deleted yet.
    app.apply(Action::DeleteRecording);
    assert!(app.recordings_delete_armed, "first d arms the confirmation");
    assert_eq!(app.recordings.len(), 2, "nothing deleted on the first d");

    // A second consecutive `d` deletes and rescans.
    app.apply(Action::DeleteRecording);
    assert!(!app.recordings_delete_armed, "delete disarms after firing");
    assert!(!dir.join(&target).exists(), "the file is removed");
    assert_eq!(app.recordings.len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn intervening_input_disarms_the_recording_delete() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-delete-disarm",
        &[
            ("a.jsonl", format!("{}\n", recording_line(0, "c", "s", "x"))),
            ("b.jsonl", format!("{}\n", recording_line(0, "c", "s", "y"))),
        ],
    );

    app.apply(Action::DeleteRecording); // arm
    assert!(app.recordings_delete_armed);
    app.apply(Action::Down); // any other input disarms
    assert!(!app.recordings_delete_armed);
    app.apply(Action::DeleteRecording); // re-arms rather than deleting
    assert_eq!(app.recordings.len(), 2, "no delete after disarm");

    let _ = std::fs::remove_dir_all(&dir);
}

// -- status-bar notifications --------------------------------------------

#[test]
fn transient_notification_self_dismisses_after_its_ttl() {
    let (mut app, _rx) = test_app();
    // Toggling mouse capture posts an ordinary (transient) notification.
    app.apply(Action::ToggleMouse);
    assert_eq!(
        app.status.as_ref().expect("notification shown").kind,
        StatusKind::Transient
    );

    // Within the TTL a tick leaves it on screen.
    app.on_tick();
    assert!(app.status.is_some(), "a fresh notification survives a tick");

    // Backdate it past the TTL; the next tick expires it on its own.
    let s = app.status.as_mut().unwrap();
    s.shown_at -= time::Duration::seconds(5);
    app.on_tick();
    assert!(
        app.status.is_none(),
        "a transient notification self-dismisses once its TTL elapses"
    );
}

#[test]
fn a_newer_notification_overrides_the_previous_one() {
    let (mut app, _rx) = test_app();
    app.apply(Action::ToggleMouse);
    let first = app.status.as_ref().unwrap().message.clone();
    app.apply(Action::ToggleMouse); // toggles back: a different message
    let second = app.status.as_ref().unwrap().message.clone();
    assert_ne!(
        first, second,
        "a newer notification replaces the previous one"
    );
}

#[test]
fn breaking_the_delete_chord_clears_the_prompt_without_replacement() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-delete-prompt",
        &[
            ("a.jsonl", format!("{}\n", recording_line(0, "c", "s", "x"))),
            ("b.jsonl", format!("{}\n", recording_line(0, "c", "s", "y"))),
        ],
    );

    app.apply(Action::DeleteRecording); // arm: shows the confirmation prompt
    let status = app.status.as_ref().expect("confirm prompt shown");
    assert_eq!(status.kind, StatusKind::Confirm);
    assert!(status.message.contains("Press d again"));

    // A chord-breaking key (a navigation move, which posts no notification of
    // its own) disarms and clears the prompt at once — nothing replaces it.
    app.apply(Action::Down);
    assert!(!app.recordings_delete_armed);
    assert!(
        app.status.is_none(),
        "breaking the chord clears the confirm prompt with no replacement"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn breaking_the_quit_chord_clears_the_prompt_without_replacement() {
    let (mut app, _rx) = test_app();
    app.apply(Action::Back); // arm quit on the Connections home screen
    let status = app.status.as_ref().expect("quit confirm shown");
    assert_eq!(status.kind, StatusKind::Confirm);
    assert!(status.message.contains("Press Esc again"));

    app.apply(Action::Down); // a non-Esc key breaks the chord
    assert!(!app.quit_armed);
    assert!(
        app.status.is_none(),
        "breaking the quit chord clears the prompt with no replacement"
    );
}

#[test]
fn a_confirm_prompt_is_exempt_from_the_auto_dismiss_timer() {
    let (mut app, _rx) = test_app();
    app.apply(Action::Back); // arm quit -> confirmation prompt
    assert_eq!(app.status.as_ref().unwrap().kind, StatusKind::Confirm);

    // Even long past the transient TTL the prompt persists: it lives with the
    // armed chord, not the clock.
    let s = app.status.as_mut().unwrap();
    s.shown_at -= time::Duration::seconds(60);
    app.on_tick();
    assert!(
        app.status.is_some(),
        "a confirmation prompt does not time out"
    );
    assert!(app.quit_armed, "and the chord stays armed");
}

#[tokio::test]
async fn b_jumps_to_the_last_viewed_browser() {
    let (mut app, _rx) = test_app();
    // Two live Redis connections; the most recently focused is "two".
    connect(&mut app, 1, "one", 16).await;
    connect(&mut app, 2, "two", 16).await;
    let two = app.active_conn().unwrap().id;
    // Step back to the home area, then `b` returns to the last-viewed browser.
    app.apply(Action::Back);
    assert_eq!(app.screen, Screen::Connections);
    app.apply(Action::GotoBrowser);
    assert_eq!(app.screen, Screen::Browser);
    assert_eq!(
        app.active_conn().unwrap().id,
        two,
        "`b` lands on the last-viewed browser"
    );
}

#[tokio::test]
async fn b_works_from_the_recordings_tab() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.apply(Action::Back); // Browser -> Connections
    app.apply(Action::NextTab); // -> Recordings tab
    assert_eq!(app.screen, Screen::Recordings);
    // `b` now jumps to the browser from the Recordings tab too.
    app.apply(Action::GotoBrowser);
    assert_eq!(
        app.screen,
        Screen::Browser,
        "`b` reaches the browser from the Recordings tab"
    );
}

#[test]
fn recordings_view_loads_for_the_selected_file() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-view",
        &[(
            "only.jsonl",
            format!(
                "{}\n{}\n",
                recording_line(0, "prod", "news", "hello"),
                recording_line(1, "prod", "news", "world"),
            ),
        )],
    );

    let (name, view) = app
        .recording_view
        .as_ref()
        .expect("view loads on entering the tab");
    assert_eq!(name, &app.recordings[0].name);
    assert_eq!(view.connection.as_deref(), Some("prod"));
    assert_eq!(view.source_type.as_deref(), Some("pubsub"));
    assert_eq!(view.records.len(), 2);
    assert_eq!(view.records[0].payload, "hello");
    // Record times carry millisecond precision.
    assert_eq!(view.records[0].time, "09:08:07.000");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recordings_view_follows_the_selection_and_resets_scroll() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-nav",
        &[
            ("a.jsonl", format!("{}\n", recording_line(0, "a", "s", "p"))),
            ("b.jsonl", format!("{}\n", recording_line(0, "b", "s", "p"))),
        ],
    );
    assert_eq!(app.recordings.len(), 2);
    assert_eq!(
        app.recording_view.as_ref().unwrap().0,
        app.recordings[0].name
    );
    // Scroll the viewer, then move the selection — scroll resets to the top.
    app.scroll_recording(20);
    assert!(app.recordings_scroll > 0);
    app.apply(Action::Down);
    assert_eq!(
        app.recording_view.as_ref().unwrap().0,
        app.recordings[1].name,
        "the viewer tracks the selected recording"
    );
    assert_eq!(
        app.recordings_scroll, 0,
        "a new recording resets the scroll"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recordings_view_is_cleared_when_there_are_no_recordings() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(&mut app, "rec-none", &[]);
    assert!(app.recording_view.is_none(), "no recordings -> no view");

    let _ = std::fs::remove_dir_all(&dir);
}

// -- monitor / keyspace tails --------------------------------------------

#[tokio::test]
async fn focusing_monitor_tab_starts_a_monitor_feed() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    // No feed until the tab is focused.
    assert!(app.active_conn().unwrap().monitor_sub().is_none());
    focus_panel(&mut app, PanelTab::Monitor);
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.subs[0].spec, SubSpec::Monitor);
    assert_eq!(conn.subs[0].label, "monitor");
    // The MONITOR feed lives under its anchor, not as its own Sub tab.
    assert_eq!(conn.active_panel(), PanelTab::Monitor);
}

#[tokio::test]
async fn leaving_monitor_tab_stops_the_feed() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    focus_panel(&mut app, PanelTab::Monitor);
    assert!(app.active_conn().unwrap().monitor_sub().is_some());
    // Cycling away stops and drops the focus-scoped feed.
    focus_panel(&mut app, PanelTab::Keyspace);
    assert!(app.active_conn().unwrap().monitor_sub().is_none());
    // …and focusing Keyspace started its own feed instead.
    assert!(app.active_conn().unwrap().keyspace_sub().is_some());
}

#[tokio::test]
async fn keyspace_feed_uses_active_db_and_restarts_on_db_change() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.connections[0].db = 3;
    focus_panel(&mut app, PanelTab::Keyspace);
    assert_eq!(
        app.active_conn().unwrap().keyspace_sub().unwrap().spec,
        SubSpec::Keyspace { db: 3 }
    );
    // A db change while focused restarts the keyspace feed on the new db.
    app.change_db(1);
    assert_eq!(
        app.active_conn().unwrap().keyspace_sub().unwrap().spec,
        SubSpec::Keyspace { db: 4 }
    );
}

#[tokio::test]
async fn leaving_browser_stops_focus_feeds() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    focus_panel(&mut app, PanelTab::Monitor);
    assert!(app.active_conn().unwrap().monitor_sub().is_some());
    app.apply(Action::Back); // Browser -> Connections
    assert_eq!(app.screen, Screen::Connections);
    assert!(
        app.active_conn().unwrap().monitor_sub().is_none(),
        "the MONITOR feed stops when the panel loses focus"
    );
}

#[tokio::test]
async fn sub_notice_is_stored_on_the_tail() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
    // Focusing the Keyspace tab starts its (focus-scoped) feed; a notice for
    // it lands on that tail and is surfaced as an error status.
    focus_panel(&mut app, PanelTab::Keyspace);
    let sub_id = app.active_conn().unwrap().keyspace_sub().unwrap().sub_id;
    app.handle_event(AppEvent::SubscriptionNotice {
        id,
        sub_id,
        notice: "notifications disabled".into(),
    });
    assert_eq!(
        app.active_conn()
            .unwrap()
            .keyspace_sub()
            .unwrap()
            .notice
            .as_deref(),
        Some("notifications disabled")
    );
    assert!(app.status.as_ref().unwrap().is_error);
}

// -- AMQP / capabilities -------------------------------------------------

/// Attach a live AMQP-capability mock connection.
async fn connect_amqp(app: &mut App, id: u32, name: &str) -> ConnId {
    let handle = mock::amqp_handle(id, name).await;
    app.handle_event(AppEvent::Connected { handle });
    ConnId(id)
}

#[tokio::test]
async fn amqp_connection_stays_on_connections() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    // AMQP has no browser, and the Realtime screen was removed, so an AMQP
    // connection has no data screen yet — it stays on the Connections list.
    assert_eq!(
        app.screen,
        Screen::Connections,
        "AMQP has no data screen, so it lands on Connections"
    );
    assert_eq!(app.active_conn().unwrap().label(), "mq [amqp]");
}

#[tokio::test]
async fn amqp_capabilities_gate_redis_only_screens() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    // The Browser (and the bottom panel it carries) is Redis-only.
    app.screen = Screen::Connections;
    app.apply(Action::GotoBrowser);
    assert_eq!(
        app.screen,
        Screen::Connections,
        "GotoBrowser must be blocked"
    );
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("no key browser"));
}

#[tokio::test]
async fn amqp_cannot_open_a_tail() {
    // Realtime tails were removed for AMQP/RabbitMQ: with no panel to host
    // them, a subscribe attempt is refused rather than opening a hidden tail.
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    app.start_subscribe(SubSpec::Topic("events".into()));
    assert!(app.active_conn().unwrap().subs.is_empty());
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("not available for this broker"));
}

#[tokio::test]
async fn amqp_tick_skips_stats_refresh() {
    // A non-dashboard broker must not be pinged for stats each tick.
    let (mut app, mut rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    // Drain the connect-time events.
    while rx.try_recv().is_ok() {}
    app.connections[0].dashboard.stat_ticks = STATS_REFRESH_TICKS - 1;
    app.on_tick();
    // The mock's stats() succeeds, so a RefreshStats would surface as
    // StatsUpdated. Give the actor a moment, then assert none arrived.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let mut saw_stats = false;
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, AppEvent::StatsUpdated { .. }) {
            saw_stats = true;
        }
    }
    assert!(!saw_stats, "AMQP tick must not request stats");
}

// -- console -------------------------------------------------------------

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

    connect(&mut app, 1, "prod", 16).await;
    // Console-capable, but not on the Browser screen: still inert.
    app.screen = Screen::Connections;
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
    connect(&mut app, 1, "prod", 16).await;
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
    // Esc steps the keyboard back to the keys pane (still on the Browser); a
    // second Esc from the keys pane then leaves the Browser.
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(app.screen, Screen::Browser);
    assert!(!app.bottom_focused(), "focus returned to the keys pane");
    assert_eq!(app.mode, InputMode::Normal);
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(app.screen, Screen::Connections);
    assert_eq!(app.mode, InputMode::Normal);
}

#[tokio::test]
async fn command_result_appends_entry_and_clears_pending() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
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
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    app.handle_key(key(KeyCode::Enter)); // empty
    assert!(app.connections[0].console.history.is_empty());
    assert!(app.connections[0].console.pending.is_none());
}

#[tokio::test]
async fn clear_console_empties_output() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
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
async fn console_scroll_via_pageup_pagedown() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    // The console band scrolls with PageUp/PageDown while focused (command
    // mode); ↑↓ and Ctrl-P/N recall history.
    app.screen = Screen::Browser;
    focus_panel(&mut app, PanelTab::Console);
    app.handle_key(key(KeyCode::PageUp)); // scroll back a page
    assert_eq!(
        app.connections[0].console.scroll,
        CONSOLE_SCROLL_STEP as u16
    );
    app.handle_key(key(KeyCode::PageDown)); // toward the newest output
    assert_eq!(app.connections[0].console.scroll, 0);
    app.handle_key(key(KeyCode::PageDown)); // clamped at the bottom
    assert_eq!(app.connections[0].console.scroll, 0);
}

// -- pane focus ----------------------------------------------------------

#[tokio::test]
async fn browser_opens_with_keys_focused_so_space_folds_groups() {
    // Regression: the Browser used to open in command mode (Console is tab 0),
    // so Space typed into the console instead of folding a group. It now opens
    // with the keys pane focused.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
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

    // The group starts folded; Space on the keys pane expands it, and types
    // nothing into the console.
    app.connections[0].browser.table.select(Some(0));
    let folded = app.connections[0].browser.collapsed.len();
    assert!(folded > 0, "groups start folded");
    app.handle_key(ch(' '));
    assert!(
        app.connections[0].browser.collapsed.len() < folded,
        "Space folds/unfolds the selected group"
    );
    assert!(
        app.connections[0].console.input.is_empty(),
        "Space did not leak into the console"
    );
}

#[tokio::test]
async fn tab_focuses_bottom_then_cycles_subpanels() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Browser;
    assert!(!app.bottom_focused());

    // The first Tab drops the keyboard onto the currently shown subpanel
    // (Console) without advancing.
    app.handle_key(key(KeyCode::Tab));
    assert!(app.bottom_focused());
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Console);
    assert_eq!(app.mode, InputMode::Command);

    // Further Tabs cycle the subpanels; Shift-Tab steps back.
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Monitor);
    assert_eq!(
        app.mode,
        InputMode::Normal,
        "a feed tab is not a text prompt"
    );
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(app.active_conn().unwrap().active_panel(), PanelTab::Console);
}

#[tokio::test]
async fn ctrl_arrows_move_focus_between_panes() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Browser;
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(app.bottom_focused(), "Ctrl-↓ focuses the bottom panel");
    app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::CONTROL));
    assert!(!app.bottom_focused(), "Ctrl-↑ focuses the keys pane");
}

#[tokio::test]
async fn esc_steps_focus_back_to_keys_before_leaving() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod", 16).await;
    app.screen = Screen::Browser;
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(app.bottom_focused());
    app.handle_key(key(KeyCode::Esc));
    assert!(!app.bottom_focused(), "Esc returns focus to the keys pane");
    assert_eq!(app.screen, Screen::Browser, "still on the Browser");
    app.handle_key(key(KeyCode::Esc));
    assert_eq!(app.screen, Screen::Connections, "Esc from keys leaves");
}

#[tokio::test]
async fn console_focus_captures_space_without_folding_groups() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod", 16).await;
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
    let id = connect(&mut app, 1, "prod", 16).await;
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
        app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .follow
    );
    app.handle_key(ch('p'));
    assert!(
        !app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .follow
    );
    let folded = app.connections[0].browser.collapsed.clone();
    app.handle_key(ch(' '));
    assert_eq!(
        app.connections[0].browser.collapsed, folded,
        "Space does not fold a group from a focused feed"
    );
}

// -- mouse ---------------------------------------------------------------

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

// -- pure helpers --------------------------------------------------------

#[test]
fn move_selection_handles_edges() {
    assert_eq!(move_selection(None, 0, 1), None, "empty list");
    assert_eq!(
        move_selection(None, 3, 1),
        Some(1),
        "from unset starts at 0"
    );
    assert_eq!(move_selection(Some(0), 3, -1), Some(0), "clamped low");
    assert_eq!(move_selection(Some(2), 3, 1), Some(2), "clamped high");
    assert_eq!(move_selection(Some(1), 3, 10), Some(2));
    assert_eq!(move_selection(Some(1), 3, -10), Some(0));
}

#[test]
fn classify_password_distinguishes_specs_from_literals() {
    assert_eq!(classify_password(""), (None, None));
    assert_eq!(
        classify_password("hunter2"),
        (Some("prompt".to_string()), Some("hunter2".to_string())),
        "a literal is never persisted; a prompt spec stands in"
    );
    assert_eq!(
        classify_password("keyring"),
        (Some("keyring".to_string()), None)
    );
    assert_eq!(
        classify_password("prompt"),
        (Some("prompt".to_string()), None)
    );
    assert_eq!(
        classify_password("env:REDIS_PW"),
        (Some("env:REDIS_PW".to_string()), None)
    );
    assert_eq!(
        classify_password("keyring:prod"),
        (Some("keyring:prod".to_string()), None)
    );
}
