use super::*;

#[tokio::test]
async fn keys_page_extends_and_tracks_cursor() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
    assert!(conn.browser.phase != ScanPhase::Complete);
    assert!(
        conn.browser.phase == ScanPhase::InProgress,
        "scan still in progress mid-page"
    );

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
    assert!(
        conn.browser.phase == ScanPhase::Complete,
        "cursor 0 marks the scan complete"
    );
    assert!(conn.browser.phase != ScanPhase::InProgress, "scan finished");
}

#[tokio::test]
async fn keys_page_from_stale_db_is_ignored() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    let epoch = app.active_conn().unwrap().browser.scan_epoch;
    app.connections[0].db = 1;
    app.handle_event(AppEvent::KeysPage {
        id,
        page: BrowsePage {
            db: 0, // stale: tagged for a different db than the connection's (1)
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
    let id = connect(&mut app, 1, "prod").await;
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

#[tokio::test]
async fn keys_page_builds_the_view_so_render_need_not_rebuild() {
    // The render path no longer rebuilds the view defensively; it relies on the
    // update phase keeping it current. Pin that: applying a SCAN page must leave
    // a non-empty view whenever keys were loaded.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "local").await;
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
    let id = connect(&mut app, 1, "prod").await;
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
    assert_eq!(refresh_ticks(33), 1, "one tick");
    assert_eq!(refresh_ticks(5000), 152, "default 5s at ~33ms ticks");
    assert_eq!(refresh_ticks(10), 1, "a sub-tick interval still fires once");
    assert_eq!(refresh_ticks(100), 4, "rounds up to whole ticks");
}

#[tokio::test]
async fn navigation_does_not_trigger_a_scan() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
        conn.browser.phase != ScanPhase::InProgress,
        "navigation must not leave a scan running"
    );
}

#[tokio::test]
async fn tick_auto_refreshes_keys_independently_of_navigation() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
    assert!(
        conn.browser.phase == ScanPhase::InProgress,
        "background scan in progress"
    );
    assert!(
        !conn.browser.scan_live,
        "auto-refresh stages into scan_buf rather than clearing the list"
    );
}

#[tokio::test]
async fn auto_refresh_is_disabled_when_interval_is_zero() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
    let id = connect(&mut app, 1, "prod").await;
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
    let id = connect(&mut app, 1, "prod").await;
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
    assert!(app.active_conn().unwrap().browser.phase == ScanPhase::InProgress);

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
    assert!(conn.browser.phase == ScanPhase::Complete);
}

#[tokio::test]
async fn changing_filter_clears_list_and_rescans() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
    assert!(
        conn.browser.phase == ScanPhase::InProgress,
        "a fresh scan is underway"
    );
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
    let id = connect(&mut app, 1, "prod").await;
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
    let id = connect(&mut app, 1, "prod").await;
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

#[tokio::test]
async fn browser_navigation_updates_selected_value() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
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

    // Enter folds the cursor's group (the binding that replaced Right) …
    app.handle_key(key(KeyCode::Enter)); // collapse
    assert!(app.connections[0].browser.collapsed.contains("user"));
    assert!(view_keys(&app.connections[0]).is_empty(), "keys hidden");

    // … and `l` still folds too, as an alias.
    app.handle_key(ch('l')); // expand
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

    app.apply(Action::ToggleGroup); // fold, from inside the group
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

    app.apply(Action::ToggleGroup); // expand again from the header
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

#[tokio::test]
async fn apply_filter_builds_scan_patterns() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;

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
    connect(&mut app, 1, "prod").await;
    app.filter = "stale".into();
    app.apply(Action::StartFilter);
    assert_eq!(app.mode, InputMode::Filter);
    assert!(app.filter.is_empty(), "filter buffer is reset on entry");
}
