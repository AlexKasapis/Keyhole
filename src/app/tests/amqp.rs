use super::*;

#[tokio::test]
async fn amqp_connection_opens_the_browser() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    // AMQP now has a (curated) destination browser, so a connection lands on the
    // Browser screen rather than the Connections list.
    assert_eq!(
        app.screen,
        Screen::Browser,
        "AMQP has a destination browser, so it opens the Browser"
    );
}

#[tokio::test]
async fn amqp_capabilities_browse_and_tail_but_no_dashboard_or_console() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    {
        let caps = &app.active_conn().unwrap().caps;
        assert!(caps.can_browse, "AMQP has a destination browser");
        assert!(caps.can_tail(), "AMQP hosts live tails");
        assert!(
            !caps.can_dashboard && !caps.can_console,
            "no dashboard or console yet (deferred to RabbitMQ)"
        );
        // The browse list is curated, not a Redis SCAN, so the scan cadence is off.
        assert!(!caps.uses_key_scan());
    }
    // GotoBrowser now succeeds for AMQP.
    app.screen = Screen::Home;
    app.apply(Action::GotoBrowser);
    assert_eq!(app.screen, Screen::Browser);
    // AMQP isn't database-scoped: it stays on db 0.
    assert_eq!(app.active_conn().unwrap().db, 0);
}

#[tokio::test]
async fn amqp_can_open_a_tail() {
    // AMQP tails are now surfaced in the UI: subscribing opens a tail tab.
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    app.start_subscribe(SubSpec::Topic("events".into()));
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1, "the tail tab was opened");
    assert_eq!(conn.subs[0].spec, SubSpec::Topic("events".into()));
    // AMQP's bottom panel is the Tail anchor plus one tab per tail (no Redis
    // anchors), and opening a tail focuses its tab.
    assert_eq!(conn.panel_slots(), vec![PanelTab::Tail, PanelTab::Sub(0)]);
    assert_eq!(conn.active_panel(), PanelTab::Sub(0));
}

#[tokio::test]
async fn amqp_add_and_remove_destination_drives_the_browser() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    // Adding a queue lands it in the curated list and selects it.
    app.add_amqp_destination(SubSpec::Queue("orders".into()));
    app.add_amqp_destination(SubSpec::Topic("events".into()));
    {
        let conn = app.active_conn().unwrap();
        assert_eq!(conn.destinations.items.len(), 2);
        assert_eq!(
            conn.selected_destination().map(|d| d.spec()),
            Some(SubSpec::Topic("events".into())),
            "the most recently added destination is selected"
        );
    }
    // Removing the selection drops it and reselects a neighbour.
    app.delete_selected_destination();
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.destinations.items.len(), 1);
    assert_eq!(
        conn.selected_destination().map(|d| d.spec()),
        Some(SubSpec::Queue("orders".into()))
    );
}

#[tokio::test]
async fn amqp_discovery_merges_dedupes_and_preserves_selection() {
    let (mut app, _path, _rx) = amqp_app_with_profile("mq", None);
    connect_amqp(&mut app, 1, "mq").await;
    // A manually-added queue, selected.
    app.add_amqp_destination(SubSpec::Queue("orders".into()));

    // Discovery returns the existing queue plus two new destinations.
    app.on_destinations_discovered(
        ConnId(1),
        Ok(vec![
            SubSpec::Queue("orders".into()),
            SubSpec::Topic("events".into()),
            SubSpec::Queue("audit".into()),
        ]),
    );

    let conn = app.active_conn().unwrap();
    assert_eq!(
        conn.destinations.items.len(),
        3,
        "the already-present queue is deduped; two new ones are appended"
    );
    assert_eq!(
        conn.selected_destination().map(|d| d.spec()),
        Some(SubSpec::Queue("orders".into())),
        "discovery preserves the existing selection"
    );
    let status = app.status.as_ref().expect("status set");
    assert!(!status.is_error);
    assert!(
        status.message.contains("3 destinations") && status.message.contains("2 new"),
        "status summarizes the merge: {}",
        status.message
    );
}

#[tokio::test]
async fn amqp_discovery_into_empty_list_selects_and_peeks_first() {
    let (mut app, _path, _rx) = amqp_app_with_profile("mq", None);
    connect_amqp(&mut app, 1, "mq").await;
    assert!(app.active_conn().unwrap().selected_destination().is_none());

    app.on_destinations_discovered(
        ConnId(1),
        Ok(vec![
            SubSpec::Queue("orders".into()),
            SubSpec::Topic("events".into()),
        ]),
    );

    let conn = app.active_conn().unwrap();
    assert_eq!(conn.destinations.items.len(), 2);
    assert_eq!(
        conn.selected_destination().map(|d| d.spec()),
        Some(SubSpec::Queue("orders".into())),
        "an empty list lands on the first discovered destination"
    );
    // The freshly-selected queue is peeked (browse mode is the default).
    assert_eq!(conn.peek.peeked, Some(SubSpec::Queue("orders".into())));
    assert!(conn.peek.pending, "selecting a queue kicks off a peek");
}

#[tokio::test]
async fn amqp_discovery_persists_new_destinations_to_config() {
    let (mut app, path, _rx) = amqp_app_with_profile("mq", None);
    connect_amqp(&mut app, 1, "mq").await;

    app.on_destinations_discovered(
        ConnId(1),
        Ok(vec![
            SubSpec::Topic("events".into()),
            SubSpec::Queue("orders".into()),
        ]),
    );

    // The discovered destinations are written back to the on-disk profile.
    let reloaded = crate::config::load(&path).expect("config reloads");
    let ConnectionConfig::Amqp(profile) = &reloaded.connections[0] else {
        panic!("expected the amqp profile");
    };
    assert_eq!(
        profile.destinations,
        vec!["topic:events", "queue:orders"],
        "discovery persists the merged list"
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn amqp_discovery_failure_reports_an_error() {
    let (mut app, _path, _rx) = amqp_app_with_profile("mq", None);
    connect_amqp(&mut app, 1, "mq").await;

    app.on_destinations_discovered(
        ConnId(1),
        Err("the management API returned HTTP 401".into()),
    );

    let status = app.status.as_ref().expect("status set");
    assert!(status.is_error, "a discovery failure is an error status");
    assert!(
        status.message.contains("Discovery failed") && status.message.contains("401"),
        "the failure reason is surfaced: {}",
        status.message
    );
    assert!(
        app.active_conn().unwrap().destinations.items.is_empty(),
        "a failed discovery adds nothing"
    );
}

#[tokio::test]
async fn amqp_discovery_without_management_url_explains_how_to_enable() {
    // The manual refresh path announces when discovery isn't configured.
    let (mut app, _path, _rx) = amqp_app_with_profile("mq", None);
    connect_amqp(&mut app, 1, "mq").await;

    app.discover_destinations(ConnId(1), true);

    let status = app.status.as_ref().expect("status set");
    assert!(status.is_error);
    assert!(
        status.message.contains("management_url"),
        "it points the user at the missing setting: {}",
        status.message
    );
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

#[tokio::test]
async fn amqp_message_pane_navigation_and_detail() {
    let (mut app, _rx) = test_app();
    amqp_with_messages(&mut app, &["one", "two", "three"]).await;
    // → steps the keyboard into the message pane.
    app.handle_key(key(KeyCode::Right));
    assert!(app.active_conn().unwrap().peek.focused);
    // ↑/↓ navigate messages and clamp at the ends.
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Down));
    assert_eq!(app.active_conn().unwrap().peek.selected, 2);
    // Enter opens the detail view for the selected message.
    app.handle_key(key(KeyCode::Enter));
    let conn = app.active_conn().unwrap();
    assert!(conn.peek.detail);
    assert_eq!(
        conn.peek.selected_event().map(|e| e.payload.as_text()),
        Some("three".into())
    );
    // Esc closes the detail view but stays in the message pane…
    app.handle_key(key(KeyCode::Esc));
    assert!(!app.active_conn().unwrap().peek.detail);
    assert!(app.active_conn().unwrap().peek.focused);
    // …and a second Esc returns to the destination list.
    app.handle_key(key(KeyCode::Esc));
    assert!(!app.active_conn().unwrap().peek.focused);
}

#[tokio::test]
async fn amqp_message_filter_narrows_and_clears() {
    let (mut app, _rx) = test_app();
    amqp_with_messages(&mut app, &["alpha", "beta", "gamma"]).await;
    app.handle_key(key(KeyCode::Right)); // focus the message pane
    app.handle_key(key(KeyCode::Down)); // selected = 1
    app.handle_key(ch('/'));
    assert_eq!(app.mode, InputMode::PeekFilter);
    for c in "mm".chars() {
        app.handle_key(ch(c));
    }
    {
        let peek = &app.active_conn().unwrap().peek;
        assert_eq!(peek.filter, "mm");
        assert_eq!(peek.filtered_len(), 1, "only gamma matches");
        assert_eq!(peek.selected, 0, "the cursor resets as the filter narrows");
    }
    // Enter commits the filter and returns to normal mode.
    app.handle_key(key(KeyCode::Enter));
    assert_eq!(app.mode, InputMode::Normal);
    assert_eq!(app.active_conn().unwrap().peek.filter, "mm");
    // '/' then Esc clears the filter.
    app.handle_key(ch('/'));
    app.handle_key(key(KeyCode::Esc));
    assert!(app.active_conn().unwrap().peek.filter.is_empty());
}

#[tokio::test]
async fn amqp_publish_prompt_confirms_and_clears() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    app.add_amqp_destination(SubSpec::Queue("orders".into()));
    // 'P' opens the publish prompt.
    app.handle_key(ch('P'));
    assert_eq!(app.mode, InputMode::Publish);
    for c in "hello".chars() {
        app.handle_key(ch(c));
    }
    assert_eq!(app.publish_buf, "hello");
    app.handle_key(key(KeyCode::Enter));
    assert_eq!(app.mode, InputMode::Normal);
    assert!(app.publish_buf.is_empty(), "buffer cleared after submit");
    let status = app.status.as_ref().unwrap();
    assert!(status.message.contains("Publishing to queue:orders"));
    assert!(!status.is_error);
}

#[tokio::test]
async fn amqp_publish_refused_without_a_destination() {
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    // No destination is selected, so the prompt is refused with an error status.
    app.handle_key(ch('P'));
    assert_eq!(app.mode, InputMode::Normal);
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("select a destination"));
}

#[tokio::test]
async fn amqp_on_published_reports_success_and_failure() {
    let (mut app, _rx) = test_app();
    let id = connect_amqp(&mut app, 1, "mq").await;
    app.handle_event(AppEvent::Published {
        id,
        target: "queue:orders".into(),
        result: Ok(()),
    });
    assert!(!app.status.as_ref().unwrap().is_error);
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("Published to"));

    app.handle_event(AppEvent::Published {
        id,
        target: "queue:orders".into(),
        result: Err("broker refused".into()),
    });
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("broker refused"));
}
