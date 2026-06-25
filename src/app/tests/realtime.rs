use super::*;

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
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
    focus_panel(&mut app, PanelTab::PubSub);
    app.submit_subscribe();
    assert!(app.active_conn().unwrap().subs.is_empty());
}

#[tokio::test]
async fn tail_anchor_typed_key_opens_stream_tail() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    assert!(
        !app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .paused
    );
    app.toggle_play_pause(); // pause: stop tracking events
    assert!(
        app.active_conn()
            .unwrap()
            .panel_subscription()
            .unwrap()
            .paused
    );
    app.toggle_play_pause(); // resume: track and follow the newest again
    let sub = app.active_conn().unwrap();
    let sub = sub.panel_subscription().unwrap();
    assert!(!sub.paused);
    assert!(sub.follow);
    assert_eq!(sub.offset, 0);
}

#[tokio::test]
async fn play_pause_on_an_anchor_reports_no_feed() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
    let next_sub = app.next_sub_id;
    app.start_subscribe(SubSpec::Channel("news".into()));
    // Pub/sub tails sit after the five leading anchors (Server Details, Console,
    // Monitor, Keyspace, Pub/Sub), so the first one's tab is panel index 5.
    assert_eq!(app.screen, Screen::Browser);
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.active_panel(), PanelTab::Sub(0));
    assert_eq!(conn.panel_tab, 5);
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
    connect(&mut app, 1, "prod").await;
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

#[tokio::test]
async fn realtime_event_marks_tail_active_and_buffers() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
async fn paused_feed_marks_active_but_drops_events() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;
    app.toggle_play_pause(); // pause the focused feed
    app.handle_event(AppEvent::Realtime {
        id,
        sub_id,
        event: broker_event("dropped"),
    });
    let sub = &app.connections[0].subs[0];
    // The event still confirms the tail is live, but is not tracked.
    assert_eq!(sub.state, SubState::Active);
    assert_eq!(sub.received, 0);
    assert!(sub.events.is_empty());

    // Resuming begins tracking again.
    app.toggle_play_pause();
    app.handle_event(AppEvent::Realtime {
        id,
        sub_id,
        event: broker_event("kept"),
    });
    let sub = &app.connections[0].subs[0];
    assert_eq!(sub.received, 1);
    assert_eq!(sub.events.len(), 1);
}

#[tokio::test]
async fn drain_events_applies_a_whole_burst_in_one_pass() {
    // The render loop drains all queued events before redrawing, so a
    // high-rate feed folds into a single frame. Every queued event must land.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;

    let (tx, mut rx) = mpsc::channel::<AppEvent>(64);
    for i in 0..10 {
        tx.try_send(AppEvent::Realtime {
            id,
            sub_id,
            event: broker_event(&format!("e{i}")),
        })
        .unwrap();
    }
    app.drain_events(&mut rx);

    let sub = &app.connections[0].subs[0];
    assert_eq!(sub.received, 10, "every queued event applied in one drain");
    assert_eq!(sub.events.len(), 10);
    assert!(
        rx.try_recv().is_err(),
        "the queue is fully drained, ready for the next blocking recv"
    );
}

#[tokio::test]
async fn drain_events_on_empty_queue_is_a_noop() {
    // Draining an empty (but open) channel must return at once without blocking
    // and without disturbing state.
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let (_tx, mut rx) = mpsc::channel::<AppEvent>(4);
    app.drain_events(&mut rx);
    assert_eq!(app.connections[0].subs[0].received, 0);
    assert!(app.running);
}

#[tokio::test]
async fn drain_events_stops_at_a_quit_event() {
    // A quit handled mid-burst stops the drain immediately so the loop exits
    // promptly instead of first chewing through a firehose backlog.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;

    let (tx, mut rx) = mpsc::channel::<AppEvent>(8);
    tx.try_send(AppEvent::Realtime {
        id,
        sub_id,
        event: broker_event("before"),
    })
    .unwrap();
    // Ctrl-c is the hard quit from any screen.
    tx.try_send(AppEvent::Input(Event::Key(ctrl_ch('c'))))
        .unwrap();
    tx.try_send(AppEvent::Realtime {
        id,
        sub_id,
        event: broker_event("after"),
    })
    .unwrap();
    app.drain_events(&mut rx);

    assert!(!app.running, "the quit event was handled");
    assert_eq!(
        app.connections[0].subs[0].received, 1,
        "draining stops at the quit; the trailing event is left in the queue"
    );
}

#[tokio::test]
async fn sub_started_marks_active() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.connections[0].subs[0].sub_id;
    app.handle_event(AppEvent::SubscriptionStarted { id, sub_id });
    assert_eq!(app.connections[0].subs[0].state, SubState::Active);
}

#[tokio::test]
async fn sub_ended_marks_ended_and_stops_recording() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
    let id = connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
    // No tail open and the Console tab is active, so there is nothing to record.
    app.toggle_recording();
    let status = app.status.as_ref().unwrap();
    assert!(status.is_error);
    assert!(status.message.contains("no active tail"));
}

#[tokio::test]
async fn toggle_recording_on_ended_tail_errors() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("a".into()));
    // Slots: 0 Details, 1 Console, 2 Monitor, 3 Keyspace, 4 Pub/Sub, 5 Sub(a), 6 Tail.
    assert_eq!(app.screen, Screen::Browser);
    assert_eq!(app.active_conn().unwrap().panel_tab_count(), 7);
    // The subscribe focused the pub/sub tail at slot 5.
    assert_eq!(app.connections[0].panel_tab, 5);
    app.apply(Action::NextTab); // 6 Tail
    assert_eq!(app.connections[0].panel_tab, 6);
    app.apply(Action::NextTab); // wraps to 0 Server Details
    assert_eq!(app.connections[0].panel_tab, 0, "wraps to the first tab");
    app.apply(Action::PrevTab); // wraps back past Server Details to 6 Tail
    assert_eq!(app.connections[0].panel_tab, 6, "wraps past the first tab");
}

#[tokio::test]
async fn tab_does_not_cycle_off_the_browser() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("a".into()));
    let before = app.connections[0].panel_tab;
    app.screen = Screen::Home;
    app.apply(Action::NextTab);
    assert_eq!(
        app.connections[0].panel_tab, before,
        "Tab is inert off the Browser"
    );
}

#[tokio::test]
async fn tail_selected_key_starts_stream_tail() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
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
    assert_eq!(app.active_conn().unwrap().panel_tab, 6);
}

#[tokio::test]
async fn tail_selected_key_rejects_non_stream() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
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
    connect(&mut app, 1, "prod").await;
    app.connections[0].browser.keys.clear();
    app.tail_selected_key();
    assert!(app
        .status
        .as_ref()
        .unwrap()
        .message
        .contains("no stream key selected"));
}

#[tokio::test]
async fn focusing_monitor_tab_starts_a_monitor_feed() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    // No feed until the tab is focused.
    assert!(app.active_conn().unwrap().monitor_sub().is_none());
    focus_panel(&mut app, PanelTab::Monitor);
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1);
    assert_eq!(conn.subs[0].spec, SubSpec::Monitor);
    assert_eq!(conn.subs[0].label, "monitor");
    // The MONITOR feed lives under its anchor, not as its own Sub tab.
    assert_eq!(conn.active_panel(), PanelTab::Monitor);
    // It starts paused so focusing the tab doesn't immediately track the stream.
    assert!(conn.subs[0].paused);
}

#[tokio::test]
async fn monitor_feed_reveals_at_most_the_per_frame_budget() {
    // The monitor feed counts every event (true throughput) but reveals only a
    // paced few per frame, so a firehose scrolls steadily instead of dumping a
    // whole batch into view; the surplus is dropped from the on-screen feed.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    let sub_id = live_monitor(&mut app);

    app.begin_frame();
    let flood = MONITOR_REVEAL_PER_FRAME + 25;
    for _ in 0..flood {
        app.handle_event(AppEvent::Realtime {
            id,
            sub_id,
            event: broker_event("x"),
        });
    }

    let sub = app.active_conn().unwrap().monitor_sub().unwrap();
    assert_eq!(
        sub.received, flood as u64,
        "every event counts toward the tally (true throughput)"
    );
    assert_eq!(
        sub.events.len(),
        MONITOR_REVEAL_PER_FRAME,
        "only the per-frame budget is revealed; the surplus is dropped"
    );
}

#[tokio::test]
async fn begin_frame_refills_the_monitor_reveal_budget() {
    // Each drawn frame refills the budget, so across frames everything within
    // budget reveals — a steady scroll rather than a one-shot cap.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    let sub_id = live_monitor(&mut app);

    let frames = 3;
    for _ in 0..frames {
        app.begin_frame();
        for _ in 0..MONITOR_REVEAL_PER_FRAME {
            app.handle_event(AppEvent::Realtime {
                id,
                sub_id,
                event: broker_event("x"),
            });
        }
    }

    let revealed = frames * MONITOR_REVEAL_PER_FRAME;
    let sub = app.active_conn().unwrap().monitor_sub().unwrap();
    assert_eq!(
        sub.events.len(),
        revealed,
        "the budget refills per frame, so all in-budget events reveal"
    );
    assert_eq!(sub.received, revealed as u64);
}

#[tokio::test]
async fn non_monitor_feed_ignores_the_reveal_budget() {
    // The reveal cap is monitor-only: a pub/sub feed stores every event in a
    // single frame regardless of the budget (it is not a firehose by nature).
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.active_conn().unwrap().subs[0].sub_id;

    app.begin_frame();
    let flood = MONITOR_REVEAL_PER_FRAME + 25;
    for _ in 0..flood {
        app.handle_event(AppEvent::Realtime {
            id,
            sub_id,
            event: broker_event("x"),
        });
    }

    let sub = &app.active_conn().unwrap().subs[0];
    assert_eq!(sub.events.len(), flood, "pub/sub feed stores everything");
    assert_eq!(sub.received, flood as u64);
}

#[tokio::test]
async fn feeds_are_not_scrollable() {
    // Live feeds always follow the newest event now — there are no scroll keys,
    // so every former scroll key is inert on a focused feed and it keeps
    // following from the bottom.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.start_subscribe(SubSpec::Channel("c".into()));
    let sub_id = app.active_conn().unwrap().subs[0].sub_id;
    app.begin_frame();
    for _ in 0..10 {
        app.handle_event(AppEvent::Realtime {
            id,
            sub_id,
            event: broker_event("x"),
        });
    }
    // Focus the tail's tab, then press every key that used to scroll a feed.
    app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::CONTROL));
    assert!(matches!(
        app.active_conn().unwrap().active_panel(),
        PanelTab::Sub(0)
    ));
    for code in [
        KeyCode::Up,
        KeyCode::Down,
        KeyCode::PageUp,
        KeyCode::PageDown,
        KeyCode::Home,
        KeyCode::End,
    ] {
        app.handle_key(key(code));
    }
    let sub = &app.active_conn().unwrap().subs[0];
    assert_eq!(sub.offset, 0, "the feed never scrolls off the newest event");
    assert!(sub.follow, "the feed keeps following");
}

#[tokio::test]
async fn focus_scoped_feeds_start_paused_and_drop_events() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    focus_panel(&mut app, PanelTab::Keyspace);
    let sub_id = app.active_conn().unwrap().keyspace_sub().unwrap().sub_id;
    assert!(app.active_conn().unwrap().keyspace_sub().unwrap().paused);
    // Events arriving while paused are dropped, not tracked.
    app.handle_event(AppEvent::Realtime {
        id,
        sub_id,
        event: broker_event("evt"),
    });
    let sub = app.active_conn().unwrap();
    let sub = sub.keyspace_sub().unwrap();
    assert_eq!(sub.received, 0);
    assert!(sub.events.is_empty());
}

#[tokio::test]
async fn leaving_monitor_tab_stops_the_feed() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    focus_panel(&mut app, PanelTab::Monitor);
    assert!(app.active_conn().unwrap().monitor_sub().is_some());
    // Cycling away stops and drops the focus-scoped feed.
    focus_panel(&mut app, PanelTab::Keyspace);
    assert!(app.active_conn().unwrap().monitor_sub().is_none());
    // …and focusing Keyspace started its own feed instead.
    assert!(app.active_conn().unwrap().keyspace_sub().is_some());
}

#[tokio::test]
async fn keyspace_feed_uses_active_db() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    app.connections[0].db = 3;
    focus_panel(&mut app, PanelTab::Keyspace);
    assert_eq!(
        app.active_conn().unwrap().keyspace_sub().unwrap().spec,
        SubSpec::Keyspace { db: 3 }
    );
}

#[tokio::test]
async fn leaving_browser_stops_focus_feeds() {
    let (mut app, _rx) = test_app();
    connect(&mut app, 1, "prod").await;
    focus_panel(&mut app, PanelTab::Monitor);
    assert!(app.active_conn().unwrap().monitor_sub().is_some());
    app.apply(Action::Back); // Browser -> Connections
    assert_eq!(app.screen, Screen::Home);
    assert!(
        app.active_conn().unwrap().monitor_sub().is_none(),
        "the MONITOR feed stops when the panel loses focus"
    );
}

#[tokio::test]
async fn sub_notice_is_stored_on_the_tail() {
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
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
