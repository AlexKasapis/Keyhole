use super::*;
use crate::broker::{ClientConnection, RosterRows};

/// Tab from the keys pane onto the Tail anchor, type `spec` and press Enter.
fn submit_on_tail_anchor(app: &mut App, spec: &str) {
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(
        app.mode,
        InputMode::Subscribe,
        "Tab lands on the Tail anchor"
    );
    for c in spec.chars() {
        app.handle_key(ch(c));
    }
    app.handle_key(key(KeyCode::Enter));
}

fn exchange(name: &str, binding_key: &str) -> SubSpec {
    SubSpec::Exchange {
        exchange: name.into(),
        binding_key: binding_key.into(),
    }
}

/// Connect a RabbitMQ handle that records the commands the app sends.
fn connect_capturing(app: &mut App) -> Receiver<ConnCommand> {
    let (handle, cmds) = mock::capture_handle(1, "rabbit", Capabilities::rabbitmq());
    app.handle_event(AppEvent::Connected { handle });
    cmds
}

/// The commands sent so far, in order.
fn sent(cmds: &mut Receiver<ConnCommand>) -> Vec<ConnCommand> {
    std::iter::from_fn(|| cmds.try_recv().ok()).collect()
}

/// Deliver a listing reply for connection 1.
fn listed(app: &mut App, result: Result<DestinationList, String>) {
    app.handle_event(AppEvent::DestinationsListed {
        id: ConnId(1),
        result,
    });
}

/// The names of the destination rows, in order.
fn row_names(app: &App) -> Vec<String> {
    let conn = app.active_conn().unwrap();
    conn.destinations
        .items
        .iter()
        .map(|d| d.name.clone())
        .collect()
}

/// Put the cursor on the row named `name`.
fn select_row(app: &mut App, name: &str) {
    let conn = app.active_conn_mut().unwrap();
    let i = conn
        .destinations
        .items
        .iter()
        .position(|d| d.name == name)
        .unwrap_or_else(|| panic!("row {name}"));
    conn.destinations.table.select(Some(i));
}

fn selected_name(app: &App) -> Option<String> {
    app.active_conn()
        .unwrap()
        .selected_destination()
        .map(|d| d.name.clone())
}

/// A connection showing the sample listing, with the command log drained.
fn listed_app() -> (App, Receiver<AppEvent>, Receiver<ConnCommand>) {
    let (mut app, rx) = test_app();
    let mut cmds = connect_capturing(&mut app);
    listed(&mut app, Ok(mock::sample_destinations()));
    sent(&mut cmds);
    (app, rx, cmds)
}

#[tokio::test]
async fn rabbitmq_connection_opens_the_browser_with_a_tail_anchor() {
    let (mut app, _rx) = test_app();
    connect_rabbitmq(&mut app, 1, "rabbit").await;
    assert_eq!(
        app.screen,
        Screen::Browser,
        "RabbitMQ lands on the Browser, whose bottom panel hosts its taps"
    );
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.panel_slots(), vec![PanelTab::Tail]);
    assert!(
        conn.destinations.items.is_empty(),
        "nothing is seeded: the list comes from the broker's listing"
    );
    assert!(
        conn.destinations.list_in_flight,
        "the first listing is sent"
    );
    assert_eq!(conn.destinations.listing, Listing::Pending);
}

#[tokio::test]
async fn rabbitmq_connect_lists_queues_then_exchanges_through_the_actor() {
    let (mut app, mut rx) = test_app();
    let mut mock = mock::MockBroker::new();
    mock.rabbitmq = true;
    mock.list = mock::sample_destinations();
    let handle = mock::spawn(1, "rabbit", mock, app.events.clone()).await;
    app.handle_event(AppEvent::Connected { handle });

    let reply = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("the actor replies")
        .expect("an event");
    assert!(matches!(reply, AppEvent::DestinationsListed { .. }));
    app.handle_event(reply);

    assert_eq!(
        row_names(&app),
        [
            "orders",
            "payments",
            "audit",
            "amq.direct",
            "amq.fanout",
            "amq.headers",
            "amq.rabbitmq.trace",
            "keyhole.demo"
        ]
    );
    let conn = app.active_conn().unwrap();
    assert!(!conn.destinations.list_in_flight);
    assert_eq!(
        conn.destinations.listing,
        Listing::Listed {
            truncated: false,
            stats_disabled: false
        }
    );
    assert_eq!(selected_name(&app).as_deref(), Some("orders"));
    let orders = conn.selected_destination().unwrap();
    assert_eq!(orders.kind, DestKind::Queue);
    assert_eq!(orders.info.as_ref().unwrap().messages, Some(1_980));
}

#[tokio::test]
async fn rabbitmq_list_error_is_recorded_for_the_pane() {
    let (mut app, mut rx) = test_app();
    let mut mock = mock::MockBroker::new();
    mock.rabbitmq = true;
    mock.list_err = Some("management API unavailable at http://h:15672: refused".into());
    let handle = mock::spawn(1, "rabbit", mock, app.events.clone()).await;
    app.handle_event(AppEvent::Connected { handle });
    let reply = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("the actor replies")
        .expect("an event");
    app.handle_event(reply);

    let conn = app.active_conn().unwrap();
    assert!(conn.destinations.items.is_empty());
    assert!(!conn.destinations.list_in_flight);
    assert_eq!(
        conn.destinations.listing,
        Listing::Failed("management API unavailable at http://h:15672: refused".into())
    );
    // Not a footer error: the pane explains it (and tails still work).
    assert!(app.status.is_none());
}

#[tokio::test]
async fn rabbitmq_failed_refresh_keeps_the_rows_already_listed() {
    let (mut app, _rx, _cmds) = listed_app();
    select_row(&mut app, "keyhole.demo");
    listed(
        &mut app,
        Err("management API unavailable at u: timed out".into()),
    );
    assert_eq!(row_names(&app).len(), 8, "the last listing stays on screen");
    assert_eq!(selected_name(&app).as_deref(), Some("keyhole.demo"));
    assert!(matches!(
        app.active_conn().unwrap().destinations.listing,
        Listing::Failed(_)
    ));
}

#[tokio::test]
async fn rabbitmq_refresh_keeps_the_selection_by_name_and_kind() {
    let (mut app, _rx, _cmds) = listed_app();
    select_row(&mut app, "keyhole.demo");

    // A new queue sorts in ahead of it, and its stats moved.
    let mut list = mock::sample_destinations();
    list.items
        .insert(0, mock::queue_row("aaa", QueueType::Classic, 1));
    let demo = list
        .items
        .iter_mut()
        .find(|d| d.name == "keyhole.demo")
        .unwrap();
    demo.publish_rate = Some(42.0);
    listed(&mut app, Ok(list));

    assert_eq!(selected_name(&app).as_deref(), Some("keyhole.demo"));
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.destinations.table.selected(), Some(8));
    let info = conn.selected_destination().unwrap().info.as_ref().unwrap();
    assert_eq!(
        info.publish_rate,
        Some(42.0),
        "the row carries the new stats"
    );
}

#[tokio::test]
async fn rabbitmq_selection_follows_the_kind_not_just_the_name() {
    let (mut app, _rx, _cmds) = listed_app();
    // A queue and an exchange may share a name.
    let mut list = mock::sample_destinations();
    list.items
        .insert(0, mock::queue_row("keyhole.demo", QueueType::Classic, 0));
    select_row(&mut app, "keyhole.demo");
    listed(&mut app, Ok(list));
    let conn = app.active_conn().unwrap();
    let dest = conn.selected_destination().unwrap();
    assert_eq!(
        (dest.name.as_str(), dest.kind),
        ("keyhole.demo", DestKind::Exchange)
    );
}

#[tokio::test]
async fn rabbitmq_refresh_drops_a_deleted_queue() {
    let (mut app, _rx, _cmds) = listed_app();
    select_row(&mut app, "payments");

    let mut list = mock::sample_destinations();
    list.items.retain(|d| d.name != "payments");
    listed(&mut app, Ok(list));

    assert!(!row_names(&app).contains(&"payments".to_string()));
    // The cursor stays where it was, now on the next row.
    assert_eq!(selected_name(&app).as_deref(), Some("audit"));

    // Everything deleted: no rows, no selection.
    listed(&mut app, Ok(DestinationList::default()));
    assert!(row_names(&app).is_empty());
    assert_eq!(
        app.active_conn().unwrap().destinations.table.selected(),
        None
    );
}

#[tokio::test]
async fn rabbitmq_r_lists_again_and_reprobes() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_capturing(&mut app);
    // Connecting lists without a re-probe (`connect` just probed).
    assert!(matches!(
        sent(&mut cmds).as_slice(),
        [ConnCommand::ListDestinations { reprobe: false }]
    ));

    // `r` while that listing is out sends nothing more.
    app.handle_key(ch('r'));
    assert!(sent(&mut cmds).is_empty(), "a refresh never stacks");
    assert!(app.status.as_ref().unwrap().message.contains("already"));

    listed(
        &mut app,
        Err("management API unavailable at u: refused".into()),
    );
    app.handle_key(ch('r'));
    assert!(matches!(
        sent(&mut cmds).as_slice(),
        [ConnCommand::ListDestinations { reprobe: true }]
    ));
    assert!(app.active_conn().unwrap().destinations.list_in_flight);
}

#[tokio::test]
async fn rabbitmq_auto_refresh_lists_on_the_browse_clock() {
    let (mut app, _rx, mut cmds) = listed_app();
    app.browse_refresh_ticks = 2;
    app.on_tick();
    assert!(sent(&mut cmds).is_empty(), "not due yet");
    app.on_tick();
    assert!(matches!(
        sent(&mut cmds).as_slice(),
        [ConnCommand::ListDestinations { reprobe: false }]
    ));

    // Still in flight: the next due tick doesn't stack another.
    app.on_tick();
    app.on_tick();
    assert!(sent(&mut cmds).is_empty());

    // Off the Browser screen the clock doesn't list at all.
    listed(&mut app, Ok(mock::sample_destinations()));
    app.screen = Screen::Recordings;
    for _ in 0..4 {
        app.on_tick();
    }
    assert!(sent(&mut cmds).is_empty());
}

#[tokio::test]
async fn rabbitmq_t_taps_a_topic_fanout_or_headers_exchange_with_hash() {
    for name in ["keyhole.demo", "amq.fanout", "amq.headers"] {
        let (mut app, _rx, mut cmds) = listed_app();
        select_row(&mut app, name);
        app.handle_key(ch('t'));
        match sent(&mut cmds).as_slice() {
            [ConnCommand::Subscribe { spec, .. }] => assert_eq!(*spec, exchange(name, "#")),
            other => panic!("expected one Subscribe for {name}, got {other:?}"),
        }
        assert_eq!(app.active_conn().unwrap().subs.len(), 1);
    }
}

#[tokio::test]
async fn rabbitmq_enter_on_an_exchange_taps_it() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "keyhole.demo");
    app.handle_key(key(KeyCode::Enter));
    assert!(matches!(
        sent(&mut cmds).as_slice(),
        [ConnCommand::Subscribe { .. }]
    ));
}

#[tokio::test]
async fn rabbitmq_t_on_a_direct_exchange_asks_for_the_routing_key() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "amq.direct");
    app.handle_key(ch('t'));
    assert!(sent(&mut cmds).is_empty(), "nothing is tapped yet");
    assert_eq!(
        app.mode,
        InputMode::Subscribe,
        "the Tail anchor has the keys"
    );
    assert_eq!(app.subscribe_buf, "exchange:amq.direct/");
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.focus, PaneFocus::Bottom);
    assert_eq!(conn.active_panel(), PanelTab::Tail);

    // Typing the key and Enter taps the exchange with it.
    for c in "order.created".chars() {
        app.handle_key(ch(c));
    }
    app.handle_key(key(KeyCode::Enter));
    match sent(&mut cmds).as_slice() {
        [ConnCommand::Subscribe { spec, .. }] => {
            assert_eq!(*spec, exchange("amq.direct", "order.created"))
        }
        other => panic!("expected a Subscribe, got {other:?}"),
    }
}

#[tokio::test]
async fn rabbitmq_t_on_a_queue_explains_why_not() {
    let (mut app, _rx, mut cmds) = listed_app();
    for name in ["orders", "payments", "audit"] {
        select_row(&mut app, name);
        app.handle_key(ch('t'));
        assert!(sent(&mut cmds).is_empty(), "{name}: nothing is tapped");
        let status = app.status.as_ref().expect("status set");
        assert!(status.is_error);
        assert!(
            status.message.contains("without consuming"),
            "{}",
            status.message
        );
    }
    assert!(app.active_conn().unwrap().subs.is_empty());
}

fn queue(name: &str) -> SubSpec {
    SubSpec::Queue(name.into())
}

/// Deliver a peek reply for connection 1.
fn peeked(app: &mut App, name: &str, result: Result<Vec<BrokerEvent>, String>) {
    app.handle_event(AppEvent::Peeked {
        id: ConnId(1),
        spec: queue(name),
        result,
    });
}

/// The `Peek` commands among `cmds`, as `(queue, mode)`.
fn peeks(cmds: &[ConnCommand]) -> Vec<(SubSpec, config::PeekMode)> {
    cmds.iter()
        .filter_map(|c| match c {
            ConnCommand::Peek { spec, mode, .. } => Some((spec.clone(), *mode)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn rabbitmq_enter_on_a_queue_peeks_it_in_the_configured_mode() {
    for mode in [config::PeekMode::Browse, config::PeekMode::Destructive] {
        let (mut app, _rx, mut cmds) = listed_app();
        app.config.settings.peek_mode = mode;
        select_row(&mut app, "orders");
        app.handle_key(key(KeyCode::Enter));
        match sent(&mut cmds).as_slice() {
            [ConnCommand::Peek {
                spec,
                mode: sent_mode,
                limit,
            }] => {
                assert_eq!(*spec, queue("orders"));
                assert_eq!(*sent_mode, mode);
                assert_eq!(*limit, VALUE_LIMIT);
            }
            other => panic!("expected one Peek in {mode:?}, got {other:?}"),
        }
        let peek = &app.active_conn().unwrap().peek;
        assert!(peek.pending);
        assert_eq!(peek.peeked, Some(queue("orders")));
    }
}

#[tokio::test]
async fn rabbitmq_skip_mode_sends_nothing() {
    let (mut app, _rx, mut cmds) = listed_app();
    app.config.settings.peek_mode = config::PeekMode::Skip;
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    assert!(sent(&mut cmds).is_empty());
    let peek = &app.active_conn().unwrap().peek;
    assert!(!peek.pending && peek.peeked.is_none());
}

#[tokio::test]
async fn rabbitmq_cursor_moves_and_list_refreshes_never_peek() {
    let (mut app, _rx, mut cmds) = listed_app();
    app.browse_refresh_ticks = 2;
    // Moving over the queues — keys and mouse wheel — sends nothing.
    for k in [
        KeyCode::Down,
        KeyCode::Down,
        KeyCode::Up,
        KeyCode::End,
        KeyCode::Home,
    ] {
        app.handle_key(key(k));
    }
    app.handle_mouse(MouseEventKind::ScrollDown);
    app.handle_mouse(MouseEventKind::ScrollUp);
    assert!(sent(&mut cmds).is_empty(), "moving the cursor never peeks");

    // Peek `orders`, then let the list refresh around it several times.
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    peeked(&mut app, "orders", Ok(vec![broker_event("m1")]));
    assert_eq!(peeks(&sent(&mut cmds)).len(), 1);
    for _ in 0..3 {
        app.on_tick();
        app.on_tick();
        listed(&mut app, Ok(mock::sample_destinations()));
    }
    let after = sent(&mut cmds);
    assert!(
        after
            .iter()
            .all(|c| matches!(c, ConnCommand::ListDestinations { reprobe: false })),
        "the refresh clock only lists: {after:?}"
    );
    assert_eq!(after.len(), 3);
    assert_eq!(app.active_conn().unwrap().peek.events.len(), 1, "kept");
}

#[tokio::test]
async fn rabbitmq_enter_steps_into_a_peeked_queue_and_peeks_one_that_isnt() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    peeked(
        &mut app,
        "orders",
        Ok(vec![broker_event("a"), broker_event("b")]),
    );
    sent(&mut cmds);

    // Enter again steps into its messages instead of reading them again.
    app.handle_key(key(KeyCode::Enter));
    assert!(sent(&mut cmds).is_empty());
    assert!(app.active_conn().unwrap().peek.focused);
    app.handle_key(ctrl_key(KeyCode::Left));

    // On another queue, the inspector still holds `orders`: Ctrl-→ has
    // nothing to step into, and Enter peeks this one.
    select_row(&mut app, "payments");
    assert!(!app.active_conn().unwrap().peek_is_selected());
    app.handle_key(ctrl_key(KeyCode::Right));
    assert!(!app.active_conn().unwrap().peek.focused);
    app.handle_key(key(KeyCode::Enter));
    assert_eq!(
        peeks(&sent(&mut cmds)),
        [(queue("payments"), config::PeekMode::Browse)]
    );
    peeked(&mut app, "payments", Ok(vec![broker_event("p")]));

    // Back on `orders`, Enter reads it again: the second peek.
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    assert_eq!(
        peeks(&sent(&mut cmds)),
        [(queue("orders"), config::PeekMode::Browse)]
    );
}

#[tokio::test]
async fn rabbitmq_scrolling_off_the_peeked_queue_returns_the_keyboard_to_the_list() {
    let (mut app, _rx, _cmds) = listed_app();
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    peeked(&mut app, "orders", Ok(vec![broker_event("a")]));
    app.handle_key(ctrl_key(KeyCode::Right));
    assert!(app.active_conn().unwrap().peek.focused);
    // The wheel moves the destination cursor even from the message list.
    app.handle_mouse(MouseEventKind::ScrollDown);
    assert!(!app.active_conn().unwrap().peek.focused);
}

#[tokio::test]
async fn rabbitmq_an_empty_or_failed_peek_is_retried_with_enter() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    peeked(&mut app, "orders", Ok(Vec::new()));
    app.handle_key(key(KeyCode::Enter));
    peeked(
        &mut app,
        "orders",
        Err("reading queue `orders`: boom".into()),
    );
    app.handle_key(key(KeyCode::Enter));
    assert_eq!(peeks(&sent(&mut cmds)).len(), 3);
}

#[tokio::test]
async fn rabbitmq_peek_error_is_kept_for_the_pane() {
    let (mut app, _rx, _cmds) = listed_app();
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    peeked(
        &mut app,
        "orders",
        Err("reading queue `orders`: permission denied (HTTP 401): ACCESS_REFUSED".into()),
    );
    let peek = &app.active_conn().unwrap().peek;
    assert!(!peek.pending, "no longer peeking");
    assert!(peek.events.is_empty());
    assert!(peek.error.as_deref().unwrap().contains("ACCESS_REFUSED"));
    assert!(app.status.as_ref().unwrap().is_error);

    // A later successful peek clears it.
    app.handle_key(key(KeyCode::Enter));
    peeked(&mut app, "orders", Ok(vec![broker_event("m")]));
    assert_eq!(app.active_conn().unwrap().peek.error, None);
}

#[tokio::test]
async fn rabbitmq_a_reply_for_a_queue_no_longer_peeked_is_dropped() {
    let (mut app, _rx, _cmds) = listed_app();
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    select_row(&mut app, "payments");
    app.handle_key(key(KeyCode::Enter));
    peeked(&mut app, "orders", Ok(vec![broker_event("late")]));
    let peek = &app.active_conn().unwrap().peek;
    assert!(peek.pending, "still waiting for payments");
    assert!(peek.events.is_empty());
}

#[tokio::test]
async fn rabbitmq_stream_queue_is_not_peeked() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "audit");
    app.handle_key(key(KeyCode::Enter));
    assert!(sent(&mut cmds).is_empty(), "a stream refuses basic.get");
    let status = app.status.as_ref().expect("status set");
    assert_eq!(status.message, "can't peek a stream queue");
    let peek = &app.active_conn().unwrap().peek;
    assert!(!peek.pending && peek.peeked.is_none());
}

#[tokio::test]
async fn rabbitmq_browse_peek_of_a_limited_quorum_queue_records_the_limit() {
    let mut list = mock::sample_destinations();
    let payments = list
        .items
        .iter_mut()
        .find(|d| d.name == "payments")
        .unwrap();
    payments.peek_delivery_limit = Some(5);
    for (mode, expect) in [
        (config::PeekMode::Browse, Some(5)),
        // A destructive read doesn't requeue, so nothing counts.
        (config::PeekMode::Destructive, None),
    ] {
        let (mut app, _rx, _cmds) = listed_app();
        listed(&mut app, Ok(list.clone()));
        app.config.settings.peek_mode = mode;
        select_row(&mut app, "payments");
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(
            app.active_conn().unwrap().peek.requeue_limit,
            expect,
            "{mode:?}"
        );
        // A queue without a limit warns of none.
        select_row(&mut app, "orders");
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.active_conn().unwrap().peek.requeue_limit, None);
    }
}

#[tokio::test]
async fn rabbitmq_a_refresh_that_drops_the_peeked_queue_leaves_its_messages() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "payments");
    app.handle_key(key(KeyCode::Enter));
    peeked(&mut app, "payments", Ok(vec![broker_event("p")]));
    app.handle_key(ctrl_key(KeyCode::Right));
    assert!(app.active_conn().unwrap().peek.focused);
    sent(&mut cmds);

    let mut list = mock::sample_destinations();
    list.items.retain(|d| d.name != "payments");
    listed(&mut app, Ok(list));
    let conn = app.active_conn().unwrap();
    assert!(!conn.peek.focused, "the keyboard returns to the list");
    assert!(sent(&mut cmds).is_empty(), "nothing is peeked in its place");
}

#[tokio::test]
async fn rabbitmq_tail_anchor_opens_an_exchange_tap() {
    let (mut app, _rx) = test_app();
    connect_rabbitmq(&mut app, 1, "rabbit").await;
    submit_on_tail_anchor(&mut app, "exchange:ex");
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.subs.len(), 1, "the tap's tab was opened");
    assert_eq!(conn.subs[0].spec, exchange("ex", "#"));
    assert_eq!(conn.panel_slots(), vec![PanelTab::Tail, PanelTab::Sub(0)]);
    assert_eq!(conn.active_panel(), PanelTab::Sub(0));
}

#[tokio::test]
async fn rabbitmq_tail_anchor_sends_an_exchange_subscribe() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_capturing(&mut app);
    assert_eq!(app.screen, Screen::Browser);
    submit_on_tail_anchor(&mut app, "exchange:ex");
    // Connecting sends only the listing (no scan, peek or stats), then the tap.
    match sent(&mut cmds).as_slice() {
        [ConnCommand::ListDestinations { reprobe: false }, ConnCommand::Subscribe { spec, record, .. }] =>
        {
            assert_eq!(*spec, exchange("ex", "#"));
            assert!(!record, "a tap from the anchor doesn't start recording");
        }
        other => panic!("expected a listing then a Subscribe, got {other:?}"),
    }
}

#[tokio::test]
async fn rabbitmq_destination_pane_ignores_curated_list_keys() {
    let (mut app, _rx, _cmds) = listed_app();
    // `a` (add) and `x` / `d` (remove) edit AMQP's curated list; RabbitMQ's
    // list comes from the broker, so they do nothing.
    for c in ['a', 'x', 'd'] {
        app.handle_key(ch(c));
        assert_eq!(app.mode, InputMode::Normal, "'{c}' opens no prompt");
        assert!(app.status.is_none(), "'{c}' reports nothing");
    }
    assert_eq!(row_names(&app).len(), 8, "nothing was removed");
}

#[tokio::test]
async fn rabbitmq_publish_is_refused_as_unsupported() {
    let (mut app, _rx) = test_app();
    connect_rabbitmq(&mut app, 1, "rabbit").await;
    app.handle_key(ch('P'));
    assert_eq!(app.mode, InputMode::Normal);
    let status = app.status.as_ref().expect("status set");
    assert!(status.is_error);
    assert!(
        status.message.contains("not available"),
        "the refusal says the broker can't publish: {}",
        status.message
    );
}

// -- stats ------------------------------------------------------------------

/// RabbitMQ's capabilities when the management probe at connect succeeded.
fn rabbitmq_with_dashboard() -> Capabilities {
    Capabilities {
        can_dashboard: true,
        ..Capabilities::rabbitmq()
    }
}

/// Connect a RabbitMQ handle whose management API answered the probe, with
/// the connect-time commands drained.
fn connect_with_dashboard(app: &mut App) -> Receiver<ConnCommand> {
    let (handle, mut cmds) = mock::capture_handle(1, "rabbit", rabbitmq_with_dashboard());
    app.handle_event(AppEvent::Connected { handle });
    sent(&mut cmds);
    cmds
}

/// Run the active connection's clock up to its next stats refresh.
fn reach_stats_tick(app: &mut App) {
    app.active_conn_mut().unwrap().dashboard.stat_ticks = STATS_REFRESH_TICKS - 1;
    app.on_tick();
}

fn stats_requests(cmds: &[ConnCommand]) -> usize {
    cmds.iter()
        .filter(|c| matches!(c, ConnCommand::RefreshStats))
        .count()
}

fn stats_reply(app: &mut App, result: Result<RabbitmqStats, String>) {
    app.handle_event(AppEvent::StatsUpdated {
        id: ConnId(1),
        result: result.map(BrokerStats::Rabbitmq),
    });
}

#[tokio::test]
async fn a_probed_management_api_adds_server_details_and_asks_for_stats() {
    let (mut app, _rx) = test_app();
    let (handle, mut cmds) = mock::capture_handle(1, "rabbit", rabbitmq_with_dashboard());
    app.handle_event(AppEvent::Connected { handle });
    assert!(
        matches!(
            sent(&mut cmds).as_slice(),
            [
                ConnCommand::ListDestinations { reprobe: false },
                ConnCommand::RefreshStats
            ]
        ),
        "connect lists and asks for the first stats"
    );
    let conn = app.active_conn().unwrap();
    assert_eq!(
        conn.panel_slots(),
        vec![
            PanelTab::ServerDetails,
            PanelTab::Connections,
            PanelTab::Consumers,
            PanelTab::Tail
        ]
    );
    assert_eq!(conn.panel_tab_count(), 4);
    assert_eq!(conn.active_panel(), PanelTab::ServerDetails);
    assert!(conn.dashboard.in_flight);
}

#[tokio::test]
async fn without_the_management_api_there_are_no_stats_or_details() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_capturing(&mut app);
    reach_stats_tick(&mut app);
    assert_eq!(stats_requests(&sent(&mut cmds)), 0);
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.panel_slots(), vec![PanelTab::Tail]);
    assert!(!conn.dashboard.in_flight);
}

#[tokio::test]
async fn tab_from_the_list_skips_server_details_for_the_tail_anchor() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_with_dashboard(&mut app);
    submit_on_tail_anchor(&mut app, "exchange:ex");
    assert!(
        sent(&mut cmds)
            .iter()
            .any(|c| matches!(c, ConnCommand::Subscribe { .. })),
        "the anchor tapped the exchange"
    );
}

#[tokio::test]
async fn stats_requests_never_stack_while_one_is_in_flight() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_with_dashboard(&mut app);
    // The connect-time request hasn't been answered: the clock skips its turn
    // (the liveness ping still goes out).
    reach_stats_tick(&mut app);
    let tick = sent(&mut cmds);
    assert_eq!(stats_requests(&tick), 0, "{tick:?}");
    assert!(tick.iter().any(|c| matches!(c, ConnCommand::Ping)));

    // A reply, success or failure, frees the next turn.
    stats_reply(&mut app, Ok(RabbitmqStats::default()));
    reach_stats_tick(&mut app);
    assert_eq!(stats_requests(&sent(&mut cmds)), 1);
    reach_stats_tick(&mut app);
    assert_eq!(stats_requests(&sent(&mut cmds)), 0, "in flight again");
    stats_reply(&mut app, Err("management API unavailable".into()));
    reach_stats_tick(&mut app);
    assert_eq!(stats_requests(&sent(&mut cmds)), 1);
}

#[tokio::test]
async fn a_repeated_stats_failure_reaches_the_footer_once() {
    let (mut app, _rx) = test_app();
    let _cmds = connect_with_dashboard(&mut app);
    let dead = "management API unavailable at http://x:15672: could not reach the management API";
    stats_reply(&mut app, Err(dead.into()));
    let status = app.status.take().expect("the first failure is reported");
    assert!(status.is_error);
    assert_eq!(status.message, format!("[1] stats: {dead}"));
    assert_eq!(
        app.active_conn().unwrap().dashboard.error.as_deref(),
        Some(dead)
    );

    // The cached error comes back on every refresh: the footer stays quiet.
    stats_reply(&mut app, Err(dead.into()));
    assert!(app.status.is_none(), "the same failure isn't repeated");

    // A different failure is news.
    stats_reply(&mut app, Err("permission denied (HTTP 401)".into()));
    assert!(app.status.take().is_some());

    // A success clears it, so a later failure is reported again.
    let stats = RabbitmqStats {
        rabbitmq_version: Some("3.13.7".into()),
        publish_rate: Some(9.6),
        ..Default::default()
    };
    stats_reply(&mut app, Ok(stats));
    let conn = app.active_conn().unwrap();
    assert!(conn.dashboard.error.is_none());
    assert_eq!(conn.dashboard.primary_history, [10]);
    stats_reply(&mut app, Err(dead.into()));
    assert!(app.status.is_some());
}

#[tokio::test]
async fn a_failed_redis_stats_refresh_still_reports_every_time() {
    // Redis keeps its behaviour: each failure reaches the footer, and the last
    // figures stay up.
    let (mut app, _rx) = test_app();
    let id = connect(&mut app, 1, "prod").await;
    app.handle_event(AppEvent::StatsUpdated {
        id,
        result: Ok(BrokerStats::Redis(ServerStats {
            redis_version: Some("7.4".into()),
            ..Default::default()
        })),
    });
    for _ in 0..2 {
        app.handle_event(AppEvent::StatsUpdated {
            id,
            result: Err("NOPERM INFO".into()),
        });
        let status = app.status.take().expect("reported");
        assert_eq!(status.message, "[1] stats: NOPERM INFO");
    }
    let conn = app.active_conn().unwrap();
    assert!(conn.dashboard.error.is_none());
    assert!(conn.dashboard.stats.is_some());
}

// -- rosters ----------------------------------------------------------------

/// The roster kinds the commands asked for, in order.
fn roster_requests(cmds: &[ConnCommand]) -> Vec<RosterKind> {
    cmds.iter()
        .filter_map(|c| match c {
            ConnCommand::RefreshRoster(kind) => Some(*kind),
            _ => None,
        })
        .collect()
}

fn roster_reply(app: &mut App, kind: RosterKind, result: Result<Roster, String>) {
    app.handle_event(AppEvent::RosterUpdated {
        id: ConnId(1),
        kind,
        result,
    });
}

/// A connections roster of `n` unnamed clients.
fn connections_roster(n: usize, own_only: bool) -> Roster {
    let rows = (0..n)
        .map(|i| ClientConnection {
            name: format!("10.0.0.{i}:5000 -> 10.0.0.99:5672"),
            ..Default::default()
        })
        .collect();
    Roster {
        rows: RosterRows::Connections(rows),
        own_only,
        stats_disabled: false,
    }
}

fn active_panel(app: &App) -> PanelTab {
    app.active_conn().unwrap().active_panel()
}

#[tokio::test]
async fn rabbitmq_slots_put_the_rosters_after_server_details_and_amqps_dont_change() {
    // With the management API: Details, Connections, Consumers, the Tail
    // anchor, then one tab per tail.
    let (mut app, _rx) = test_app();
    let _cmds = connect_with_dashboard(&mut app);
    submit_on_tail_anchor(&mut app, "exchange:ex");
    let conn = app.active_conn().unwrap();
    assert_eq!(
        conn.panel_slots(),
        [
            PanelTab::ServerDetails,
            PanelTab::Connections,
            PanelTab::Consumers,
            PanelTab::Tail,
            PanelTab::Sub(0)
        ]
    );
    assert_eq!(conn.panel_tab_count(), 5);
    assert_eq!(
        conn.active_panel(),
        PanelTab::Sub(0),
        "the new tap is focused"
    );

    // Without it: the anchor and the tails only.
    let (mut app, _rx) = test_app();
    let _cmds = connect_capturing(&mut app);
    submit_on_tail_anchor(&mut app, "exchange:ex");
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.panel_slots(), [PanelTab::Tail, PanelTab::Sub(0)]);
    assert_eq!(conn.panel_tab_count(), 2);

    // AMQP 1.0 never lists clients, even with stats.
    let (mut app, _rx) = test_app();
    connect_amqp(&mut app, 1, "mq").await;
    app.start_subscribe(SubSpec::Topic("t".into()));
    let conn = app.active_conn().unwrap();
    assert_eq!(conn.panel_slots(), [PanelTab::Tail, PanelTab::Sub(0)]);
    let conn = app.active_conn_mut().unwrap();
    conn.caps.can_dashboard = true;
    assert_eq!(
        conn.panel_slots(),
        [PanelTab::ServerDetails, PanelTab::Tail, PanelTab::Sub(0)]
    );
    assert_eq!(conn.panel_tab_count(), 3);
}

#[tokio::test]
async fn a_roster_tab_asks_when_focused_then_on_the_stats_clock_and_others_ask_nothing() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_with_dashboard(&mut app);
    stats_reply(&mut app, Ok(RabbitmqStats::default()));
    // Server Details is selected: the clock asks for stats, not rosters.
    reach_stats_tick(&mut app);
    assert!(roster_requests(&sent(&mut cmds)).is_empty());

    // Tab lands on the Tail anchor, which asks for nothing either.
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(active_panel(&app), PanelTab::Tail);
    reach_stats_tick(&mut app);
    assert!(roster_requests(&sent(&mut cmds)).is_empty());

    // Shift-Tab steps back onto Consumers, which asks at once rather than
    // stay blank until the next tick.
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(active_panel(&app), PanelTab::Consumers);
    assert_eq!(roster_requests(&sent(&mut cmds)), [RosterKind::Consumers]);
    assert!(app.active_conn().unwrap().rosters.consumers.in_flight);

    // In flight: the clock skips its turn, so requests never stack.
    reach_stats_tick(&mut app);
    assert!(roster_requests(&sent(&mut cmds)).is_empty());
    roster_reply(
        &mut app,
        RosterKind::Consumers,
        Ok(Roster::empty(RosterKind::Consumers, false, false)),
    );
    assert!(!app.active_conn().unwrap().rosters.consumers.in_flight);
    reach_stats_tick(&mut app);
    assert_eq!(roster_requests(&sent(&mut cmds)), [RosterKind::Consumers]);
    // A failure frees the next turn too.
    roster_reply(&mut app, RosterKind::Consumers, Err("refused".into()));
    reach_stats_tick(&mut app);
    assert_eq!(roster_requests(&sent(&mut cmds)), [RosterKind::Consumers]);
    roster_reply(
        &mut app,
        RosterKind::Consumers,
        Ok(Roster::empty(RosterKind::Consumers, false, false)),
    );

    // Connections takes over; Consumers stops refreshing.
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(active_panel(&app), PanelTab::Connections);
    assert_eq!(roster_requests(&sent(&mut cmds)), [RosterKind::Connections]);
    roster_reply(
        &mut app,
        RosterKind::Connections,
        Ok(connections_roster(1, false)),
    );
    reach_stats_tick(&mut app);
    assert_eq!(roster_requests(&sent(&mut cmds)), [RosterKind::Connections]);
    roster_reply(
        &mut app,
        RosterKind::Connections,
        Ok(connections_roster(1, false)),
    );

    // Back on Server Details, nothing is listed any more.
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(active_panel(&app), PanelTab::ServerDetails);
    reach_stats_tick(&mut app);
    reach_stats_tick(&mut app);
    assert!(roster_requests(&sent(&mut cmds)).is_empty());
}

#[tokio::test]
async fn a_selected_roster_refreshes_only_on_the_browser() {
    // Like MONITOR, the selected tab keeps refreshing while the keys pane
    // has the keyboard, but not once the Browser is off screen.
    let (mut app, _rx) = test_app();
    let mut cmds = connect_with_dashboard(&mut app);
    focus_panel(&mut app, PanelTab::Connections);
    app.handle_key(ctrl_key(KeyCode::Up));
    assert_eq!(app.active_conn().unwrap().focus, PaneFocus::Keys);
    assert_eq!(active_panel(&app), PanelTab::Connections, "still selected");
    sent(&mut cmds);
    roster_reply(
        &mut app,
        RosterKind::Connections,
        Ok(connections_roster(1, false)),
    );
    reach_stats_tick(&mut app);
    assert_eq!(roster_requests(&sent(&mut cmds)), [RosterKind::Connections]);
    roster_reply(
        &mut app,
        RosterKind::Connections,
        Ok(connections_roster(1, false)),
    );

    app.screen = Screen::Home;
    reach_stats_tick(&mut app);
    assert!(roster_requests(&sent(&mut cmds)).is_empty());
}

#[tokio::test]
async fn without_the_management_api_no_roster_is_ever_asked_for() {
    let (mut app, _rx) = test_app();
    let mut cmds = connect_capturing(&mut app);
    for _ in 0..3 {
        app.handle_key(key(KeyCode::Tab));
        reach_stats_tick(&mut app);
    }
    assert!(roster_requests(&sent(&mut cmds)).is_empty());
    assert!(app.active_conn().unwrap().roster_tab().is_none());
}

#[tokio::test]
async fn a_repeated_roster_failure_reaches_the_footer_once() {
    let (mut app, _rx) = test_app();
    let _cmds = connect_with_dashboard(&mut app);
    focus_panel(&mut app, PanelTab::Connections);
    app.status = None;
    let dead = "management API unavailable at http://x:15672: could not reach the management API";
    roster_reply(&mut app, RosterKind::Connections, Err(dead.into()));
    let status = app.status.take().expect("the first failure is reported");
    assert!(status.is_error);
    assert_eq!(status.message, format!("[1] connections: {dead}"));
    let panel = &app.active_conn().unwrap().rosters.connections;
    assert_eq!(panel.error.as_deref(), Some(dead));
    assert!(!panel.in_flight);

    // The cached error comes back on every refresh: the footer stays quiet.
    roster_reply(&mut app, RosterKind::Connections, Err(dead.into()));
    assert!(app.status.is_none(), "the same failure isn't repeated");
    // Each tab keeps its own.
    roster_reply(&mut app, RosterKind::Consumers, Err(dead.into()));
    let status = app.status.take().expect("reported for consumers");
    assert_eq!(status.message, format!("[1] consumers: {dead}"));

    // A success clears it, so a later failure is reported again.
    roster_reply(
        &mut app,
        RosterKind::Connections,
        Ok(connections_roster(2, false)),
    );
    let panel = &app.active_conn().unwrap().rosters.connections;
    assert!(panel.error.is_none());
    assert_eq!(panel.roster.as_ref().map(Roster::len), Some(2));
    roster_reply(&mut app, RosterKind::Connections, Err(dead.into()));
    assert!(app.status.is_some());

    // A reply for a connection that's gone is dropped.
    app.handle_event(AppEvent::RosterUpdated {
        id: ConnId(9),
        kind: RosterKind::Connections,
        result: Err("late".into()),
    });
    assert!(app.status.take().unwrap().message.contains(dead));
}

#[tokio::test]
async fn each_roster_scrolls_its_own_list_like_server_details() {
    let (mut app, _rx) = test_app();
    let _cmds = connect_with_dashboard(&mut app);
    focus_panel(&mut app, PanelTab::Connections);
    roster_reply(
        &mut app,
        RosterKind::Connections,
        Ok(connections_roster(30, false)),
    );
    let scrolls = |app: &App| {
        let c = app.active_conn().unwrap();
        (
            c.dashboard.details_scroll,
            c.rosters.connections.scroll,
            c.rosters.consumers.scroll,
        )
    };
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Down));
    assert_eq!(scrolls(&app), (0, 2, 0));
    app.handle_key(key(KeyCode::Up));
    assert_eq!(scrolls(&app), (0, 1, 0));
    app.handle_key(key(KeyCode::End));
    assert_eq!(scrolls(&app), (0, u16::MAX, 0));
    app.handle_key(key(KeyCode::Home));
    assert_eq!(scrolls(&app), (0, 0, 0));
    // No feed controls on a roster tab.
    app.handle_key(ch('x'));
    assert_eq!(active_panel(&app), PanelTab::Connections);
    assert_eq!(app.mode, InputMode::Normal);

    focus_panel(&mut app, PanelTab::Consumers);
    app.handle_key(key(KeyCode::Down));
    assert_eq!(scrolls(&app), (0, 0, 1));
    focus_panel(&mut app, PanelTab::ServerDetails);
    app.handle_key(key(KeyCode::Down));
    assert_eq!(scrolls(&app), (1, 0, 1));
}
