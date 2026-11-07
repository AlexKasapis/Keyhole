use super::*;

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
async fn rabbitmq_t_taps_a_topic_or_fanout_exchange_with_hash() {
    for name in ["keyhole.demo", "amq.fanout"] {
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
async fn rabbitmq_t_on_a_headers_exchange_explains_why_not() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "amq.headers");
    app.handle_key(ch('t'));
    assert!(sent(&mut cmds).is_empty());
    assert!(app.active_conn().unwrap().subs.is_empty());
    let status = app.status.as_ref().expect("status set");
    assert!(status.is_error);
    assert!(
        status.message.contains("header-based taps"),
        "{}",
        status.message
    );
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

#[tokio::test]
async fn rabbitmq_enter_on_a_queue_sends_no_peek() {
    let (mut app, _rx, mut cmds) = listed_app();
    select_row(&mut app, "orders");
    app.handle_key(key(KeyCode::Enter));
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Enter));
    assert!(
        sent(&mut cmds).is_empty(),
        "RabbitMQ queues aren't peeked: the broker couldn't answer a Peek"
    );
    assert!(app.status.is_none());
    assert!(!app.active_conn().unwrap().peek.pending);
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
