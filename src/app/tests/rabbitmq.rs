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
        "AMQP 0.9.1 can't list destinations, and nothing is seeded"
    );
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
    let (handle, mut cmds) = mock::capture_handle(1, "rabbit", Capabilities::rabbitmq());
    app.handle_event(AppEvent::Connected { handle });
    assert_eq!(app.screen, Screen::Browser);
    submit_on_tail_anchor(&mut app, "exchange:ex");
    // Connecting sends nothing (no scan, peek or stats), so the tap comes first.
    match cmds.try_recv() {
        Ok(ConnCommand::Subscribe { spec, record, .. }) => {
            assert_eq!(spec, exchange("ex", "#"));
            assert!(!record, "a tap from the anchor doesn't start recording");
        }
        other => panic!("expected a Subscribe, got {other:?}"),
    }
    assert!(cmds.try_recv().is_err(), "nothing else was sent");
}

#[tokio::test]
async fn rabbitmq_destination_pane_ignores_curated_list_keys() {
    let (mut app, _rx) = test_app();
    connect_rabbitmq(&mut app, 1, "rabbit").await;
    // `a` (add), `r` (Jolokia discovery) and `x` / `d` (remove) edit AMQP's
    // curated list; RabbitMQ's list comes from the broker, so they do nothing.
    for c in ['a', 'r', 'x', 'd'] {
        app.handle_key(ch(c));
        assert_eq!(app.mode, InputMode::Normal, "'{c}' opens no prompt");
        assert!(app.status.is_none(), "'{c}' reports nothing");
    }
    assert!(app.active_conn().unwrap().destinations.items.is_empty());
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
