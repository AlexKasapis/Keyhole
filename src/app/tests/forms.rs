use super::*;

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
fn form_up_down_move_focus_and_arrows_toggle_tls() {
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    // The form is a vertical stack: Type sits directly under Name, so Down
    // steps Name → Type, and Up steps back.
    app.handle_key(key(KeyCode::Down));
    assert_eq!(app.form.as_ref().unwrap().focus, ConnForm::TYPE_FOCUS);
    app.handle_key(key(KeyCode::Up));
    assert_eq!(app.form.as_ref().unwrap().focus, 0);
    // Up from the first row wraps to the last (TLS).
    app.handle_key(key(KeyCode::Up));
    assert_eq!(app.form.as_ref().unwrap().focus, ConnForm::TLS_FOCUS);

    // ←/→ flip the TLS toggle when it is focused — vertical field movement and
    // the horizontal toggle never collide (Space no longer toggles either — see
    // `form_space_types_into_fields_and_no_longer_toggles`).
    app.form.as_mut().unwrap().focus = ConnForm::TLS_FOCUS;
    app.handle_key(key(KeyCode::Right));
    assert!(app.form.as_ref().unwrap().tls);
    app.handle_key(key(KeyCode::Left));
    assert!(!app.form.as_ref().unwrap().tls);
}

#[test]
fn form_tab_keys_no_longer_navigate() {
    // Field movement moved from Tab/Shift-Tab to ↑/↓; the Tab keys are now
    // inert in the form, so this can't be silently "fixed" back.
    let (mut app, _rx) = test_app();
    app.apply(Action::AddConnection);
    assert_eq!(app.form.as_ref().unwrap().focus, 0);
    app.handle_key(key(KeyCode::Tab));
    app.handle_key(key(KeyCode::BackTab));
    assert_eq!(
        app.form.as_ref().unwrap().focus,
        0,
        "Tab / Shift-Tab no longer move focus in the form"
    );
}

#[test]
fn form_space_types_into_fields_and_no_longer_toggles() {
    // Regression: Space used to flip the TLS/Type toggles even while typing a
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
        form.cycle_type(true); // Redis -> AMQP
        form.cycle_type(true); // AMQP  -> RabbitMQ
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
        form.cycle_type(true); // -> AMQP
        form.cycle_type(true); // -> RabbitMQ
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

#[tokio::test]
async fn edit_key_opens_a_prefilled_form_for_the_selected_profile() {
    let (mut app, _rx) = build_app(config_with(&["alpha", "beta"]), unique_config_path(), None);
    app.screen = Screen::Home;
    app.profile_state.select(Some(1));
    app.handle_key(ch('e'));

    let form = app.form.as_ref().expect("edit form opened");
    assert_eq!(
        form.editing,
        Some(1),
        "remembers which profile is being edited"
    );
    assert_eq!(form.fields[0], "beta", "name pre-filled from the profile");
    assert_eq!(app.mode, InputMode::Form);
}

#[tokio::test]
async fn edit_key_is_a_noop_off_the_connections_tab() {
    let (mut app, _rx) = build_app(config_with(&["alpha"]), unique_config_path(), None);
    app.screen = Screen::Recordings;
    app.profile_state.select(Some(0));
    app.handle_key(ch('e'));
    assert!(
        app.form.is_none(),
        "no edit form opens off the Connections tab"
    );
}

#[tokio::test]
async fn editing_an_offline_profile_replaces_and_persists_without_connecting() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(config_with(&["alpha"]), path.clone(), None);
    let id_before = app.next_id;
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.handle_key(ch('e'));
    app.form.as_mut().unwrap().fields[1] = "10.0.0.9".into(); // host
    app.submit_form();

    assert_eq!(
        app.profiles.len(),
        1,
        "an edit replaces in place, never appends"
    );
    let ConnectionConfig::Redis(p) = &app.profiles[0] else {
        panic!("expected a redis profile");
    };
    assert_eq!(p.host, "10.0.0.9", "the edit is applied");
    assert!(app.form.is_none(), "the form closes on save");
    assert_eq!(
        app.next_id, id_before,
        "an offline profile is not connected merely by saving an edit"
    );

    let saved = std::fs::read_to_string(&path).expect("config written");
    assert!(saved.contains("10.0.0.9"), "the edit is persisted to disk");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn editing_an_amqp_profile_keeps_destinations_and_management_fields() {
    // The connection form doesn't surface the curated destinations or the
    // management-API fields, so an edit must carry them over from the existing
    // profile rather than reset them.
    let path = unique_config_path();
    let config = Config {
        connections: vec![ConnectionConfig::Amqp(AmqpProfile {
            name: "mq".into(),
            host: "127.0.0.1".into(),
            port: 5672,
            username: None,
            password: None,
            tls: false,
            destinations: vec!["topic:events".into(), "queue:orders".into()],
            management_url: Some("http://127.0.0.1:8161".into()),
            management_username: Some("admin".into()),
            management_password: Some("env:MQ_PW".into()),
        })],
        ..Default::default()
    };
    let (mut app, _rx) = build_app(config, path.clone(), None);
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.handle_key(ch('e')); // open the edit form
    app.form.as_mut().unwrap().fields[1] = "10.0.0.9".into(); // change the host
    app.submit_form();

    let ConnectionConfig::Amqp(p) = &app.profiles[0] else {
        panic!("expected an amqp profile");
    };
    assert_eq!(p.host, "10.0.0.9", "the host edit is applied");
    assert_eq!(
        p.destinations,
        vec!["topic:events", "queue:orders"],
        "the curated destinations survive an edit"
    );
    assert_eq!(p.management_url.as_deref(), Some("http://127.0.0.1:8161"));
    assert_eq!(p.management_username.as_deref(), Some("admin"));
    assert_eq!(p.management_password.as_deref(), Some("env:MQ_PW"));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn editing_a_live_profile_reconnects_with_the_new_settings() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(config_with(&["prod"]), path.clone(), None);
    connect(&mut app, 1, "prod").await;
    assert!(app.is_connected("prod"));
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.handle_key(ch('e'));
    app.form.as_mut().unwrap().fields[2] = "6400".into(); // change the port
    app.submit_form();

    // The old session is torn down and a reconnect is kicked off; its Connected
    // event hasn't been delivered yet, so no live connection is present.
    assert!(
        app.connections.is_empty(),
        "the live session is torn down before reconnecting"
    );
    assert_eq!(
        app.conn_health(),
        ConnHealth::Connecting,
        "a reconnect with the new settings is in flight"
    );
    let ConnectionConfig::Redis(p) = &app.profiles[0] else {
        panic!("expected a redis profile");
    };
    assert_eq!(p.port, 6400, "the new port is saved");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn x_disconnects_the_selected_live_profile() {
    let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.handle_key(ch('x'));

    assert!(app.connections.is_empty(), "x disconnects the live session");
    assert!(!app.is_connected("prod"));
    let status = app.status.as_ref().expect("status set");
    assert!(
        status.message.contains("Disconnected"),
        "reports the disconnect: {}",
        status.message
    );
}

#[tokio::test]
async fn x_on_an_offline_profile_reports_that_it_is_not_connected() {
    let (mut app, _rx) = build_app(config_with(&["prod"]), unique_config_path(), None);
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.handle_key(ch('x'));
    let status = app.status.as_ref().expect("status set");
    assert!(
        status.message.contains("not connected"),
        "nothing to disconnect: {}",
        status.message
    );
}

#[tokio::test]
async fn ctrl_d_in_the_edit_form_deletes_after_a_confirming_repeat() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(config_with(&["alpha", "beta"]), path.clone(), None);
    app.screen = Screen::Home;
    app.profile_state.select(Some(0)); // alpha
    app.handle_key(ch('e'));

    // First Ctrl-D only arms the confirmation; nothing is removed.
    app.handle_key(ctrl_ch('d'));
    assert!(
        app.form.as_ref().unwrap().confirm_delete,
        "first press arms"
    );
    assert_eq!(app.profiles.len(), 2, "nothing deleted on the first press");

    // Any other key breaks the confirmation.
    app.handle_key(key(KeyCode::Tab));
    assert!(
        !app.form.as_ref().unwrap().confirm_delete,
        "an unrelated key disarms the pending delete"
    );

    // Re-arm and confirm with a consecutive pair.
    app.handle_key(ctrl_ch('d'));
    app.handle_key(ctrl_ch('d'));
    assert!(
        app.form.is_none(),
        "the form closes once the profile is deleted"
    );
    assert_eq!(app.profiles.len(), 1);
    assert_eq!(app.profiles[0].name(), "beta", "alpha removed, beta kept");

    let saved = std::fs::read_to_string(&path).expect("config written");
    assert!(
        !saved.contains("alpha"),
        "the deleted profile is gone from disk"
    );
    assert!(saved.contains("beta"));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn deleting_a_live_profile_disconnects_it_first() {
    let path = unique_config_path();
    let (mut app, _rx) = build_app(config_with(&["prod"]), path.clone(), None);
    connect(&mut app, 1, "prod").await;
    app.screen = Screen::Home;
    app.profile_state.select(Some(0));
    app.handle_key(ch('e'));
    app.handle_key(ctrl_ch('d'));
    app.handle_key(ctrl_ch('d'));

    assert!(app.profiles.is_empty(), "the profile is removed");
    assert!(
        app.connections.is_empty(),
        "its live session is closed as part of the delete"
    );
    let _ = std::fs::remove_file(&path);
}
