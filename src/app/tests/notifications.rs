use super::*;

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
fn a_transient_notification_fades_over_the_tail_of_its_life() {
    let (mut app, _rx) = test_app();
    app.apply(Action::ToggleMouse); // posts a transient notification
    assert_eq!(
        app.status.as_ref().unwrap().kind,
        StatusKind::Transient,
        "mouse toggle posts a transient"
    );

    // Fresh, and for most of its life, it reads fully opaque.
    assert_eq!(app.status_fade(), 1.0, "a fresh notification is solid");
    app.status.as_mut().unwrap().shown_at = app.now - time::Duration::milliseconds(1500);
    assert_eq!(app.status_fade(), 1.0, "still solid before the fade window");

    // Inside the final stretch the opacity drops below 1 …
    app.status.as_mut().unwrap().shown_at = app.now - time::Duration::milliseconds(2500);
    let mid = app.status_fade();
    assert!(mid > 0.0 && mid < 1.0, "fading mid-window: {mid}");

    // … and deepens as expiry nears.
    app.status.as_mut().unwrap().shown_at = app.now - time::Duration::milliseconds(2950);
    let late = app.status_fade();
    assert!(
        late < mid,
        "the fade deepens toward expiry: {late} !< {mid}"
    );
}

#[test]
fn a_confirm_prompt_never_fades() {
    let (mut app, _rx) = test_app();
    app.apply(Action::Back); // arm quit -> confirmation prompt
    assert_eq!(app.status.as_ref().unwrap().kind, StatusKind::Confirm);
    // A confirm prompt doesn't self-dismiss, so it has no fade-out: it stays
    // fully opaque even long past the transient lifetime.
    app.status.as_mut().unwrap().shown_at = app.now - time::Duration::seconds(10);
    assert_eq!(app.status_fade(), 1.0);
}

#[test]
fn an_absent_notification_reads_as_fully_opaque() {
    let (app, _rx) = test_app();
    assert!(app.status.is_none());
    assert_eq!(app.status_fade(), 1.0);
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
    assert!(app.confirm != ConfirmState::DeleteRecording);
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
    assert!(app.confirm != ConfirmState::Quit);
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
    assert!(
        app.confirm == ConfirmState::Quit,
        "and the chord stays armed"
    );
}
