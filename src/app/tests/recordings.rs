use super::*;

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
    assert_eq!(app.screen, Screen::Home);

    // Tab moves to the Recordings tab and scans the directory; Shift-Tab back.
    app.apply(Action::NextTab);
    assert_eq!(app.screen, Screen::Recordings);
    assert_eq!(app.recordings.len(), 1, "entering the tab scans");
    app.apply(Action::PrevTab);
    assert_eq!(app.screen, Screen::Home);

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
    assert!(
        app.confirm == ConfirmState::DeleteRecording,
        "first d arms the confirmation"
    );
    assert_eq!(app.recordings.len(), 2, "nothing deleted on the first d");

    // A second consecutive `d` deletes and rescans.
    app.apply(Action::DeleteRecording);
    assert!(
        app.confirm != ConfirmState::DeleteRecording,
        "delete disarms after firing"
    );
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
    assert!(app.confirm == ConfirmState::DeleteRecording);
    app.apply(Action::Down); // any other input disarms
    assert!(app.confirm != ConfirmState::DeleteRecording);
    app.apply(Action::DeleteRecording); // re-arms rather than deleting
    assert_eq!(app.recordings.len(), 2, "no delete after disarm");

    let _ = std::fs::remove_dir_all(&dir);
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
fn recordings_view_follows_the_selection() {
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
    // Moving the selection re-targets the viewer at the newly selected
    // recording and resets its scroll to the top.
    app.recordings_scroll = 7; // pretend the previous file was scrolled down
    app.apply(Action::Down);
    assert_eq!(
        app.recording_view.as_ref().unwrap().0,
        app.recordings[1].name,
        "the viewer tracks the selected recording"
    );
    assert_eq!(
        app.recordings_scroll, 0,
        "a different recording starts at the top"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ctrl_arrows_move_focus_between_the_recordings_list_and_viewer() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-focus",
        &[("a.jsonl", format!("{}\n", recording_line(0, "a", "s", "p")))],
    );
    // The tab opens on the list.
    assert_eq!(app.recordings_focus, RecordingsFocus::List);

    // Ctrl-→ moves the keyboard into the viewer; Ctrl-← back to the list.
    app.handle_key(ctrl_key(KeyCode::Right));
    assert_eq!(app.recordings_focus, RecordingsFocus::Viewer);
    app.handle_key(ctrl_key(KeyCode::Left));
    assert_eq!(app.recordings_focus, RecordingsFocus::List);

    // Switching away and back resets focus to the list.
    app.handle_key(ctrl_key(KeyCode::Right));
    assert_eq!(app.recordings_focus, RecordingsFocus::Viewer);
    app.apply(Action::PrevTab); // -> Connections
    app.apply(Action::NextTab); // -> Recordings again
    assert_eq!(
        app.recordings_focus,
        RecordingsFocus::List,
        "re-entering the tab lands on the list"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn arrows_scroll_the_viewer_when_it_is_focused_and_move_the_list_otherwise() {
    let (mut app, _rx) = test_app();
    let dir = open_recordings(
        &mut app,
        "rec-scroll",
        &[
            ("a.jsonl", format!("{}\n", recording_line(0, "a", "s", "p"))),
            ("b.jsonl", format!("{}\n", recording_line(0, "b", "s", "p"))),
        ],
    );
    let first = app.recordings_state.selected();

    // List focused: ↓ moves the selection, leaving the scroll at the top.
    app.handle_key(key(KeyCode::Down));
    assert_ne!(app.recordings_state.selected(), first, "the list moved");
    assert_eq!(app.recordings_scroll, 0);

    // Viewer focused: ↓/↑ scroll the recording and leave the selection put.
    app.handle_key(ctrl_key(KeyCode::Right));
    let sel = app.recordings_state.selected();
    app.handle_key(key(KeyCode::Down));
    app.handle_key(key(KeyCode::Down));
    assert_eq!(
        app.recordings_scroll, 2,
        "the viewer scrolled down two lines"
    );
    assert_eq!(
        app.recordings_state.selected(),
        sel,
        "scrolling the viewer does not move the list selection"
    );
    app.handle_key(key(KeyCode::Up));
    assert_eq!(app.recordings_scroll, 1, "↑ scrolls back up");
    // Home pins the viewer to the top.
    app.handle_key(key(KeyCode::Home));
    assert_eq!(app.recordings_scroll, 0, "Home jumps to the top");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn focusing_the_viewer_is_a_no_op_without_a_loaded_recording() {
    let (mut app, _rx) = test_app();
    // An empty recordings dir: there is nothing to scroll, so Ctrl-→ is inert.
    let dir = open_recordings(&mut app, "rec-empty-focus", &[]);
    assert!(app.recording_view.is_none());
    app.handle_key(ctrl_key(KeyCode::Right));
    assert_eq!(
        app.recordings_focus,
        RecordingsFocus::List,
        "no recording loaded -> focus stays on the list"
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
