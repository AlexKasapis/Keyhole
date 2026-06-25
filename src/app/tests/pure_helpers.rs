use super::*;

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
