//! Rendering. [`render`] is a pure function of [`App`] state, called once per
//! frame by the main loop: a header, the active screen, a footer (hints or the
//! active text-entry prompt), plus modal overlays (connection form, help).

mod views;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{App, ConnHealth, InputMode, Screen};
use crate::broker::BrokerKind;
use crate::theme::Theme;

/// Draw one frame from the current application state.
pub fn render(frame: &mut Frame, app: &mut App) {
    // `Theme` is `Copy`, so taking it by value frees `app` for `&mut` borrows.
    let theme = app.theme;
    let [header_area, body_area, footer_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    render_header(frame, app, &theme, header_area);
    match app.screen {
        Screen::Connections => views::connections(frame, app, &theme, body_area),
        Screen::Browser => views::browser(frame, app, &theme, body_area),
        Screen::Recordings => views::recordings(frame, app, &theme, body_area),
    }
    render_footer(frame, app, &theme, footer_area);

    let full = frame.area();
    if app.form.is_some() {
        views::conn_form(frame, app, &theme, full);
    }
    if app.show_help {
        views::help(frame, &theme, full);
    }
}

fn render_header(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    // The right side carries the connection indicator (a coloured dot + word)
    // followed by the clock, right-aligned. The column is wide enough for the
    // longest label ("connecting") so nothing clips.
    let [left, right] =
        Layout::horizontal([Constraint::Min(0), Constraint::Length(30)]).areas(area);

    let active = app
        .active_conn()
        .map(|c| c.label())
        .unwrap_or_else(|| "no connection".to_string());
    let screen = match app.screen {
        Screen::Connections => "connections",
        Screen::Browser => "browser",
        Screen::Recordings => "recordings",
    };
    let line = Line::from(vec![
        Span::styled(" Keyhole ", theme.title),
        Span::raw("  "),
        Span::styled(active, theme.accent),
        Span::styled(format!("  · {screen}"), theme.dim),
    ]);
    frame.render_widget(Paragraph::new(line).style(theme.status_bar), left);

    let (dot, label, dot_style) = conn_indicator(app, theme);
    let right_line = Line::from(vec![
        Span::styled(format!("{dot} "), dot_style),
        Span::raw(format!("{label}   ")),
        Span::raw(format!("{} ", clock(app))),
    ]);
    frame.render_widget(
        Paragraph::new(right_line)
            .alignment(Alignment::Right)
            .style(theme.status_bar),
        right,
    );
}

/// The header connection indicator: a status-dot glyph, a one-word label, and
/// the style that colours the dot to convey connection health at a glance.
/// A filled `●` marks any live or transitional state; a dim hollow `○` marks
/// having no connection. The word keeps the state legible under `NO_COLOR`,
/// where the dot colours collapse to modifiers.
fn conn_indicator(app: &App, theme: &Theme) -> (&'static str, &'static str, Style) {
    match app.conn_health() {
        ConnHealth::Connected => ("●", "connected", theme.success),
        ConnHealth::Connecting => ("●", "connecting", theme.warning),
        ConnHealth::Error => ("●", "error", theme.error),
        ConnHealth::Offline => ("○", "offline", theme.dim),
    }
}

fn render_footer(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    match app.mode {
        InputMode::Filter => {
            let line = Line::from(vec![
                Span::styled(" filter ", theme.accent),
                Span::raw(format!("{}▏", app.filter)),
                Span::styled("   Enter apply · Esc cancel", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Form => {
            frame.render_widget(
                Paragraph::new(Line::styled(
                    " editing connection — Tab move · Enter save · Esc cancel ",
                    theme.dim,
                ))
                .style(theme.status_bar),
                area,
            );
        }
        InputMode::Subscribe => {
            // The accepted source specs depend on the active broker, so the
            // prompt's hint follows its kind (falling back to Redis if somehow
            // there is no active connection).
            let hint = app
                .active_conn()
                .map(|c| c.caps.kind.sub_spec_hint())
                .unwrap_or_else(|| BrokerKind::Redis.sub_spec_hint());
            let line = Line::from(vec![
                Span::styled(" subscribe ", theme.accent),
                Span::raw(format!("{}▏", app.subscribe_buf)),
                Span::styled(
                    format!("   {hint}   Enter start · Tab tabs · Esc back"),
                    theme.dim,
                ),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Command => {
            let input = app
                .active_conn()
                .map(|c| c.console.input.as_str())
                .unwrap_or("");
            let line = Line::from(vec![
                Span::styled(" cmd ", theme.accent),
                Span::raw(format!("{input}▏")),
                Span::styled(
                    "   ^P/^N history · PgUp/PgDn scroll · ^L clear · Enter run · Esc back",
                    theme.dim,
                ),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Normal => match &app.status {
            // A status message shares the row: hints left, status right.
            Some(status) => {
                let [hints_area, status_area] =
                    Layout::horizontal([Constraint::Min(0), Constraint::Length(44)]).areas(area);
                frame.render_widget(
                    Paragraph::new(hint_line(app, theme)).style(theme.status_bar),
                    hints_area,
                );
                let style = if status.is_error {
                    theme.error
                } else {
                    theme.success
                };
                frame.render_widget(
                    Paragraph::new(Line::styled(format!("{} ", status.message), style))
                        .alignment(Alignment::Right)
                        .style(theme.status_bar),
                    status_area,
                );
            }
            // No status: give the whole row to the hints so they aren't clipped
            // by an empty reserved status column.
            None => {
                frame.render_widget(
                    Paragraph::new(hint_line(app, theme)).style(theme.status_bar),
                    area,
                );
            }
        },
    }
}

/// The footer keybinds for the active screen, grouped into labelled sections.
/// Each entry is a `(section label, keys)` pair; the keys within a section keep
/// the `·` separator. [`hint_line`] turns these into a styled row.
fn hint_sections(app: &App) -> Vec<(&'static str, &'static str)> {
    match app.screen {
        Screen::Connections => vec![
            ("nav", "↑↓ move"),
            ("conn", "Enter connect · a add"),
            ("go", "R recordings"),
            ("app", "? help · Esc Esc quit"),
        ],
        // Keys are always grouped by prefix, so the footer always offers the
        // collapse/expand controls — there is no grouping toggle. The bottom
        // panel's fixed tabs are cycled with Tab / Shift-Tab (the only way).
        Screen::Browser => vec![
            ("nav", "↑↓ keys · [ ] db"),
            ("groups", "⏎/Space collapse · z all"),
            ("view", "/ filter · o sort · O dir"),
            ("panel", "Tab tabs · p play/pause · r rec · x close"),
            ("go", "R recordings"),
            ("app", "? help · Esc back"),
        ],
        Screen::Recordings => vec![("nav", "↑↓ move"), ("app", "? help · Esc back")],
    }
}

/// Render the footer hints as a single line: each section's label in the
/// heading style (matching the help overlay), its keys in the status-bar
/// foreground, and a dim vertical rule between sections.
fn hint_line(app: &App, theme: &Theme) -> Line<'static> {
    let mut spans = Vec::new();
    for (i, (label, keys)) in hint_sections(app).iter().enumerate() {
        spans.push(if i == 0 {
            Span::raw("  ")
        } else {
            Span::styled(" │ ", theme.dim)
        });
        spans.push(Span::styled(format!("{label} "), theme.heading));
        spans.push(Span::raw(keys.to_string()));
    }
    Line::from(spans)
}

fn clock(app: &App) -> String {
    format!(
        "{:02}:{:02}:{:02} UTC",
        app.now.hour(),
        app.now.minute(),
        app.now.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{ConnForm, Connection, RecordState, SubState, Subscription};
    use crate::broker::actor::mock;
    use crate::broker::{
        BrokerEvent, EntryMeta, Payload, PayloadEncoding, ServerStats, StreamEntry, SubSpec, Ttl,
        ValueType, ValueView,
    };
    use crate::config::Config;
    use crate::event::AppEvent;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use tokio::sync::mpsc::{self, Receiver};
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    fn test_app() -> (App, Receiver<AppEvent>) {
        let (tx, rx) = mpsc::channel(64);
        let app = App::new(
            Config::default(),
            std::path::PathBuf::from("/tmp/keyhole-ui-test.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        (app, rx)
    }

    /// Render one frame at 100x30 and return the on-screen text (row-major).
    fn screen_text(app: &mut App) -> String {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let frame = terminal
            .draw(|f| render(f, app))
            .expect("render must not fail");
        frame.buffer.content().iter().map(|c| c.symbol()).collect()
    }

    fn entry(name: &str, vtype: ValueType) -> EntryMeta {
        EntryMeta {
            key: name.into(),
            vtype,
            ttl: Ttl::Seconds(120),
            size: Some(128),
        }
    }

    // -- header & footer (always drawn) --------------------------------------

    #[test]
    fn header_shows_title_and_clock() {
        let (mut app, _rx) = test_app();
        let text = screen_text(&mut app);
        assert!(text.contains("Keyhole"));
        assert!(text.contains("UTC"), "the clock is in the header");
        assert!(text.contains("no connection"), "no active connection label");
    }

    #[test]
    fn conn_indicator_maps_each_health_to_dot_word_and_colour() {
        // The glyph/word/colour triple must be distinct per state: a filled dot
        // for live or transitional states, a dim hollow one only when there is
        // no connection. The word keeps states legible without colour.
        let (mut app, _rx) = test_app();
        let theme = app.theme;
        let cases = [
            (ConnHealth::Connecting, "●", "connecting", theme.warning),
            (ConnHealth::Error, "●", "error", theme.error),
            (ConnHealth::Offline, "○", "offline", theme.dim),
            (ConnHealth::Connected, "●", "connected", theme.success),
        ];
        for (health, glyph, label, style) in cases {
            // With no active connection, `conn_health` returns this field as-is,
            // so each branch of `conn_indicator` is exercised.
            app.health = health;
            assert_eq!(
                conn_indicator(&app, &theme),
                (glyph, label, style),
                "{health:?}"
            );
        }
    }

    #[tokio::test]
    async fn header_connection_indicator_renders_in_the_top_right() {
        // Offline: a hollow dim dot and the "offline" word render in the header.
        let (mut app, _rx) = test_app();
        assert!(screen_text(&mut app).contains("○ offline"));

        // Connected: a filled dot and "connected", and the offline word is gone.
        let handle = mock::handle(1, "prod", 16).await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        app.health = ConnHealth::Connected;
        let text = screen_text(&mut app);
        assert!(text.contains("● connected"));
        assert!(
            !text.contains("offline"),
            "offline label gone once connected"
        );

        // Dropped: the dot turns to the red "error" state.
        app.handle_event(AppEvent::Disconnected {
            id: crate::broker::ConnId(1),
            reason: "bye".into(),
        });
        assert!(screen_text(&mut app).contains("● error"));
    }

    #[test]
    fn footer_reflects_filter_mode() {
        let (mut app, _rx) = test_app();
        app.mode = InputMode::Filter;
        app.filter = "abc".into();
        assert!(screen_text(&mut app).contains("filter"));
    }

    #[test]
    fn footer_reflects_subscribe_mode() {
        let (mut app, _rx) = test_app();
        app.mode = InputMode::Subscribe;
        assert!(screen_text(&mut app).contains("subscribe"));
    }

    #[test]
    fn footer_groups_hints_into_labelled_sections() {
        // Each screen's hint row is split into labelled sections; rendered wide
        // so nothing clips. The section labels and a key from each must appear.
        let cases = [
            (
                Screen::Connections,
                vec!["nav", "conn", "app"],
                "Enter connect",
            ),
            (Screen::Browser, vec!["nav", "view", "panel"], "/ filter"),
            (Screen::Recordings, vec!["nav", "app"], "Esc back"),
        ];
        for (screen, labels, key) in cases {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let text = render_lines(&mut app, 160, 8);
            for label in labels {
                assert!(
                    text.contains(label),
                    "{screen:?} footer should show section {label:?}: {text:?}"
                );
            }
            assert!(
                text.contains(key),
                "{screen:?} footer should still list {key:?}: {text:?}"
            );
        }
    }

    #[test]
    fn footer_has_no_palette_hint_and_offers_navigation() {
        // The command palette was removed, so no screen's footer may advertise
        // it; every screen instead reaches its actions directly by key, and the
        // "app" group always offers help.
        for screen in [Screen::Connections, Screen::Browser, Screen::Recordings] {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let sections = hint_sections(&app);

            for (label, keys) in &sections {
                assert!(
                    !label.contains("palette") && !keys.contains("palette"),
                    "{screen:?} footer still mentions the palette: {label} {keys}"
                );
            }

            let app_keys = sections
                .iter()
                .find(|(label, _)| *label == "app")
                .map(|(_, keys)| *keys)
                .unwrap_or_else(|| panic!("{screen:?} footer has no 'app' group"));
            assert!(
                app_keys.contains("? help"),
                "{screen:?} 'app' group should offer help: {app_keys:?}"
            );
        }

        // The cross-screen jumps the palette used to provide now live in a "go"
        // group on the screens that still navigate onward. The Recordings screen
        // only steps back (Esc), so it offers neither a "go" group nor a rescan.
        let expected_go = [
            (Screen::Connections, vec!["recordings"]),
            (Screen::Browser, vec!["recordings"]),
        ];
        for (screen, targets) in expected_go {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let sections = hint_sections(&app);
            let go = sections
                .iter()
                .find(|(label, _)| *label == "go")
                .map(|(_, keys)| *keys)
                .unwrap_or_else(|| panic!("{screen:?} footer has no 'go' navigation group"));
            for target in targets {
                assert!(
                    go.contains(target),
                    "{screen:?} 'go' group should offer {target:?}: {go:?}"
                );
            }
        }

        let (mut app, _rx) = test_app();
        app.screen = Screen::Recordings;
        let recordings = hint_sections(&app);
        assert!(
            !recordings.iter().any(|(label, _)| *label == "go"),
            "Recordings footer must not offer a 'go' group: {recordings:?}"
        );
        assert!(
            !recordings.iter().any(|(_, keys)| keys.contains("rescan")),
            "Recordings footer must not advertise a rescan: {recordings:?}"
        );
    }

    #[tokio::test]
    async fn browser_footer_advertises_collapse_and_has_no_grouping_toggle() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        // Keys are always grouped, so the footer always carries the collapse
        // controls and never a grouping toggle.
        let text = render_lines(&mut app, 160, 8);
        assert!(
            text.contains("groups"),
            "browser footer has a groups section"
        );
        assert!(text.contains("collapse"), "and advertises collapse/expand");
        assert!(!text.contains("p group"), "no `p group` toggle");
        assert!(!text.contains("ungroup"), "no `p ungroup` toggle");
        // The bottom panel (Console + tails) is advertised, cycled with Tab.
        assert!(text.contains("panel"), "browser footer has a panel section");
        assert!(
            text.contains("Tab tabs"),
            "and advertises Tab to cycle tabs"
        );
    }

    // -- connections screen --------------------------------------------------

    #[test]
    fn connections_empty_state() {
        let (mut app, _rx) = test_app();
        assert!(screen_text(&mut app).contains("No saved connections"));
    }

    // -- placeholder screens with no connection ------------------------------

    #[test]
    fn browser_renders_no_connection_placeholder() {
        let (mut app, _rx) = test_app();
        app.screen = Screen::Browser;
        assert!(
            screen_text(&mut app).contains("No active connection"),
            "the Browser should show a placeholder without a connection"
        );
    }

    #[test]
    fn recordings_empty_state() {
        let (mut app, _rx) = test_app();
        app.screen = Screen::Recordings;
        assert!(screen_text(&mut app).contains("No recordings found"));
    }

    // -- overlays ------------------------------------------------------------

    #[test]
    fn help_overlay_renders() {
        let (mut app, _rx) = test_app();
        app.show_help = true;
        let text = screen_text(&mut app);
        assert!(text.contains("Help"));
        assert!(text.contains("Navigation"));
    }

    #[test]
    fn connection_form_overlay_renders() {
        let (mut app, _rx) = test_app();
        app.form = Some(ConnForm::new());
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("Add connection"));
        assert!(text.contains("Password"));
        assert!(text.contains("Kind"), "the broker-kind toggle renders");
    }

    #[test]
    fn connection_form_shows_amqp_hint_when_amqp_kind() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.toggle_kind(); // -> AMQP
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("[amqp]"));
        assert!(text.contains("DB is ignored"), "AMQP hint shown");
    }

    #[test]
    fn connection_form_shows_rabbitmq_hint_and_vhost_label() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.toggle_kind(); // Redis -> AMQP
        form.toggle_kind(); // AMQP  -> RabbitMQ
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("[rabbitmq]"));
        // The shared DB slot is relabelled "Vhost" for RabbitMQ …
        assert!(text.contains("Vhost"));
        // … and the RabbitMQ-specific hint is shown.
        assert!(text.contains("Vhost defaults to /"));
    }

    #[test]
    fn connection_form_renders_validation_error() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.error = Some("port must be a number 0-65535".into());
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(
            text.contains("port must be a number"),
            "the form's error line renders"
        );
    }

    #[test]
    fn connections_list_shows_broker_kind() {
        use crate::config::{AmqpProfile, Config, ConnectionConfig, RedisProfile};
        let config = Config {
            connections: vec![
                ConnectionConfig::Redis(RedisProfile {
                    name: "cache".into(),
                    host: "127.0.0.1".into(),
                    port: 6379,
                    db: 0,
                    username: None,
                    password: None,
                    tls: false,
                }),
                ConnectionConfig::Amqp(AmqpProfile {
                    name: "events".into(),
                    host: "broker".into(),
                    port: 5671,
                    username: None,
                    password: None,
                    tls: true,
                }),
            ],
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(64);
        let mut app = App::new(
            config,
            std::path::PathBuf::from("/tmp/bt.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        let text = screen_text(&mut app);
        // The connections screen is a column table with a header row …
        assert!(text.contains("NAME"));
        assert!(text.contains("KIND"));
        assert!(text.contains("ENDPOINT"));
        assert!(text.contains("INFO"));
        // … and the broker kind is its own (unbracketed) column value.
        assert!(text.contains("redis"));
        assert!(text.contains("amqp"));
        assert!(text.contains("cache"));
        assert!(text.contains("events"));
        // Neither profile is connected here, so both read "offline".
        assert!(text.contains("offline"));
    }

    #[tokio::test]
    async fn connections_connected_row_shows_live_info() {
        use crate::config::{Config, ConnectionConfig, RedisProfile};
        let config = Config {
            connections: vec![
                ConnectionConfig::Redis(RedisProfile {
                    name: "cache".into(),
                    host: "127.0.0.1".into(),
                    port: 6379,
                    db: 0,
                    username: Some("admin".into()),
                    password: None,
                    tls: false,
                }),
                ConnectionConfig::Redis(RedisProfile {
                    name: "spare".into(),
                    host: "127.0.0.1".into(),
                    port: 6380,
                    db: 0,
                    username: None,
                    password: None,
                    tls: false,
                }),
            ],
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(64);
        let mut app = App::new(
            config,
            std::path::PathBuf::from("/tmp/bt.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        // "cache" is connected with server stats; "spare" stays offline.
        let handle = mock::handle(1, "cache", 16).await;
        let mut conn = Connection::new(handle);
        conn.stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(120),
            used_memory: Some(1024 * 1024),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        });
        app.connections.push(conn);

        // Render wide so the full INFO column fits (it truncates on narrow
        // terminals, which is fine — the point here is the content it carries).
        let text = render_lines(&mut app, 140, 10);
        // The live Redis row surfaces version, key count, clients, ops/s, memory.
        assert!(text.contains("v7.4.0"), "version in the info column");
        assert!(text.contains("49 keys"), "summed key count across dbs");
        assert!(text.contains("7 clients"));
        assert!(text.contains("120 ops/s"));
        assert!(text.contains("1.0 MiB"), "human-readable memory");
        // The username is shown as a user@ prefix on the endpoint.
        assert!(text.contains("admin@127.0.0.1:6379"));
        // The unconnected profile still reads "offline".
        assert!(text.contains("offline"));
    }

    #[tokio::test]
    async fn connections_connected_amqp_row_reports_tails() {
        use crate::config::{AmqpProfile, Config, ConnectionConfig};
        let config = Config {
            connections: vec![ConnectionConfig::Amqp(AmqpProfile {
                name: "rmq".into(),
                host: "broker".into(),
                port: 5672,
                username: None,
                password: None,
                tls: false,
            })],
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(64);
        let mut app = App::new(
            config,
            std::path::PathBuf::from("/tmp/bt.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        let handle = mock::rabbitmq_handle(1, "rmq").await;
        let mut conn = Connection::new(handle);
        conn.subs
            .push(Subscription::new(1, SubSpec::Channel("c".into()), 10));
        conn.subs
            .push(Subscription::new(2, SubSpec::Channel("d".into()), 10));
        app.connections.push(conn);

        // A broker with no stats endpoint reports liveness and the tail count.
        assert!(screen_text(&mut app).contains("live · 2 tails"));
    }

    // -- connection-bearing screens ------------------------------------------

    async fn app_with_connection() -> (App, Receiver<AppEvent>) {
        let (mut app, rx) = test_app();
        let handle = mock::handle(1, "prod", 16).await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        (app, rx)
    }

    #[tokio::test]
    async fn browser_renders_keys_and_all_value_views() {
        let entries = StreamEntry {
            id: "1-0".into(),
            fields: vec![("field".into(), "value".into())],
        };
        let views = vec![
            ValueView::Str {
                total_bytes: 8,
                shown_bytes: 8,
                text: "hi\nthere".into(),
                encoding: PayloadEncoding::Utf8,
            },
            // shown < total exercises the truncation note.
            ValueView::Str {
                total_bytes: 100,
                shown_bytes: 4,
                text: "{ }".into(),
                encoding: PayloadEncoding::Json,
            },
            ValueView::List {
                len: 2,
                offset: 0,
                items: vec!["a".into(), "b".into()],
            },
            ValueView::Set {
                len: 1,
                members: vec!["m".into()],
            },
            ValueView::Hash {
                len: 1,
                fields: vec![("k".into(), "v".into())],
            },
            ValueView::ZSet {
                len: 1,
                items: vec![("m".into(), 1.5)],
            },
            ValueView::Stream {
                len: 1,
                last_id: "1-0".into(),
                entries: vec![entries],
            },
            ValueView::Missing,
        ];

        for view in views {
            let (mut app, _rx) = app_with_connection().await;
            app.screen = Screen::Browser;
            app.connections[0].keys = vec![entry("mykey", ValueType::String)];
            app.connections[0].table.select(Some(0));
            app.connections[0].value_key = Some("mykey".into());
            app.connections[0].value = Some(view);
            // The assertion is implicit: render_value must not panic for any view.
            let text = screen_text(&mut app);
            assert!(text.contains("mykey"), "the key table should render");
        }
    }

    #[tokio::test]
    async fn browser_shows_server_stats_band() {
        // The Dashboard's server stats are now merged into the Browser as a
        // compact band atop the keys/value panes (Redis has `can_dashboard`).
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            connected_clients: Some(7),
            used_memory: Some(1024),
            maxmemory: Some(4096),
            keyspace_hits: Some(3),
            keyspace_misses: Some(1),
            db_keys: vec![(0, 9)],
            ..Default::default()
        });
        let text = screen_text(&mut app);
        assert!(text.contains("Server"), "the stats band has a title");
        assert!(text.contains("7.4.0"), "version in the metrics line");
        assert!(text.contains("clients"), "client count in the metrics line");
        // The gauges' name prefixes render.
        assert!(text.contains("Mem"));
        assert!(text.contains("Hit"));
    }

    #[tokio::test]
    async fn browser_stats_band_shows_loading_without_stats() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        // No stats yet → the band shows a loading placeholder, not gauges.
        assert!(screen_text(&mut app).contains("Loading server stats"));
    }

    #[tokio::test]
    async fn browser_without_dashboard_capability_hides_band() {
        // A browse-capable broker that lacks server stats shows no band at all —
        // the keys/value panes get the full height. (Redis is the only such
        // broker today, so this guards the capability gate, not a live config.)
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].caps.can_dashboard = false;
        app.connections[0].keys = vec![entry("mykey", ValueType::String)];
        let text = screen_text(&mut app);
        assert!(text.contains("mykey"), "the key table still renders");
        assert!(!text.contains("Server"), "no stats band title");
        assert!(
            !text.contains("Loading server stats"),
            "no band placeholder"
        );
    }

    #[tokio::test]
    async fn panel_renders_active_tail_with_events() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        let mut sub = Subscription::new(1, SubSpec::Channel("news".into()), 100);
        sub.state = SubState::Active;
        for i in 0..5 {
            sub.push(BrokerEvent {
                ts: time::OffsetDateTime::UNIX_EPOCH,
                source: "news".into(),
                payload: Payload::Utf8(format!("message {i}")),
                meta: Vec::new(),
            });
        }
        app.connections[0].subs.push(sub);
        // Focus the tail's tab: pub/sub tails sit after the four leading anchors
        // (Console, Monitor, Keyspace, Pub/Sub), so the first one is slot 4.
        app.connections[0].panel_tab = 4;
        let text = screen_text(&mut app);
        assert!(
            text.contains("pubsub:news"),
            "the tail tab label should render"
        );
        assert!(text.contains("live"), "the tail status row renders");
    }

    #[tokio::test]
    async fn panel_tail_renders_paused_and_recording_indicators() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        let mut sub = Subscription::new(1, SubSpec::Channel("c".into()), 100);
        sub.state = SubState::Active;
        for i in 0..10 {
            sub.push(BrokerEvent {
                ts: time::OffsetDateTime::UNIX_EPOCH,
                source: "c".into(),
                payload: Payload::Utf8(format!("m{i}")),
                meta: Vec::new(),
            });
        }
        // Paused (scrolled off newest) with recording on.
        sub.follow = false;
        sub.offset = 3;
        sub.recording = RecordState::On {
            records: 7,
            bytes: 2048,
            path: std::path::PathBuf::from("/tmp/r.jsonl"),
        };
        app.connections[0].subs.push(sub);
        app.connections[0].panel_tab = 4; // the pub/sub tail tab
        let text = screen_text(&mut app);
        assert!(text.contains("paused"));
        assert!(text.contains("REC"));
    }

    #[tokio::test]
    async fn panel_tab_strip_lists_fixed_anchors_and_tails() {
        // The five fixed anchors are always present; each pub/sub or stream tail
        // adds a tab. The strip shows them all even when the Console is active.
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        let sub = Subscription::new(1, SubSpec::Channel("news".into()), 100);
        app.connections[0].subs.push(sub);
        app.connections[0].panel_tab = 0; // Console active
        let text = screen_text(&mut app);
        assert!(text.contains("Console"), "the Console anchor is listed");
        assert!(text.contains("Monitor"), "the Monitor anchor is listed");
        assert!(text.contains("Keyspace"), "the Keyspace anchor is listed");
        assert!(text.contains("Pub/Sub"), "the Pub/Sub anchor is listed");
        assert!(text.contains("Tail"), "the Tail anchor is listed");
        assert!(text.contains("pubsub:news"), "the tail tab is listed too");
    }

    #[tokio::test]
    async fn panel_renders_keyspace_notice_banner() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        let mut sub = Subscription::new(1, SubSpec::Keyspace { db: 0 }, 100);
        sub.state = SubState::Active;
        sub.notice = Some("keyspace notifications are disabled".into());
        app.connections[0].subs.push(sub);
        // The keyspace feed renders under its anchor (slot 2), not its own tab.
        app.connections[0].panel_tab = 2;
        let text = screen_text(&mut app);
        assert!(text.contains("Keyspace"), "the Keyspace anchor renders");
        assert!(text.contains("disabled"), "the notice banner renders");
    }

    #[tokio::test]
    async fn browser_console_band_renders_prompt_and_entries() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].keys = vec![entry("mykey", ValueType::String)];
        app.connections[0]
            .console
            .entries
            .push(crate::app::ConsoleEntry {
                command: "PING".into(),
                output: "PONG".into(),
                is_error: false,
            });
        let text = screen_text(&mut app);
        // The console is now a band inside the Browser: it coexists with keys.
        assert!(text.contains("mykey"), "the key browser is still shown");
        assert!(text.contains("Console"), "the console band title renders");
        assert!(text.contains("PING"));
        assert!(text.contains("PONG"));
    }

    #[tokio::test]
    async fn browser_console_band_empty_state_shows_hint() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        let text = screen_text(&mut app);
        assert!(text.contains("Read-only command console"));
        assert!(text.contains("INFO server"), "shows example commands");
    }

    #[tokio::test]
    async fn browser_without_console_capability_hides_bottom_panel() {
        // An AMQP broker has no console, so the Browser (were it reachable) must
        // not draw the bottom panel at all. Force the screen to prove the gate.
        let (mut app, _rx) = test_app();
        let handle = mock::amqp_handle(1, "mq").await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        app.screen = Screen::Browser;
        let text = screen_text(&mut app);
        assert!(
            !text.contains("read-only"),
            "no console/panel for a broker without a console"
        );
        assert!(
            !text.contains("Console"),
            "no Console tab for a broker without a console"
        );
    }

    // -- snapshot tests ------------------------------------------------------
    //
    // These capture the rendered frame (text layout, styles excluded) for the
    // key screens. State is pinned (fixed clock + fixed data) so the output is
    // deterministic. Regenerate after an intentional UI change with:
    //   INSTA_UPDATE=always cargo test
    // then review/commit the updated `src/snapshots/*.snap`.

    /// Render one frame and return it as trimmed rows joined by newlines.
    fn render_lines(app: &mut App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let frame = terminal.draw(|f| render(f, app)).expect("render");
        let buf = &frame.buffer;
        let w = buf.area.width as usize;
        buf.content()
            .chunks(w)
            .map(|row| {
                row.iter()
                    .map(|c| c.symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn recordings_screen_renders_list_and_preview_panes() {
        use crate::recording::{PreviewRecord, RecordingPreview};
        let (mut app, _rx) = test_app();
        app.screen = Screen::Recordings;
        app.recordings = vec![crate::app::RecordingFile {
            name: "prod-pubsub-news-20260619-090807.jsonl".into(),
            size: 2048,
            modified: None,
        }];
        app.recordings_state.select(Some(0));
        app.recording_preview = Some((
            "prod-pubsub-news-20260619-090807.jsonl".into(),
            RecordingPreview {
                connection: Some("prod".into()),
                source_type: Some("pubsub".into()),
                records: vec![PreviewRecord {
                    seq: 0,
                    time: "09:08:07".into(),
                    source: "news".into(),
                    payload: "hello world".into(),
                }],
                truncated: false,
                error: None,
            },
        ));
        let text = render_lines(&mut app, 120, 16);
        assert!(text.contains("Recordings"), "the list pane is titled");
        assert!(text.contains("Preview"), "the preview pane is titled");
        assert!(text.contains("pubsub"), "the preview shows the source type");
        assert!(
            text.contains("hello world"),
            "the preview shows the record payload: {text:?}"
        );
    }

    /// A fixed instant so the header clock is stable in snapshots.
    fn pin_clock(app: &mut App) {
        app.now = time::macros::datetime!(2026 - 06 - 19 12:34:56 UTC);
    }

    #[test]
    fn snapshot_connections_empty() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        insta::assert_snapshot!("connections_empty", render_lines(&mut app, 90, 16));
    }

    #[tokio::test]
    async fn snapshot_connections_populated() {
        use crate::config::{AmqpProfile, Config, ConnectionConfig, RabbitmqProfile, RedisProfile};
        let config = Config {
            connections: vec![
                ConnectionConfig::Redis(RedisProfile {
                    name: "cache".into(),
                    host: "127.0.0.1".into(),
                    port: 6379,
                    db: 0,
                    username: Some("admin".into()),
                    password: None,
                    tls: false,
                }),
                ConnectionConfig::Amqp(AmqpProfile {
                    name: "events".into(),
                    host: "broker".into(),
                    port: 5671,
                    username: None,
                    password: None,
                    tls: true,
                }),
                ConnectionConfig::Rabbitmq(RabbitmqProfile {
                    name: "rabbit".into(),
                    host: "rabbit".into(),
                    port: 5672,
                    vhost: "prod".into(),
                    username: None,
                    password: None,
                    tls: false,
                }),
            ],
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(64);
        let mut app = App::new(
            config,
            std::path::PathBuf::from("/tmp/bt.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        pin_clock(&mut app);
        // "cache" is live with stats; the AMQP brokers stay offline.
        let handle = mock::handle(1, "cache", 16).await;
        let mut conn = Connection::new(handle);
        conn.stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(120),
            used_memory: Some(1024 * 1024),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        });
        app.connections.push(conn);
        insta::assert_snapshot!("connections_populated", render_lines(&mut app, 120, 12));
    }

    #[test]
    fn snapshot_help_overlay() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        app.show_help = true;
        insta::assert_snapshot!("help_overlay", render_lines(&mut app, 90, 32));
    }

    #[tokio::test]
    async fn snapshot_browser_with_console() {
        // The read-only console is the first tab of the always-visible bottom
        // panel (the former standalone Console screen). With no tails open, the
        // tab strip shows just the Console tab.
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].keys = vec![
            entry("user:1", ValueType::String),
            entry("session:abc", ValueType::Hash),
        ];
        app.connections[0].complete = true;
        app.connections[0].table.select(Some(0));
        app.connections[0]
            .console
            .entries
            .push(crate::app::ConsoleEntry {
                command: "INFO server".into(),
                output: "redis_version:7.4.0\nuptime_in_seconds:42".into(),
                is_error: false,
            });
        app.connections[0]
            .console
            .entries
            .push(crate::app::ConsoleEntry {
                command: "SET k v".into(),
                output: "`SET` is not on the read-only allowlist".into(),
                is_error: true,
            });
        insta::assert_snapshot!("browser_with_console", render_lines(&mut app, 90, 24));
    }

    #[tokio::test]
    async fn snapshot_browser_with_stats() {
        // The Browser with its merged server-stats band atop the keys + value
        // panes (the former Dashboard content, now part of the main panel).
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].keys = vec![
            entry("user:1", ValueType::String),
            entry("session:abc", ValueType::Hash),
        ];
        app.connections[0].complete = true;
        app.connections[0].table.select(Some(0));
        app.connections[0].value_key = Some("user:1".into());
        app.connections[0].value = Some(ValueView::Str {
            total_bytes: 5,
            shown_bytes: 5,
            text: "alice".into(),
            encoding: PayloadEncoding::Utf8,
        });
        app.connections[0].stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            uptime_seconds: Some(3661),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(120),
            used_memory: Some(1024 * 1024),
            used_memory_peak: Some(2 * 1024 * 1024),
            maxmemory: Some(4 * 1024 * 1024),
            keyspace_hits: Some(900),
            keyspace_misses: Some(100),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        });
        insta::assert_snapshot!("browser_with_stats", render_lines(&mut app, 90, 20));
    }

    #[tokio::test]
    async fn snapshot_browser_panel_keyspace_notice() {
        // The keyspace feed focused under its fixed anchor (slot 2), showing the
        // tab strip (the five anchors) and the advisory notice banner.
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].complete = true;
        app.connections[0].table.select(Some(0));
        let mut sub = Subscription::new(1, SubSpec::Keyspace { db: 0 }, 100);
        sub.state = SubState::Active;
        sub.notice =
            Some("keyspace notifications are disabled (notify-keyspace-events is empty)".into());
        app.connections[0].subs.push(sub);
        app.connections[0].panel_tab = 2;
        insta::assert_snapshot!(
            "browser_panel_keyspace_notice",
            render_lines(&mut app, 90, 26)
        );
    }

    #[tokio::test]
    async fn snapshot_browser_panel_tail_recording() {
        // A live tail focused in the bottom panel with recording on: exercises the
        // tab strip plus the status row, where the state + event tally sit flush
        // left and the REC / paused indicators are pinned to the right edge.
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].complete = true;
        app.connections[0].table.select(Some(0));
        let mut sub = Subscription::new(1, SubSpec::Channel("orders".into()), 100);
        sub.state = SubState::Active;
        for i in 0..6 {
            sub.push(BrokerEvent {
                ts: time::macros::datetime!(2026 - 06 - 19 12:34:56 UTC),
                source: "orders".into(),
                payload: Payload::Utf8(format!("event {i}")),
                meta: Vec::new(),
            });
        }
        // Paused (scrolled off newest) with recording active → paused + REC show.
        sub.follow = false;
        sub.offset = 2;
        sub.recording = RecordState::On {
            records: 6,
            bytes: 4096,
            path: std::path::PathBuf::from("/tmp/orders.jsonl"),
        };
        app.connections[0].subs.push(sub);
        app.connections[0].panel_tab = 4; // the pub/sub tail tab
        insta::assert_snapshot!(
            "browser_panel_tail_recording",
            render_lines(&mut app, 90, 26)
        );
    }
}
