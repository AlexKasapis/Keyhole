//! Rendering. [`render`] is a pure function of [`App`] state, called once per
//! frame by the main loop: a header, the active screen, a footer (hints or the
//! active text-entry prompt), plus modal overlays (connection form, help).

mod views;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{App, InputMode, Screen};
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
        Screen::Dashboard => views::dashboard(frame, app, &theme, body_area),
        Screen::Realtime => views::realtime(frame, app, &theme, body_area),
        Screen::Recordings => views::recordings(frame, app, &theme, body_area),
        Screen::Console => views::console(frame, app, &theme, body_area),
    }
    render_footer(frame, app, &theme, footer_area);

    let full = frame.area();
    if app.form.is_some() {
        views::conn_form(frame, app, &theme, full);
    }
    if app.palette.is_some() {
        views::palette(frame, app, &theme, full);
    }
    if app.show_help {
        views::help(frame, &theme, full);
    }
}

fn render_header(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let [left, right] =
        Layout::horizontal([Constraint::Min(0), Constraint::Length(13)]).areas(area);

    let active = app
        .active_conn()
        .map(|c| c.label())
        .unwrap_or_else(|| "no connection".to_string());
    let screen = match app.screen {
        Screen::Connections => "connections",
        Screen::Browser => "browser",
        Screen::Dashboard => "dashboard",
        Screen::Realtime => "realtime",
        Screen::Recordings => "recordings",
        Screen::Console => "console",
    };
    let line = Line::from(vec![
        Span::styled(" BrokerTUI ", theme.title),
        Span::raw("  "),
        Span::styled(active, theme.accent),
        Span::styled(format!("  · {screen}"), theme.dim),
    ]);
    frame.render_widget(Paragraph::new(line).style(theme.status_bar), left);
    frame.render_widget(
        Paragraph::new(Line::from(format!("{} ", clock(app))))
            .alignment(Alignment::Right)
            .style(theme.status_bar),
        right,
    );
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
            let line = Line::from(vec![
                Span::styled(" subscribe ", theme.accent),
                Span::raw(format!("{}▏", app.subscribe_buf)),
                Span::styled(
                    "   pubsub:ch · psub:ch.* · stream:key · keyspace · monitor   Enter start · Esc cancel",
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
                    "   ↑↓ history · Enter run (read-only) · Esc done",
                    theme.dim,
                ),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Palette => {
            let query = app.palette.as_ref().map(|p| p.query.as_str()).unwrap_or("");
            let line = Line::from(vec![
                Span::styled(" palette ", theme.accent),
                Span::raw(format!("{query}▏")),
                Span::styled("   ↑↓ select · Enter run · Esc cancel", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Normal => {
            let [hints_area, status_area] =
                Layout::horizontal([Constraint::Min(0), Constraint::Length(44)]).areas(area);
            frame.render_widget(
                Paragraph::new(Line::from(hints(app.screen))).style(theme.status_bar),
                hints_area,
            );
            match &app.status {
                Some(status) => {
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
                None => {
                    frame.render_widget(Paragraph::new("").style(theme.status_bar), status_area)
                }
            }
        }
    }
}

fn hints(screen: Screen) -> &'static str {
    match screen {
        Screen::Connections => "  ↑↓ move · Enter connect · a add · : palette · ? help · q quit",
        Screen::Browser => {
            "  ↑↓ keys · / filter · [ ] db · t tail · s sub · e console · w watch · : palette · ? help"
        }
        Screen::Dashboard => "  b browser · w watch · e console · c conns · r refresh · : palette · ? help",
        Screen::Realtime => {
            "  ↑↓ scroll · Tab tab · s sub · m monitor · r rec · x stop · G follow · : palette · ? help"
        }
        Screen::Recordings => {
            "  ↑↓ move · r rescan · w watch · b browser · : palette · ? help · q quit"
        }
        Screen::Console => {
            "  i type · ↑↓ scroll · r clear · K keyspace · m monitor · b browser · : palette · ? help"
        }
    }
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
            std::path::PathBuf::from("/tmp/brokertui-ui-test.toml"),
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
        }
    }

    // -- header & footer (always drawn) --------------------------------------

    #[test]
    fn header_shows_title_and_clock() {
        let (mut app, _rx) = test_app();
        let text = screen_text(&mut app);
        assert!(text.contains("BrokerTUI"));
        assert!(text.contains("UTC"), "the clock is in the header");
        assert!(text.contains("no connection"), "no active connection label");
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

    // -- connections screen --------------------------------------------------

    #[test]
    fn connections_empty_state() {
        let (mut app, _rx) = test_app();
        assert!(screen_text(&mut app).contains("No saved connections"));
    }

    // -- placeholder screens with no connection ------------------------------

    #[test]
    fn data_screens_render_no_connection_placeholders() {
        for screen in [Screen::Browser, Screen::Dashboard, Screen::Realtime] {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            assert!(
                screen_text(&mut app).contains("No active connection"),
                "{screen:?} should show a placeholder"
            );
        }
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
    async fn dashboard_renders_stats() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Dashboard;
        app.connections[0].stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            used_memory: Some(1024),
            maxmemory: Some(4096),
            keyspace_hits: Some(3),
            keyspace_misses: Some(1),
            db_keys: vec![(0, 9)],
            ..Default::default()
        });
        let text = screen_text(&mut app);
        assert!(text.contains("Version"));
        assert!(text.contains("7.4.0"));
        assert!(text.contains("Hit ratio"));
    }

    #[tokio::test]
    async fn dashboard_shows_loading_without_stats() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Dashboard;
        assert!(screen_text(&mut app).contains("Loading server stats"));
    }

    #[tokio::test]
    async fn realtime_renders_tail_with_events() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Realtime;
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
        app.connections[0].active_sub = Some(0);
        let text = screen_text(&mut app);
        assert!(text.contains("pubsub:news"), "the tab label should render");
        assert!(text.contains("live"));
    }

    #[tokio::test]
    async fn realtime_renders_paused_and_recording_indicators() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Realtime;
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
        // Scrolled up into history, with recording on.
        sub.follow = false;
        sub.offset = 3;
        sub.recording = RecordState::On {
            records: 7,
            bytes: 2048,
            path: std::path::PathBuf::from("/tmp/r.jsonl"),
        };
        app.connections[0].subs.push(sub);
        app.connections[0].active_sub = Some(0);
        let text = screen_text(&mut app);
        assert!(text.contains("paused"));
        assert!(text.contains("REC"));
    }

    #[tokio::test]
    async fn realtime_with_no_tails_shows_hint() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Realtime;
        assert!(screen_text(&mut app).contains("No live tails"));
    }

    #[tokio::test]
    async fn realtime_renders_keyspace_notice_banner() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Realtime;
        let mut sub = Subscription::new(1, SubSpec::Keyspace { db: 0 }, 100);
        sub.state = SubState::Active;
        sub.notice = Some("keyspace notifications are disabled".into());
        app.connections[0].subs.push(sub);
        app.connections[0].active_sub = Some(0);
        let text = screen_text(&mut app);
        assert!(text.contains("keyspace:db0"), "the tab label renders");
        assert!(text.contains("disabled"), "the notice banner renders");
    }

    #[tokio::test]
    async fn console_renders_prompt_and_entries() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Console;
        app.connections[0]
            .console
            .entries
            .push(crate::app::ConsoleEntry {
                command: "PING".into(),
                output: "PONG".into(),
                is_error: false,
            });
        let text = screen_text(&mut app);
        assert!(text.contains("Console"));
        assert!(text.contains("PING"));
        assert!(text.contains("PONG"));
    }

    #[tokio::test]
    async fn console_empty_state_shows_hint() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Console;
        let text = screen_text(&mut app);
        assert!(text.contains("Read-only command console"));
        assert!(text.contains("INFO server"), "shows example commands");
    }

    #[test]
    fn console_without_connection_shows_placeholder() {
        let (mut app, _rx) = test_app();
        app.screen = Screen::Console;
        assert!(screen_text(&mut app).contains("No active connection"));
    }

    #[test]
    fn palette_overlay_renders_filtered_items() {
        let (mut app, _rx) = test_app();
        app.palette = Some(crate::app::PaletteState::default());
        app.mode = InputMode::Palette;
        let text = screen_text(&mut app);
        assert!(text.contains("Command palette"));
        assert!(text.contains("Quit"), "palette lists actions");
    }

    // -- snapshot tests ------------------------------------------------------
    //
    // These capture the rendered frame (text layout, styles excluded) for the
    // key screens. State is pinned (fixed clock + fixed data) so the output is
    // deterministic. Regenerate after an intentional UI change with:
    //   INSTA_UPDATE=always cargo test
    // then review/commit the updated `src/ui/snapshots/*.snap`.

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

    #[test]
    fn snapshot_help_overlay() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        app.show_help = true;
        insta::assert_snapshot!("help_overlay", render_lines(&mut app, 90, 32));
    }

    #[test]
    fn snapshot_command_palette() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        app.palette = Some(crate::app::PaletteState::default());
        app.mode = InputMode::Palette;
        insta::assert_snapshot!("command_palette", render_lines(&mut app, 90, 24));
    }

    #[tokio::test]
    async fn snapshot_console_with_output() {
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Console;
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
        insta::assert_snapshot!("console_with_output", render_lines(&mut app, 90, 20));
    }

    #[tokio::test]
    async fn snapshot_dashboard() {
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Dashboard;
        app.connections[0].stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            uptime_seconds: Some(3661),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(120),
            used_memory: Some(1024 * 1024),
            used_memory_peak: Some(2 * 1024 * 1024),
            keyspace_hits: Some(900),
            keyspace_misses: Some(100),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        });
        insta::assert_snapshot!("dashboard", render_lines(&mut app, 90, 20));
    }

    #[tokio::test]
    async fn snapshot_realtime_keyspace_notice() {
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Realtime;
        let mut sub = Subscription::new(1, SubSpec::Keyspace { db: 0 }, 100);
        sub.state = SubState::Active;
        sub.notice =
            Some("keyspace notifications are disabled (notify-keyspace-events is empty)".into());
        app.connections[0].subs.push(sub);
        app.connections[0].active_sub = Some(0);
        insta::assert_snapshot!("realtime_keyspace_notice", render_lines(&mut app, 90, 18));
    }
}
