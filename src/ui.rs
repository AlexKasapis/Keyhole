//! Rendering. [`render`] is a pure function of [`App`] state, called once per
//! frame by the main loop: the active screen and a footer (hints or the active
//! text-entry prompt), then modal overlays (connection form,
//! help). There is no top bar — connection health lives with its screen (the
//! Browser's Server band, the connections list's per-row dots).

mod views;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{App, ConnHealth, InputMode, PaneFocus, PanelTab, Screen};
use crate::broker::BrokerKind;
use crate::theme::Theme;

/// Draw one frame from the current application state.
pub fn render(frame: &mut Frame, app: &mut App) {
    // `Theme` is `Copy`, so taking it by value frees `app` for `&mut` borrows.
    let theme = app.theme;
    let [body_area, footer_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(frame.area());

    match app.screen {
        // Connections and Recordings are two tabs of one merged home area,
        // drawn as a single bordered box whose title is the tab strip.
        Screen::Home | Screen::Recordings => views::home(frame, app, &theme, body_area),
        Screen::Browser => views::browser(frame, app, &theme, body_area),
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

/// Map a [`ConnHealth`] to a status-dot glyph, a one-word label, and the style
/// that colours the dot. A filled `●` marks any live or transitional state; a
/// dim hollow `○` marks having no connection. The word keeps the state legible
/// under `NO_COLOR`, where the dot colours collapse to modifiers. Used by the
/// Browser's Server band.
pub(crate) fn health_indicator(
    health: ConnHealth,
    theme: &Theme,
) -> (&'static str, &'static str, Style) {
    match health {
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
                    " editing connection — Tab move · ←/→ toggle · Enter save · Esc cancel ",
                    theme.dim,
                ))
                .style(theme.status_bar),
                area,
            );
        }
        InputMode::Rename => {
            let line = Line::from(vec![
                Span::styled(" rename ", theme.accent),
                Span::raw(format!("{}▏", app.rename_buf)),
                Span::styled("   Enter save · Esc cancel", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
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
                    format!("   {hint}   Enter start · Tab tabs · Esc keys"),
                    theme.dim,
                ),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        // The console tab has its own input prompt inside the panel, so command
        // mode keeps the keybind row rather than echoing the input a second time.
        InputMode::Normal | InputMode::Command => match &app.status {
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
/// the `·` separator. [`hint_line`] turns these into a styled row. The Browser's
/// "view" section also carries the live sort (and any non-default match) — the
/// former top info-bar indicators now live with their controls here.
fn hint_sections(app: &App) -> Vec<(&'static str, String)> {
    let owned = |pairs: &[(&'static str, &str)]| {
        pairs
            .iter()
            .map(|(l, k)| (*l, k.to_string()))
            .collect::<Vec<_>>()
    };
    match app.screen {
        Screen::Home => owned(&[
            ("nav", "↑↓ move"),
            ("conn", "Enter connect · a add"),
            ("tabs", "Tab recordings"),
            ("go", "b browser"),
            ("app", "? help · Esc Esc quit"),
        ]),
        // The footer follows the focused pane: the key list keeps its grouping /
        // sort / db controls, while a focused bottom subpanel shows its own keys.
        // (Pub/Sub & Tail anchors render the Subscribe prompt instead — see
        // `render_footer` — so only Console and the feed tabs reach this arm.)
        Screen::Browser => {
            let bottom = app
                .active_conn()
                .map(|c| c.focus == PaneFocus::Bottom)
                .unwrap_or(false);
            if bottom {
                let console = matches!(
                    app.active_conn().map(|c| c.active_panel()),
                    Some(PanelTab::Console)
                );
                if console {
                    owned(&[
                        ("console", "type · Enter run"),
                        ("history", "↑↓ · Ctrl-P/N"),
                        ("output", "Ctrl-L clear · PgUp/PgDn scroll"),
                        ("focus", "Tab tabs · Ctrl-↑/Esc keys"),
                    ])
                } else {
                    owned(&[
                        ("scroll", "↑↓ line · PgUp/PgDn page · g/G ends"),
                        ("feed", "p play/pause · r rec · x close"),
                        ("focus", "Tab tabs · Ctrl-↑/Esc keys"),
                    ])
                }
            } else {
                let (sort, arrow, pattern) = app
                    .active_conn()
                    .map(|c| {
                        let arrow = if c.browser.sort_desc { "↓" } else { "↑" };
                        (c.browser.sort.label(), arrow, c.browser.pattern.clone())
                    })
                    .unwrap_or(("name", "↑", "*".to_string()));
                // Show the match pattern only once it differs from the default
                // `*`, so the everyday case stays terse.
                let filter = if pattern == "*" {
                    "/ filter".to_string()
                } else {
                    format!("/ filter {pattern}")
                };
                vec![
                    ("nav", "↑↓ keys · [ ] db".to_string()),
                    ("groups", "⏎/Space collapse · z all".to_string()),
                    ("view", format!("{filter} · o sort {sort}{arrow} · O dir")),
                    ("panel", "Tab/Ctrl-↓ panel".to_string()),
                    ("app", "? help · Esc back".to_string()),
                ]
            }
        }
        Screen::Recordings => owned(&[
            ("nav", "↑↓ move · PgUp/PgDn scroll"),
            ("file", "r rename · dd delete"),
            ("tabs", "Tab connections"),
            ("go", "b browser"),
            ("app", "? help · Esc back"),
        ]),
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

    /// The buffer cell index where `label` first appears as a contiguous run of
    /// cell symbols. Matches by composed symbols (not string bytes) so multi-byte
    /// border glyphs don't misalign the result with screen columns. `label` must
    /// be ASCII (one cell per byte), which the tab labels are.
    fn find_label(buf: &ratatui::buffer::Buffer, label: &str) -> usize {
        let syms: Vec<&str> = buf.content().iter().map(|c| c.symbol()).collect();
        (0..syms.len())
            .find(|&i| i + label.len() <= syms.len() && syms[i..i + label.len()].concat() == label)
            .unwrap_or_else(|| panic!("label {label:?} not found in the rendered frame"))
    }

    fn entry(name: &str, vtype: ValueType) -> EntryMeta {
        EntryMeta {
            key: name.into(),
            vtype,
            ttl: Ttl::Seconds(120),
            size: Some(128),
        }
    }

    // -- footer (always drawn) -----------------------------------------------

    #[test]
    fn footer_has_no_clock_and_there_is_no_top_bar() {
        // The top bar is gone: there is no "Keyhole" brand and no active-connection
        // label up top. The clock has been removed entirely — it rides neither a
        // top bar nor the footer.
        let (mut app, _rx) = test_app();
        let text = screen_text(&mut app);
        assert!(!text.contains("Keyhole"), "the brand label is gone");
        assert!(
            !text.contains("no connection"),
            "no top-bar connection label"
        );
        assert!(!text.contains("UTC"), "the clock no longer renders");
    }

    #[test]
    fn home_panel_renders_at_full_brightness() {
        // The home area is the primary surface: its box border and tab labels use
        // the main foreground (not the dim border/dim style that made it look
        // perpetually unfocused). Connections is the active tab and Recordings is
        // inactive, but neither label uses the muted dim foreground.
        let (mut app, _rx) = test_app();
        let theme = app.theme;
        let mut terminal = Terminal::new(TestBackend::new(90, 16)).unwrap();
        let frame = terminal.draw(|f| render(f, &mut app)).expect("render");
        let buf = frame.buffer;
        // The box border brightened off the dim border colour.
        assert_eq!(buf.content()[0].symbol(), "┌", "top-left box corner");
        assert_ne!(
            buf.content()[0].style().fg,
            theme.border.fg,
            "the home box border is no longer the dim border colour"
        );
        // The inactive tab label is not the muted dim foreground.
        let start = find_label(buf, "Recordings");
        for i in start..start + "Recordings".len() {
            assert_ne!(
                buf.content()[i].style().fg,
                theme.dim.fg,
                "inactive home tab must not use the dim foreground"
            );
        }
    }

    #[tokio::test]
    async fn browser_panel_tabs_keep_the_selected_tab_the_brightest() {
        // In the Browser's bottom panel the selected tab (Console) carries the
        // bright `tab_selected` foreground, so it stays the standout even though
        // the inactive tabs (Monitor) now use the brighter `tab_inactive` colour.
        // Both differ from the muted dim style. The panel is unfocused here (keys
        // own focus), which is exactly when the selected tab must not wash out.
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        let theme = app.theme;
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let frame = terminal.draw(|f| render(f, &mut app)).expect("render");
        let buf = frame.buffer;

        let sel = find_label(buf, "Console");
        for i in sel..sel + "Console".len() {
            assert_eq!(
                buf.content()[i].style().fg,
                theme.tab_selected.fg,
                "selected panel tab uses the bright tab_selected foreground"
            );
        }
        let inactive = find_label(buf, "Monitor");
        for i in inactive..inactive + "Monitor".len() {
            assert_eq!(
                buf.content()[i].style().fg,
                theme.tab_inactive.fg,
                "inactive panel tab uses the tab_inactive foreground"
            );
            assert_ne!(
                buf.content()[i].style().fg,
                theme.dim.fg,
                "and not the muted dim foreground"
            );
        }
        // The selected tab's foreground is distinct from (brighter than) the
        // inactive tabs', so it remains the most prominent label.
        assert_ne!(theme.tab_selected.fg, theme.tab_inactive.fg);
    }

    #[test]
    fn health_indicator_maps_each_health_to_dot_word_and_colour() {
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
            // so each branch of `health_indicator` is exercised.
            app.health = health;
            assert_eq!(
                health_indicator(app.conn_health(), &theme),
                (glyph, label, style),
                "{health:?}"
            );
        }
    }

    #[tokio::test]
    async fn browser_server_band_surfaces_connection_health() {
        // The connection-health indicator (formerly the top-right header dot) now
        // lives in the Browser's Server band: a filled dot and the "connected"
        // word beside the band title. (The band only shows for a live connection —
        // a dropped one bounces back to Home — so "connected" is what it carries;
        // the full health→glyph mapping is covered by the unit test above.)
        let (mut app, _rx) = test_app();
        let handle = mock::handle(1, "prod", 16).await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        app.screen = Screen::Browser;
        assert!(screen_text(&mut app).contains("● connected"));
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
            (Screen::Home, vec!["nav", "conn", "app"], "Enter connect"),
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
        for screen in [Screen::Home, Screen::Browser, Screen::Recordings] {
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
                .map(|(_, keys)| keys.as_str())
                .unwrap_or_else(|| panic!("{screen:?} footer has no 'app' group"));
            assert!(
                app_keys.contains("? help"),
                "{screen:?} 'app' group should offer help: {app_keys:?}"
            );
        }

        // Both home-area tabs offer the `b` jump to the browser in a "go" group
        // and the Tab tab-switch in a "tabs" group. The Browser steps back with
        // Esc, so it carries neither.
        let expected = [
            (Screen::Home, "b browser", "recordings"),
            (Screen::Recordings, "b browser", "connections"),
        ];
        for (screen, go_key, tab_target) in expected {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let sections = hint_sections(&app);
            let group = |label: &str| {
                sections
                    .iter()
                    .find(|(l, _)| *l == label)
                    .map(|(_, keys)| keys.clone())
            };
            let go = group("go")
                .unwrap_or_else(|| panic!("{screen:?} footer has no 'go' navigation group"));
            assert!(
                go.contains(go_key),
                "{screen:?} 'go' group should offer {go_key:?}: {go:?}"
            );
            let tabs =
                group("tabs").unwrap_or_else(|| panic!("{screen:?} footer has no 'tabs' group"));
            assert!(
                tabs.contains(tab_target),
                "{screen:?} 'tabs' group should switch to {tab_target:?}: {tabs:?}"
            );
        }

        // The Browser footer no longer advertises a cross-screen jump (the `R`
        // recordings keybind is gone; Esc backs out to the home area).
        let (mut app, _rx) = test_app();
        app.screen = Screen::Browser;
        let browser = hint_sections(&app);
        assert!(
            !browser.iter().any(|(label, _)| *label == "go"),
            "Browser footer must not offer a 'go' group: {browser:?}"
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
        // With the keys pane focused, the footer advertises moving focus to the
        // bottom panel (Tab / Ctrl-↓) rather than the feed/console controls.
        assert!(text.contains("panel"), "browser footer has a panel section");
        assert!(
            text.contains("Tab/Ctrl-↓ panel"),
            "and advertises Tab/Ctrl-↓ to focus the panel"
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
        // Redis is database-scoped, so its DB row and consolidated note show.
        assert!(text.contains("DB:"), "the Redis DB row renders");
        assert!(
            text.contains("DB selects the database index"),
            "the consolidated Redis note renders"
        );
    }

    #[test]
    fn connection_form_amqp_omits_db_row_and_shows_note() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.toggle_kind(); // -> AMQP
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("[amqp]"));
        // The consolidated note replaces the per-kind blurb …
        assert!(
            text.contains("not database-scoped"),
            "consolidated AMQP note shown"
        );
        // … and AMQP, not being database-scoped, drops the DB row entirely.
        assert!(!text.contains("DB:"), "AMQP has no DB row");
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
        conn.dashboard.stats = Some(ServerStats {
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

    /// End-to-end regression for the on-connect panic at `ui::views::browser`
    /// ("view must be rebuilt in the update phase before render"). Runs the
    /// *real* connect → multi-page SCAN → render loop against a dockerized
    /// Redis: it seeds far more keys than one SCAN page (`scan_count`, default
    /// 500), connects through the real actor, and renders the browser after
    /// every event. Crucially it renders *mid-scan* — keys loaded but the scan
    /// still running — which is exactly the state `begin_scan`'s view-throttle
    /// bug left with an empty view, tripping the render-time `debug_assert!`.
    ///
    /// Run with `just test-int` or `cargo test --features integration --
    /// --include-ignored` and Redis reachable on
    /// `127.0.0.1:$KEYHOLE_TEST_REDIS_PORT` (default 6380).
    #[cfg(feature = "integration")]
    #[tokio::test]
    #[ignore = "needs a live Redis (just test-int)"]
    async fn browser_survives_mid_scan_render_on_real_redis() {
        use crate::broker::actor::spawn_connection;
        use crate::broker::redis::RedisConnection;
        use crate::broker::ConnId;
        use crate::config::RedisProfile;
        use std::time::Duration;
        use tokio::time::timeout;

        let port: u16 = std::env::var("KEYHOLE_TEST_REDIS_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(6380);
        let ns = format!("it:render:{}", std::process::id());

        // Seed well over one SCAN page so the foreground scan spans multiple
        // pages — the bug only bit a multi-page scan.
        let mut raw = redis::Client::open(format!("redis://127.0.0.1:{port}/0"))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .expect("seed connection to test redis");
        let mut pipe = redis::pipe();
        for i in 0..1500u32 {
            pipe.cmd("SET").arg(format!("{ns}:{i:04}")).arg(i).ignore();
        }
        let _: () = pipe.query_async(&mut raw).await.expect("seed keys");

        let (tx, mut rx) = mpsc::channel(512);
        let tracker = TaskTracker::new();
        let cancel = CancellationToken::new();
        let mut app = App::new(
            Config::default(),
            std::path::PathBuf::from("/tmp/keyhole-it-render.toml"),
            std::env::temp_dir(),
            tx.clone(),
            tracker.clone(),
            cancel.clone(),
            None,
        );

        let profile = RedisProfile {
            name: "it-render".into(),
            host: "127.0.0.1".into(),
            port,
            db: 0,
            username: None,
            password: None,
            tls: false,
        };
        let handle = spawn_connection(
            ConnId(1),
            "it-render".into(),
            Box::new(RedisConnection::new(profile, None, 64 * 1024)),
            tx,
            &tracker,
            &cancel,
            std::env::temp_dir(),
        )
        .await
        .expect("connect to test redis");

        // The real connect handler lands on the Browser screen and kicks off the
        // foreground scan; render once right away (no keys yet — the early-out).
        app.handle_event(AppEvent::Connected { handle });
        let _ = screen_text(&mut app);

        // Pump real events, rendering after each — the render is the assertion
        // (a broken invariant panics inside `screen_text`). Track that we really
        // rendered mid-scan with keys present, and that the scan was multi-page.
        let mut pages = 0;
        let mut rendered_mid_scan = false;
        loop {
            let ev = timeout(Duration::from_secs(30), rx.recv())
                .await
                .expect("timed out waiting for a scan page")
                .expect("event channel closed before scan completed");
            let is_page = matches!(ev, AppEvent::KeysPage { .. });
            app.handle_event(ev);
            let _ = screen_text(&mut app);
            if is_page {
                pages += 1;
                let b = &app.connections[0].browser;
                if b.scanning && !b.keys.is_empty() {
                    rendered_mid_scan = true;
                }
                if b.complete {
                    break;
                }
            }
        }

        assert!(
            pages >= 2,
            "expected a multi-page scan, saw {pages} page(s)"
        );
        assert!(
            rendered_mid_scan,
            "expected to render at least once mid-scan with keys loaded"
        );
        assert!(
            app.connections[0].browser.keys.len() >= 1500,
            "all seeded keys should have loaded"
        );

        // Tidy up so the shared db doesn't accumulate this run's keys.
        let keys: Vec<String> = (0..1500u32).map(|i| format!("{ns}:{i:04}")).collect();
        let _: () = redis::cmd("DEL")
            .arg(&keys)
            .query_async(&mut raw)
            .await
            .expect("cleanup seeded keys");
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
            app.connections[0].browser.keys = vec![entry("mykey", ValueType::String)];
            app.connections[0].rebuild_view();
            // Row 0 is the "(no prefix)" group header; the key itself is row 1.
            // Select it so the value pane actually renders the view under test.
            app.connections[0].browser.table.select(Some(1));
            app.connections[0].inspector.value_key = Some("mykey".into());
            app.connections[0].inspector.value = Some(view);
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
        app.connections[0].dashboard.stats = Some(ServerStats {
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
        // With a single DB, the key count shows once — the total, with no
        // redundant `db0 N` breakdown repeating the same number.
        assert!(text.contains("9 keys"), "total key count");
        assert!(
            !text.contains("db0"),
            "no single-DB breakdown that doubles the count"
        );
    }

    #[tokio::test]
    async fn browser_stats_band_breaks_keys_down_across_multiple_dbs() {
        // With keys in more than one DB the per-DB breakdown is informative (the
        // total no longer equals any single DB), so it is shown alongside the total.
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].dashboard.stats = Some(ServerStats {
            redis_version: Some("7.4.0".into()),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        });
        let text = screen_text(&mut app);
        assert!(text.contains("49 keys"), "summed total across DBs");
        assert!(
            text.contains("db0"),
            "per-DB breakdown shown for multiple DBs"
        );
        assert!(text.contains("db1"));
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
        app.connections[0].browser.keys = vec![entry("mykey", ValueType::String)];
        app.connections[0].rebuild_view();
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
        // Explicitly paused (scrolled into history) with recording on.
        sub.paused = true;
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
        // Paused reads as its own state, never alongside "live".
        assert!(!text.contains("live"));
        assert!(text.contains("REC"));
    }

    #[tokio::test]
    async fn panel_tail_renders_scrolled_cue_while_live() {
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
        // Scrolled up but still live (not paused): events keep flowing.
        sub.follow = false;
        sub.offset = 3;
        app.connections[0].subs.push(sub);
        app.connections[0].panel_tab = 4; // the pub/sub tail tab
        let text = screen_text(&mut app);
        assert!(text.contains("live"));
        assert!(text.contains("scrolled"));
        assert!(!text.contains("paused"));
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
        app.connections[0].browser.keys = vec![entry("mykey", ValueType::String)];
        app.connections[0].rebuild_view();
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
    fn recordings_tab_renders_list_and_viewer_panes() {
        use crate::recording::{RecordingView, ViewRecord};
        let (mut app, _rx) = test_app();
        // The Recordings tab is part of the merged home area, reached with Tab.
        app.screen = Screen::Recordings;
        app.recordings = vec![crate::app::RecordingFile {
            name: "prod-pubsub-news-20260619-090807.jsonl".into(),
            size: 2048,
            modified: None,
        }];
        app.recordings_state.select(Some(0));
        app.recording_view = Some((
            "prod-pubsub-news-20260619-090807.jsonl".into(),
            RecordingView {
                connection: Some("prod".into()),
                source_type: Some("pubsub".into()),
                records: vec![ViewRecord {
                    seq: 0,
                    time: "09:08:07.000".into(),
                    source: "news".into(),
                    payload: "hello world".into(),
                }],
                error: None,
            },
        ));
        let text = render_lines(&mut app, 120, 16);
        // The home box's tab strip carries both tab labels, Recordings active.
        assert!(
            text.contains("Connections"),
            "the tab strip lists both tabs"
        );
        assert!(text.contains("Recordings"), "the Recordings tab is shown");
        assert!(text.contains("pubsub"), "the viewer shows the source type");
        assert!(
            text.contains("hello world"),
            "the viewer shows the record payload: {text:?}"
        );
        // Exact count, never a "1000+" estimate.
        assert!(
            text.contains("1 record"),
            "the viewer shows the exact count"
        );
    }

    /// A fixed instant so any time-derived rendering (e.g. status-message
    /// expiry) is stable in snapshots.
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
    fn snapshot_recordings_tab() {
        // The Recordings tab of the merged home area: the single-frame tab strip
        // (Recordings active), the borderless file list, and the scrollable
        // viewer ruled off by a left border, with an exact record count and
        // millisecond record times.
        use crate::recording::{RecordingView, ViewRecord};
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        app.screen = Screen::Recordings;
        app.recordings = vec![crate::app::RecordingFile {
            name: "prod-pubsub-orders-20260619-090807.jsonl".into(),
            size: 4096,
            modified: Some(time::macros::datetime!(2026 - 06 - 19 09:08:07 UTC)),
        }];
        app.recordings_state.select(Some(0));
        app.recording_view = Some((
            "prod-pubsub-orders-20260619-090807.jsonl".into(),
            RecordingView {
                connection: Some("prod".into()),
                source_type: Some("pubsub".into()),
                records: vec![
                    ViewRecord {
                        seq: 0,
                        time: "09:08:07.123".into(),
                        source: "orders".into(),
                        payload: "first event".into(),
                    },
                    ViewRecord {
                        seq: 1,
                        time: "09:08:07.456".into(),
                        source: "orders".into(),
                        payload: "second event".into(),
                    },
                ],
                error: None,
            },
        ));
        insta::assert_snapshot!("recordings_tab", render_lines(&mut app, 100, 14));
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
        conn.dashboard.stats = Some(ServerStats {
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
        insta::assert_snapshot!("help_overlay", render_lines(&mut app, 90, 33));
    }

    #[tokio::test]
    async fn snapshot_browser_with_console() {
        // The read-only console is the first tab of the always-visible bottom
        // panel (the former standalone Console screen). With no tails open, the
        // tab strip shows just the Console tab.
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].browser.keys = vec![
            entry("user:1", ValueType::String),
            entry("session:abc", ValueType::Hash),
        ];
        app.connections[0].rebuild_view();
        app.connections[0].browser.complete = true;
        app.connections[0].browser.table.select(Some(0));
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
        app.connections[0].browser.keys = vec![
            entry("user:1", ValueType::String),
            entry("session:abc", ValueType::Hash),
        ];
        app.connections[0].rebuild_view();
        app.connections[0].browser.complete = true;
        // Rows: [session hdr, session:abc, user hdr, user:1] — select the user:1
        // key (row 3) so the value pane shows its string value, not the prompt.
        app.connections[0].browser.table.select(Some(3));
        app.connections[0].inspector.value_key = Some("user:1".into());
        app.connections[0].inspector.value = Some(ValueView::Str {
            total_bytes: 5,
            shown_bytes: 5,
            text: "alice".into(),
            encoding: PayloadEncoding::Utf8,
        });
        app.connections[0].dashboard.stats = Some(ServerStats {
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
        app.connections[0].browser.keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].rebuild_view();
        app.connections[0].browser.complete = true;
        app.connections[0].browser.table.select(Some(0));
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
        app.connections[0].browser.keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].rebuild_view();
        app.connections[0].browser.complete = true;
        app.connections[0].browser.table.select(Some(0));
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
        // Explicitly paused (scrolled into history) with recording active →
        // the status reads "paused" (never "live") and REC shows.
        sub.paused = true;
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
