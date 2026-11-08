//! Rendering. [`render`] is a pure function of [`App`] state, called once per
//! frame by the main loop: the active screen and a footer (hints or the active
//! text-entry prompt), then modal overlays (connection form, help, the command
//! palette, and the settings page). There is no top bar — connection health
//! lives with its screen (the Browser's Server band, the connections list's
//! per-row dots).

pub(crate) mod anim;
mod views;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{
    App, ConnHealth, DestKind, InputMode, PaneFocus, PanelTab, RecordingsFocus, Screen,
};
use crate::broker::{BrokerType, ExchangeType};
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
    // The command palette and the settings page it opens are top-most overlays.
    // They are mutually exclusive in practice (settings opens as the palette
    // closes); settings draws last so it wins if both were ever set.
    if app.palette.is_some() {
        views::command_palette(frame, app, &theme, full);
    }
    if app.settings.is_some() {
        views::settings(frame, app, &theme, full);
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

/// Width (columns) of the right-aligned status-message column in the footer; the
/// keybind hints take the rest of the row.
const STATUS_WIDTH: u16 = 44;

/// `message` cut to fit the footer's status column (which ends with a
/// one-space pad), with an ellipsis when it's longer. For a status whose full
/// text shows elsewhere, such as a RabbitMQ publish failure in the details
/// pane.
pub(crate) fn fit_status(message: &str) -> String {
    views::truncate(message, usize::from(STATUS_WIDTH) - 1)
}

fn render_footer(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    match app.mode {
        InputMode::Filter => {
            let line = Line::from(vec![
                Span::styled(" filter ", theme.accent),
                Span::raw(format!("{}▏", app.filter)),
                Span::styled("   Enter apply", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Form => {
            frame.render_widget(
                Paragraph::new(Line::styled(
                    " editing connection — ←/→ toggle · Enter save ",
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
                Span::styled("   Enter save", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Subscribe => {
            // The accepted source specs depend on the active broker, so the
            // prompt's hint follows its type (falling back to Redis if somehow
            // there is no active connection).
            let hint = app
                .active_conn()
                .map(|c| c.caps.r#type.sub_spec_hint())
                .unwrap_or_else(|| BrokerType::Redis.sub_spec_hint());
            let line = Line::from(vec![
                Span::styled(" subscribe ", theme.accent),
                Span::raw(format!("{}▏", app.subscribe_buf)),
                Span::styled(format!("   {hint}   Enter start · Tab nav"), theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::AddDestination => {
            let line = Line::from(vec![
                Span::styled(" add destination ", theme.accent),
                Span::raw(format!("{}▏", app.subscribe_buf)),
                Span::styled("   topic:name · queue:name   Enter add", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::PublishKey => {
            let exchange = app
                .publish_draft
                .as_ref()
                .map(|d| d.dest.name.as_str())
                .unwrap_or_default();
            let line = Line::from(vec![
                Span::styled(" routing key ", theme.accent),
                Span::raw(format!("{}▏", app.publish_buf)),
                Span::styled(
                    format!("   publish to {exchange} · Enter next · Esc cancel"),
                    theme.dim,
                ),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::Publish => {
            let line = Line::from(vec![
                Span::styled(" publish ", theme.accent),
                Span::raw(format!("{}▏", app.publish_buf)),
                Span::styled(format!("{}   Enter send", publish_target(app)), theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        InputMode::PeekFilter => {
            let filter = app
                .active_conn()
                .map(|c| c.peek.filter.clone())
                .unwrap_or_default();
            let line = Line::from(vec![
                Span::styled(" filter messages ", theme.accent),
                Span::raw(format!("{filter}▏")),
                Span::styled("   Enter apply", theme.dim),
            ]);
            frame.render_widget(Paragraph::new(line).style(theme.status_bar), area);
        }
        // The console tab has its own input prompt inside the panel, so command
        // mode keeps the keybind row rather than echoing the input a second time.
        InputMode::Normal | InputMode::Command => match &app.status {
            // A status message shares the row: hints left, status right.
            Some(status) => {
                let [hints_area, status_area] =
                    Layout::horizontal([Constraint::Min(0), Constraint::Length(STATUS_WIDTH)])
                        .areas(area);
                frame.render_widget(
                    Paragraph::new(hint_line(app, theme)).style(theme.status_bar),
                    hints_area,
                );
                let base = if status.is_error {
                    theme.error
                } else {
                    theme.success
                };
                // A transient notification dissolves into the bar over the tail
                // of its life; a confirmation prompt stays solid (fade == 1.0).
                let style = anim::fade(base, theme, app.status_fade());
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

/// Where the body prompt's RabbitMQ publish goes: the queue, or the exchange
/// and how it routes the message. Empty for AMQP 1.0, whose prompt names
/// nothing.
fn publish_target(app: &App) -> String {
    let Some(draft) = app.publish_draft.as_ref() else {
        return String::new();
    };
    let rabbitmq = app
        .active_conn()
        .is_some_and(|c| c.caps.r#type == BrokerType::Rabbitmq);
    if !rabbitmq {
        return String::new();
    }
    let dest = &draft.dest;
    let exchange_type = dest.info.as_ref().and_then(|i| i.exchange_type.as_ref());
    match (dest.kind, exchange_type) {
        (DestKind::Queue, _) => format!("   to queue {}", dest.name),
        (_, Some(ExchangeType::Fanout)) => format!("   to {} (fanout)", dest.name),
        (_, Some(ExchangeType::Headers)) => format!(
            "   to {} · no headers, so only header-less bindings match",
            dest.name
        ),
        _ if draft.routing_key.is_empty() => format!("   to {} with an empty key", dest.name),
        _ => format!("   to {} with key {}", dest.name, draft.routing_key),
    }
}

/// The footer keybinds for the active screen as a flat, ordered list of
/// `(key, action)` pairs. [`hint_line`] renders each key in the heading style
/// and its action dimmed, joined by a dim ` · ` — no group labels, so the row
/// packs tightly. The Browser's sort/filter pair carries the live sort column
/// and any non-default match pattern; the former top info-bar indicators now
/// live with their controls here.
///
/// Basic navigation — the ↑↓ arrows, the Ctrl-arrows (move focus between panes),
/// and Esc (back / quit) — is deliberately *omitted*: it is the same on every
/// screen and is documented once in the help overlay ("Moving around — one model
/// everywhere"), so repeating it in each footer is just clutter. The footer
/// therefore lists only the screen-specific action keys (Tab is kept — it is not
/// basic navigation).
fn hint_sections(app: &App) -> Vec<(&'static str, String)> {
    let owned = |pairs: &[(&'static str, &str)]| {
        pairs
            .iter()
            .map(|(k, a)| (*k, a.to_string()))
            .collect::<Vec<_>>()
    };
    match app.screen {
        Screen::Home => owned(&[
            ("Enter", "connect"),
            ("a", "add"),
            ("Tab", "recordings"),
            (":", "palette"),
            ("?", "help"),
        ]),
        // The footer follows the focused pane: the key list keeps its grouping /
        // sort controls, while a focused bottom subpanel shows its own keys.
        // (Pub/Sub & Tail anchors render the Subscribe prompt instead — see
        // `render_footer` — so only Console and the feed tabs reach this arm.)
        // Pane focus (Ctrl-↑↓) and Esc-back are basic navigation, so they are
        // omitted here; Tab (cycle the bottom tabs) is kept.
        Screen::Browser => {
            let bottom = app
                .active_conn()
                .map(|c| c.focus == PaneFocus::Bottom)
                .unwrap_or(false);
            if bottom {
                // The footer follows the focused bottom tab. Tab cycles the tabs;
                // moving focus between the keys pane and the subpanel (Ctrl-↑↓)
                // and Esc-back are basic navigation, so they stay out of here.
                match app.active_conn().map(|c| c.active_panel()) {
                    Some(PanelTab::Console) => owned(&[
                        ("Enter", "run"),
                        ("Ctrl-P/N", "history"),
                        ("Ctrl-L", "clear"),
                        ("Tab", "nav"),
                    ]),
                    // Server Details, Connections and Consumers are passive
                    // lists: ↑↓ scroll them (basic nav, not shown) — there is
                    // no feed to play/pause, record or close.
                    Some(PanelTab::ServerDetails | PanelTab::Connections | PanelTab::Consumers) => {
                        owned(&[("Tab", "nav"), ("?", "help")])
                    }
                    // A live-feed tab (Monitor / Keyspace / a pub-sub or stream
                    // tail): it follows newest (not scrollable). p/r play-pause /
                    // record it; only a runtime tail (`Sub`) can be closed with x.
                    panel => {
                        let mut pairs = vec![
                            ("Tab", "nav".to_string()),
                            ("p", "play/pause".to_string()),
                            ("r", "rec".to_string()),
                        ];
                        if matches!(panel, Some(PanelTab::Sub(_))) {
                            pairs.push(("x", "close".to_string()));
                        }
                        pairs.push(("?", "help".to_string()));
                        pairs
                    }
                }
            } else if app.active_conn().is_some_and(|c| c.caps.has_destinations()) {
                // The destination browser's body is the destination list (left)
                // over a peeked message list + body preview (right). The hints
                // follow whether the keyboard is on the destinations or the
                // message list. Moving between them (Ctrl-←/→) and into the tails
                // (Ctrl-↓), the ↑↓ list movement and Esc-back are all basic
                // navigation, so only the screen-specific keys (plus Tab to the
                // tails) appear here.
                let on_messages = app.active_conn().is_some_and(|c| c.peek.focused);
                let curated = app
                    .active_conn()
                    .is_some_and(|c| c.caps.curates_destinations());
                // An internal exchange refuses client publishes, so `P`
                // isn't offered on one.
                let internal = app
                    .active_conn()
                    .and_then(|c| c.selected_destination())
                    .is_some_and(|d| d.is_internal());
                if !curated && on_messages {
                    // RabbitMQ's peeked messages: filter them, or publish to
                    // their queue.
                    owned(&[
                        ("/", "filter"),
                        ("P", "publish"),
                        ("Tab", "tails"),
                        (":", "palette"),
                        ("?", "help"),
                    ])
                } else if !curated {
                    // RabbitMQ: its list comes from the broker (nothing to add or
                    // remove), so `r` lists it again, and `t` taps an exchange.
                    let mut pairs = vec![("r", "refresh"), ("t", "tap")];
                    if !internal {
                        pairs.push(("P", "publish"));
                    }
                    pairs.extend([("Tab", "tails"), (":", "palette"), ("?", "help")]);
                    owned(&pairs)
                } else if on_messages {
                    owned(&[
                        ("/", "filter"),
                        ("P", "publish"),
                        ("Tab", "tails"),
                        (":", "palette"),
                        ("?", "help"),
                    ])
                } else {
                    owned(&[
                        ("a", "add"),
                        ("x", "remove"),
                        ("t", "tail"),
                        ("P", "publish"),
                        ("Tab", "tails"),
                        (":", "palette"),
                        ("?", "help"),
                    ])
                }
            } else {
                let pattern = app
                    .active_conn()
                    .map(|c| c.browser.pattern.clone())
                    .unwrap_or_else(|| "*".to_string());
                // Show the match pattern only once it differs from the default
                // `*`, so the everyday case stays terse.
                let filter = if pattern == "*" {
                    "filter".to_string()
                } else {
                    format!("filter {pattern}")
                };
                vec![
                    ("Enter", "fold".to_string()),
                    ("z", "all".to_string()),
                    ("/", filter),
                    ("oO", "sort".to_string()),
                    ("Tab", "panel".to_string()),
                    (":", "palette".to_string()),
                    ("?", "help".to_string()),
                ]
            }
        }
        // The footer follows the focused pane: the list moves/renames/deletes,
        // while the viewer scrolls. The ↑↓ movement, Ctrl-←/→ focus swap and
        // Esc-back are basic navigation, so they are omitted.
        Screen::Recordings => match app.recordings_focus {
            RecordingsFocus::Viewer => {
                owned(&[("Tab", "connections"), (":", "palette"), ("?", "help")])
            }
            RecordingsFocus::List => owned(&[
                ("r", "rename"),
                ("dd", "delete"),
                ("Tab", "connections"),
                (":", "palette"),
                ("?", "help"),
            ]),
        },
    }
}

/// Render the footer hints as a single line: each `(key, action)` pair shows the
/// key in the heading style (bold accent, matching the help overlay) and the
/// action dimmed, with a dim ` · ` between pairs and a single leading space. The
/// row never wraps, so it clips on the right on a narrow terminal.
fn hint_line(app: &App, theme: &Theme) -> Line<'static> {
    let mut spans = vec![Span::raw(" ")];
    for (i, (key, action)) in hint_sections(app).iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" · ", theme.dim));
        }
        spans.push(Span::styled((*key).to_string(), theme.heading));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(action.clone(), theme.dim));
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{
        ConnForm, Connection, DestKind, RecordState, ScanPhase, Status, StatusKind, SubState,
        Subscription,
    };
    use crate::broker::actor::mock;
    use crate::broker::{
        BrokerEvent, BrokerStats, ClientConnection, ClientInfo, ConnId, DestinationList, EntryMeta,
        NodeHealth, Payload, PayloadEncoding, QueueConsumer, RabbitmqStats, Roster, RosterKind,
        RosterRows, ServerStats, StreamEntry, SubSpec, Ttl, ValueType, ValueView,
    };
    use crate::config::Config;
    use crate::event::AppEvent;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::backend::TestBackend;
    use ratatui::style::Color;
    use ratatui::Terminal;
    use time::OffsetDateTime;
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
        // Select the Console tab (Server Details is the leftmost/default tab now).
        app.connections[0].panel_tab = 1;
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
        let handle = mock::handle(1, "prod").await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        app.screen = Screen::Browser;
        assert!(screen_text(&mut app).contains("● connected"));
    }

    /// Style of the first filled status dot (`●`) in a rendered frame.
    fn dot_style(buf: &ratatui::buffer::Buffer) -> Style {
        let i = buf
            .content()
            .iter()
            .position(|c| c.symbol() == "●")
            .expect("a filled status dot is rendered");
        buf.content()[i].style()
    }

    /// Render the current frame at a given clock instant and return the style of
    /// its first filled status dot.
    fn dot_at(app: &mut App, unix_secs: i64) -> Style {
        app.now = OffsetDateTime::from_unix_timestamp(unix_secs).unwrap();
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let frame = terminal.draw(|f| render(f, app)).expect("render");
        dot_style(frame.buffer)
    }

    /// The green channel of the first filled dot rendered at `unix_secs` — the
    /// breath modulates a connected (green) dot's luminance, so this tracks the
    /// pulse brightness over time.
    fn dot_green_at(app: &mut App, unix_secs: i64) -> u8 {
        match dot_at(app, unix_secs).fg {
            Some(ratatui::style::Color::Rgb(r, g, b)) => {
                assert_eq!((r, b), (0, 0), "a connected dot stays green");
                g
            }
            other => panic!("the connected dot should breathe in green, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn connected_server_dot_breathes_on_the_tick_clock() {
        // The connected dot in the Browser's Server band pulses: the 2s breath
        // eases its green luminance up and down, dimmest at the epoch start (the
        // trough) and brightest one second in (the peak). The hue is preserved —
        // only the brightness moves — so the pulse stays faithful to the palette.
        let (mut app, _rx) = app_with_connection().await;
        // Pin the dark palette: this test is about the breath (a green dot's
        // luminance over time), and `dot_green_at` asserts a *pure*-green dot,
        // which only the dark palette's `success` (Color::Green) yields — the
        // default gruvbox `success` is an olive RGB, not pure green.
        app.theme = crate::theme::Theme::dark();
        app.screen = Screen::Browser;
        let trough = dot_green_at(&mut app, 0);
        let peak = dot_green_at(&mut app, 1);
        assert!(
            trough < peak,
            "the breath brightens from trough to peak ({trough} < {peak})"
        );
    }

    #[tokio::test]
    async fn connected_connections_row_dot_breathes() {
        // The same breathing rides the Connections list's per-row dot, which is
        // green only for a profile with a live connection. `mock::handle` opens a
        // connection named "prod", so a matching profile reads as connected.
        use crate::config::{Config, ConnectionConfig, RedisProfile};
        let (tx, _rx) = mpsc::channel(64);
        let config = Config {
            connections: vec![ConnectionConfig::Redis(RedisProfile {
                name: "prod".into(),
                host: "127.0.0.1".into(),
                port: 6379,
                db: 0,
                username: None,
                password: None,
                tls: false,
            })],
            ..Default::default()
        };
        let mut app = App::new(
            config,
            std::path::PathBuf::from("/tmp/bt.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        // Pin the dark palette (see `connected_server_dot_breathes_on_the_tick_clock`):
        // the dot is `theme.success`, and `dot_green_at` asserts a pure-green dot.
        app.theme = crate::theme::Theme::dark();
        let handle = mock::handle(1, "prod").await;
        app.connections.push(Connection::new(handle));
        // Home (Connections tab) is the default screen for this test app.
        assert!(app.is_connected("prod"), "the profile reads as connected");
        assert!(
            dot_green_at(&mut app, 0) < dot_green_at(&mut app, 1),
            "the connected row's dot brightens from the breath's trough to its peak"
        );
    }

    /// Style of the last non-blank cell on the footer (last) row — where the
    /// right-aligned status notification is drawn.
    fn footer_status_style(app: &mut App) -> Style {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        let frame = terminal.draw(|f| render(f, app)).expect("render");
        let buf = &frame.buffer;
        let w = buf.area.width as usize;
        let last_row = &buf.content()[(buf.area.height as usize - 1) * w..];
        last_row
            .iter()
            .rev()
            .find(|c| c.symbol() != " ")
            .expect("the footer carries the status text")
            .style()
    }

    #[test]
    fn footer_transient_notification_fades_as_it_nears_expiry() {
        let (mut app, _rx) = test_app();
        let theme = app.theme;
        app.status = Some(Status {
            message: "saved".into(),
            is_error: false,
            kind: StatusKind::Transient,
            shown_at: app.now,
        });

        // Fresh: drawn solid in the success colour.
        assert_eq!(
            footer_status_style(&mut app).fg,
            theme.success.fg,
            "a fresh notification is solid"
        );

        // Backdated into the final stretch of its life: the colour dissolves
        // toward the bar, so it is now an interpolated RGB, no longer flat green.
        app.status.as_mut().unwrap().shown_at = app.now - time::Duration::milliseconds(2700);
        let fading = footer_status_style(&mut app).fg;
        assert!(
            matches!(fading, Some(Color::Rgb(..))),
            "the fading colour interpolates in RGB, got {fading:?}"
        );
        assert_ne!(fading, theme.success.fg, "it has left solid green");
    }

    #[test]
    fn footer_confirm_prompt_does_not_fade() {
        // A confirmation prompt doesn't self-dismiss, so it never fades: even past
        // the transient lifetime it stays in its solid colour.
        let (mut app, _rx) = test_app();
        let theme = app.theme;
        app.status = Some(Status {
            message: "Press d again to delete".into(),
            is_error: false,
            kind: StatusKind::Confirm,
            shown_at: app.now - time::Duration::seconds(10),
        });
        assert_eq!(
            footer_status_style(&mut app).fg,
            theme.success.fg,
            "a confirm prompt is drawn solid regardless of age"
        );
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
    fn footer_lists_flat_key_action_pairs() {
        // The hint row is a flat list of `key action` pairs — no group labels —
        // rendered wide so nothing clips. Each pair stays contiguous (`key action`)
        // and the pairs are joined by ` · `, with a single leading space. Basic
        // navigation (↑↓, Ctrl-arrows, Esc) is omitted, so each row opens straight
        // on its screen-specific action keys and ends on the shared `? help`.
        let cases = [
            (Screen::Home, " Enter connect · a add", "? help"),
            (Screen::Browser, " Enter fold · z all", "? help"),
            (Screen::Recordings, " r rename · dd delete", "? help"),
        ];
        for (screen, head, tail) in cases {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let text = render_lines(&mut app, 160, 8);
            let footer = text.lines().last().unwrap_or_default();
            assert!(
                footer.starts_with(head),
                "{screen:?} footer should open with the flat pairs {head:?}: {footer:?}"
            );
            assert!(
                footer.contains(tail),
                "{screen:?} footer should still list {tail:?}: {footer:?}"
            );
            // The old grouping labels are gone (each is a word followed by a
            // space): the footer is a flat list of action words now.
            for label in ["conn ", "view ", "groups ", "go ", "app "] {
                assert!(
                    !footer.contains(label),
                    "{screen:?} footer must not show the {label:?} group label: {footer:?}"
                );
            }
            // Basic navigation never appears in the footer — it is universal and
            // documented in the help overlay instead.
            for nav in ["↑↓", "Ctrl-↑", "Ctrl-↓", "Ctrl-←", "Ctrl-→", "Esc"] {
                assert!(
                    !footer.contains(nav),
                    "{screen:?} footer must not advertise basic nav {nav:?}: {footer:?}"
                );
            }
        }
    }

    #[test]
    fn footer_offers_palette_help_and_navigation() {
        // Every screen advertises the command palette (`:`) and help (`?`) as
        // their own pairs, so the palette is discoverable without opening help.
        for screen in [Screen::Home, Screen::Browser, Screen::Recordings] {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let pairs = hint_sections(&app);
            let has = |key: &str, action: &str| pairs.iter().any(|(k, a)| *k == key && a == action);
            assert!(
                has(":", "palette"),
                "{screen:?} footer should advertise `: palette`: {pairs:?}"
            );
            assert!(
                has("?", "help"),
                "{screen:?} footer should offer `? help`: {pairs:?}"
            );
        }

        // Both home-area tabs offer a `Tab` switch to the sibling tab. The
        // Browser steps back with Esc, so it carries no tab switch.
        let expected = [
            (Screen::Home, "recordings"),
            (Screen::Recordings, "connections"),
        ];
        for (screen, tab_target) in expected {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let pairs = hint_sections(&app);
            let has = |key: &str, action: &str| pairs.iter().any(|(k, a)| *k == key && a == action);
            assert!(
                has("Tab", tab_target),
                "{screen:?} footer should switch tabs to {tab_target:?}: {pairs:?}"
            );
        }

        // The `b` jump to the browser was removed: no footer advertises it.
        for screen in [Screen::Home, Screen::Browser, Screen::Recordings] {
            let (mut app, _rx) = test_app();
            app.screen = screen;
            let pairs = hint_sections(&app);
            assert!(
                !pairs
                    .iter()
                    .any(|(k, action)| *k == "b" || action == "browser"),
                "{screen:?} footer must not offer a `b browser` jump: {pairs:?}"
            );
        }
    }

    #[test]
    fn recordings_footer_follows_the_focused_pane() {
        // List focused (the default): the list-only controls (rename / delete)
        // appear. Movement (↑↓) and focus steps (Ctrl-←/→) are basic nav, so the
        // footer never advertises them.
        let (mut app, _rx) = test_app();
        app.screen = Screen::Recordings;
        app.recordings_focus = RecordingsFocus::List;
        let pairs = hint_sections(&app);
        let has = |pairs: &[(&'static str, String)], key: &str, action: &str| {
            pairs.iter().any(|(k, a)| *k == key && a == action)
        };
        let no_nav = |pairs: &[(&'static str, String)]| {
            !pairs
                .iter()
                .any(|(k, _)| k.contains('↑') || k.contains("Ctrl-") || *k == "Esc")
        };
        assert!(has(&pairs, "r", "rename"), "list footer renames: {pairs:?}");
        assert!(
            has(&pairs, "dd", "delete"),
            "list footer deletes: {pairs:?}"
        );
        assert!(no_nav(&pairs), "list footer drops basic nav: {pairs:?}");

        // Viewer focused: the list-only controls (rename / delete) drop off; only
        // the shared keys remain. Scrolling (↑↓) and the focus step (Ctrl-←) are
        // basic nav and stay out of the footer.
        app.recordings_focus = RecordingsFocus::Viewer;
        let pairs = hint_sections(&app);
        assert!(
            !has(&pairs, "r", "rename"),
            "viewer footer drops rename: {pairs:?}"
        );
        assert!(no_nav(&pairs), "viewer footer drops basic nav: {pairs:?}");

        // Palette / help / the tab switch stay available regardless of focus.
        assert!(
            has(&pairs, ":", "palette")
                && has(&pairs, "?", "help")
                && has(&pairs, "Tab", "connections")
        );
    }

    #[test]
    fn footer_styles_keys_bold_and_actions_dim() {
        // Keys carry the heading style (bold accent); action words are dimmed, so
        // the row scans key-first now that the group labels are gone.
        let (mut app, _rx) = test_app();
        let theme = app.theme;
        let mut terminal = Terminal::new(TestBackend::new(160, 8)).unwrap();
        let frame = terminal.draw(|f| render(f, &mut app)).expect("render");
        let buf = frame.buffer;

        // The `Enter connect` pair: `Enter` is a bold, heading-coloured key …
        let k = find_label(buf, "Enter");
        assert_eq!(
            buf.content()[k].style().fg,
            theme.heading.fg,
            "key uses the heading colour"
        );
        assert!(
            buf.content()[k]
                .style()
                .add_modifier
                .contains(ratatui::style::Modifier::BOLD),
            "key is bold"
        );
        // … and its action `connect` is dimmed.
        let a = find_label(buf, "connect");
        assert_eq!(
            buf.content()[a].style().fg,
            theme.dim.fg,
            "action uses the dim colour"
        );
    }

    #[tokio::test]
    async fn browser_footer_advertises_nav_and_has_no_grouping_toggle() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        // Keys are always grouped; the footer advertises Enter to fold the
        // cursor's group, and never a grouping toggle. Movement (↑↓) is basic
        // nav and stays out of the footer.
        let text = render_lines(&mut app, 160, 8);
        assert!(
            text.contains("Enter fold"),
            "browser footer advertises the Enter fold hint"
        );
        assert!(!text.contains("↑↓"), "movement is omitted (basic nav)");
        assert!(!text.contains("p group"), "no `p group` toggle");
        assert!(!text.contains("ungroup"), "no `p ungroup` toggle");
        // With the keys pane focused, the footer advertises Tab to focus the
        // bottom panel (Ctrl-↓ also does, but it is basic nav, so only Tab shows).
        assert!(
            text.contains("Tab panel"),
            "browser footer advertises Tab to focus the panel"
        );
    }

    #[tokio::test]
    async fn bottom_panel_footer_shows_nav_and_only_relevant_tab_keys() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].focus = PaneFocus::Bottom;
        // A live pub/sub tail gives a closeable `Sub` tab. It sits at index 5 —
        // after the five fixed anchors, a Channel tail follows the Pub/Sub anchor.
        let mut sub = Subscription::new(1, SubSpec::Channel("c".into()), 100);
        sub.state = SubState::Active;
        app.connections[0].subs.push(sub);

        let has = |pairs: &[(&'static str, String)], key: &str, action: &str| {
            pairs.iter().any(|(k, a)| *k == key && a == action)
        };

        // Every bottom tab leads with the Tab nav hint and never advertises basic
        // navigation (the Ctrl-↑↓ focus move, ↑↓ scroll, or Esc back), nor pages,
        // nor carries the old Esc-to-keys step (Esc leaves the Browser now).
        for tab in [0usize, 1, 2, 5] {
            app.connections[0].panel_tab = tab;
            let pairs = hint_sections(&app);
            assert!(
                has(&pairs, "Tab", "nav"),
                "tab {tab} footer carries the Tab nav hint: {pairs:?}"
            );
            assert!(
                !pairs
                    .iter()
                    .any(|(k, _)| k.contains('↑') || k.contains("Ctrl-↑") || *k == "Esc"),
                "tab {tab} footer drops basic nav: {pairs:?}"
            );
            assert!(
                !pairs.iter().any(|(k, _)| k.contains("PgUp")),
                "tab {tab} footer drops PgUp/PgDn: {pairs:?}"
            );
            assert!(
                !pairs.iter().any(|(_, a)| a == "keys"),
                "tab {tab} footer drops the Esc-keys hint: {pairs:?}"
            );
        }

        // Server Details (tab 0): a passive overview — only Tab nav + help (its
        // ↑↓ client-list scroll is basic nav, so it is not advertised), and no
        // feed controls.
        app.connections[0].panel_tab = 0;
        let details = hint_sections(&app);
        assert!(
            has(&details, "?", "help"),
            "details offers help: {details:?}"
        );
        assert!(
            !has(&details, "↑↓", "scroll"),
            "details does not advertise scrolling (basic nav): {details:?}"
        );
        for (k, a) in [("p", "play/pause"), ("r", "rec"), ("x", "close")] {
            assert!(!has(&details, k, a), "details hides `{k} {a}`: {details:?}");
        }

        // Console (tab 1): runs commands, no p/r/x.
        app.connections[0].panel_tab = 1;
        let console = hint_sections(&app);
        assert!(has(&console, "Enter", "run"), "console runs: {console:?}");
        for (k, a) in [("p", "play/pause"), ("r", "rec"), ("x", "close")] {
            assert!(!has(&console, k, a), "console hides `{k} {a}`: {console:?}");
        }

        // Monitor (tab 2): a fixed live feed — play/pause + record, not closeable.
        app.connections[0].panel_tab = 2;
        let monitor = hint_sections(&app);
        assert!(
            has(&monitor, "p", "play/pause"),
            "monitor pauses: {monitor:?}"
        );
        assert!(has(&monitor, "r", "rec"), "monitor records: {monitor:?}");
        assert!(
            !has(&monitor, "x", "close"),
            "a fixed anchor is not closeable: {monitor:?}"
        );

        // The pub/sub tail (tab 5): a live feed that can also be closed.
        app.connections[0].panel_tab = 5;
        let tail = hint_sections(&app);
        assert!(has(&tail, "p", "play/pause"), "tail pauses: {tail:?}");
        assert!(has(&tail, "r", "rec"), "tail records: {tail:?}");
        assert!(
            has(&tail, "x", "close"),
            "a runtime tail is closeable: {tail:?}"
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
        assert!(text.contains("Moving around"));
        assert!(text.contains("Global keys"));
        // RabbitMQ's bottom tabs and publish's routing-key step are covered.
        assert!(text.contains("bottom: Details · Connections · Consumers"));
        assert!(text.contains("fanout/headers exchanges skip the routing key"));
        // The whole overlay fits a 30-row terminal: its last row isn't cut.
        assert!(text.contains("Esc back · Ctrl-C quit"), "{text}");
    }

    #[test]
    fn command_palette_overlay_renders_its_one_command() {
        let (mut app, _rx) = test_app();
        app.palette = Some(crate::app::PaletteState::default());
        let text = screen_text(&mut app);
        assert!(
            text.contains("Command palette"),
            "the palette title renders"
        );
        assert!(text.contains("Settings page"), "its one command renders");
    }

    #[test]
    fn settings_overlay_renders_the_theme_and_animation_options() {
        let (mut app, _rx) = test_app();
        app.settings = Some(crate::app::SettingsState::default());
        let text = screen_text(&mut app);
        assert!(text.contains("Settings"), "the settings title renders");
        assert!(text.contains("Theme"), "the theme option renders");
        // The default (unset) base reads as the first cycle entry, "gruvbox".
        assert!(text.contains("gruvbox"), "current theme shown");
        // The animation option renders alongside it, at its default (on).
        assert!(text.contains("Animations"), "the animation option renders");
        assert!(text.contains("‹ on ›"), "current animation setting shown");
        // The queue peek mode option renders at its default (browse), its
        // value clear of the label.
        assert!(
            text.contains("Queue peek mode  ‹ browse ›"),
            "the peek mode option renders"
        );
    }

    #[test]
    fn connection_form_overlay_renders() {
        let (mut app, _rx) = test_app();
        app.form = Some(ConnForm::new());
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("Add connection"));
        assert!(text.contains("Password"));
        assert!(text.contains("Type"), "the broker-type toggle renders");
        // Redis is database-scoped, so its DB row and consolidated note show.
        assert!(text.contains("DB:"), "the Redis DB row renders");
        assert!(
            text.contains("DB selects the database index"),
            "the consolidated Redis note renders"
        );
    }

    #[test]
    fn edit_connection_form_shows_edit_title_and_delete_hint() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.editing = Some(0); // edit mode
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("Edit connection"), "edit title shown");
        assert!(
            text.contains("Ctrl-D delete"),
            "the delete hint shows in edit mode"
        );
        // The add form's title and "save & connect" footer give way to the edit
        // wording.
        assert!(!text.contains("Add connection"));
    }

    #[test]
    fn add_connection_form_omits_the_delete_hint() {
        let (mut app, _rx) = test_app();
        app.form = Some(ConnForm::new()); // add mode (editing = None)
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("Add connection"));
        assert!(
            !text.contains("Ctrl-D delete"),
            "the add form has nothing to delete"
        );
    }

    #[test]
    fn edit_form_shows_a_delete_confirmation_when_armed() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.editing = Some(0);
        form.confirm_delete = true;
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(
            text.contains("Press Ctrl-D again"),
            "the delete confirmation prompt is shown while armed"
        );
    }

    #[test]
    fn connection_form_amqp_omits_db_row_and_shows_note() {
        let (mut app, _rx) = test_app();
        let mut form = ConnForm::new();
        form.cycle_type(true); // -> AMQP
        app.form = Some(form);
        app.mode = InputMode::Form;
        let text = screen_text(&mut app);
        assert!(text.contains("[amqp]"));
        // The consolidated note replaces the per-type blurb …
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
        form.cycle_type(true); // Redis -> AMQP
        form.cycle_type(true); // AMQP  -> RabbitMQ
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
                    destinations: Vec::new(),
                    management_url: None,
                    management_username: None,
                    management_password: None,
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
        assert!(text.contains("TYPE"));
        assert!(text.contains("ENDPOINT"));
        assert!(text.contains("INFO"));
        // … and the broker type is its own (unbracketed) column value.
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
        let handle = mock::handle(1, "cache").await;
        let mut conn = Connection::new(handle);
        conn.dashboard.stats = Some(BrokerStats::Redis(ServerStats {
            redis_version: Some("7.4.0".into()),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(120),
            used_memory: Some(1024 * 1024),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        }));
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
                destinations: Vec::new(),
                management_url: None,
                management_username: None,
                management_password: None,
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
        let handle = mock::handle(1, "prod").await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        (app, rx)
    }

    /// An app with one AMQP connection seeded with a couple of curated
    /// destinations and a pinned peek result, for the AMQP-browser snapshot.
    async fn app_with_amqp_connection() -> (App, Receiver<AppEvent>) {
        use crate::app::{DestKind, Destination};
        let (mut app, rx) = test_app();
        let handle = mock::amqp_handle(1, "events").await;
        let mut conn = Connection::new(handle);
        conn.add_destination(Destination::new("orders", DestKind::Queue));
        conn.add_destination(Destination::new("alerts", DestKind::Topic));
        conn.destinations.table.select(Some(0));
        // A pinned peek result for the selected queue (`orders`).
        conn.peek.peeked = Some(SubSpec::Queue("orders".into()));
        conn.peek.events = vec![
            BrokerEvent {
                ts: OffsetDateTime::UNIX_EPOCH,
                source: "orders".into(),
                payload: Payload::Utf8("order #1".into()),
                meta: Vec::new(),
            },
            BrokerEvent {
                ts: OffsetDateTime::UNIX_EPOCH,
                source: "orders".into(),
                payload: Payload::Json(r#"{"id":2}"#.into()),
                meta: Vec::new(),
            },
        ];
        app.connections.push(conn);
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
        use crate::broker::actor::{spawn_connection, SpawnParams};
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
        let handle = spawn_connection(SpawnParams {
            id: ConnId(1),
            name: "it-render".into(),
            addr: format!("127.0.0.1:{port}"),
            conn: Box::new(RedisConnection::new(profile, None, 64 * 1024)),
            events: tx,
            tracker: &tracker,
            parent_cancel: &cancel,
            recordings_dir: std::env::temp_dir(),
        })
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
                if b.phase == ScanPhase::InProgress && !b.keys.is_empty() {
                    rendered_mid_scan = true;
                }
                if b.phase == ScanPhase::Complete {
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
    async fn browser_band_identifies_the_connection() {
        // The Server band atop the Browser identifies the live connection — its
        // name, address, and the db in view — while the statistics now live in
        // the Server Details tab below (the default bottom-panel tab, drawn here
        // too, so the moved figures are on screen).
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].dashboard.stats = Some(BrokerStats::Redis(ServerStats {
            redis_version: Some("7.4.0".into()),
            connected_clients: Some(7),
            used_memory: Some(1024),
            maxmemory: Some(4096),
            keyspace_hits: Some(3),
            keyspace_misses: Some(1),
            db_keys: vec![(0, 9)],
            ..Default::default()
        }));
        let text = screen_text(&mut app);
        // Band: title, connection name, address, and the db in view.
        assert!(text.contains("Server"), "the band has a title");
        assert!(text.contains("prod"), "the connection name");
        assert!(text.contains("127.0.0.1:6379"), "the address");
        assert!(text.contains("db0"), "the db in view");
        // Statistics moved down into the Server Details tab.
        assert!(text.contains("7.4.0"), "version moved to Server Details");
        assert!(
            text.contains("clients"),
            "client count moved to Server Details"
        );
        assert!(text.contains("hit"), "hit ratio moved to Server Details");
    }

    #[tokio::test]
    async fn server_details_shows_total_keys_across_dbs() {
        // The total key count, summed across DBs, is carried as a figure on the
        // Server Details facts line (the Keys time-series graph was retired in
        // favour of the network graph).
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser; // Server Details is the default bottom tab.
        app.connections[0].dashboard.stats = Some(BrokerStats::Redis(ServerStats {
            redis_version: Some("7.4.0".into()),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        }));
        let text = screen_text(&mut app);
        assert!(text.contains("49 keys"), "the summed total across DBs");
    }

    #[tokio::test]
    async fn browser_band_renders_without_stats() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        // No stats yet: the band still identifies the connection (it no longer
        // depends on stats), while the Server Details tab notes it is collecting.
        let text = screen_text(&mut app);
        assert!(text.contains("prod"), "the band identifies the connection");
        assert!(text.contains("127.0.0.1:6379"), "with its address");
        assert!(
            text.contains("Collecting server details"),
            "the details tab waits for the first stats reply"
        );
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
        // Focus the tail's tab: pub/sub tails sit after the five leading anchors
        // (Server Details, Console, Monitor, Keyspace, Pub/Sub), so the first one
        // is slot 5.
        app.connections[0].panel_tab = 5;
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
        app.connections[0].panel_tab = 5; // the pub/sub tail tab
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
        app.connections[0].panel_tab = 5; // the pub/sub tail tab
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
        app.connections[0].panel_tab = 1; // Console active
        let text = screen_text(&mut app);
        assert!(
            text.contains("Details"),
            "the Server Details anchor is listed"
        );
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
        // The keyspace feed renders under its anchor (slot 3), not its own tab.
        app.connections[0].panel_tab = 3;
        let text = screen_text(&mut app);
        assert!(text.contains("Keyspace"), "the Keyspace anchor renders");
        assert!(text.contains("disabled"), "the notice banner renders");
    }

    #[tokio::test]
    async fn browser_console_band_renders_prompt_and_entries() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.connections[0].panel_tab = 1; // the Console tab
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
        app.connections[0].panel_tab = 1; // the Console tab
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
                    destinations: Vec::new(),
                    management_url: None,
                    management_username: None,
                    management_password: None,
                }),
                ConnectionConfig::Rabbitmq(RabbitmqProfile {
                    name: "rabbit".into(),
                    host: "rabbit".into(),
                    port: 5672,
                    vhost: "prod".into(),
                    username: None,
                    password: None,
                    tls: false,
                    management_url: None,
                    management_username: None,
                    management_password: None,
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
        let handle = mock::handle(1, "cache").await;
        let mut conn = Connection::new(handle);
        conn.dashboard.stats = Some(BrokerStats::Redis(ServerStats {
            redis_version: Some("7.4.0".into()),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(120),
            used_memory: Some(1024 * 1024),
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        }));
        app.connections.push(conn);
        insta::assert_snapshot!("connections_populated", render_lines(&mut app, 120, 12));
    }

    #[test]
    fn snapshot_help_overlay() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        app.show_help = true;
        insta::assert_snapshot!("help_overlay", render_lines(&mut app, 90, 40));
    }

    #[test]
    fn snapshot_command_palette() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        app.palette = Some(crate::app::PaletteState::default());
        insta::assert_snapshot!("command_palette", render_lines(&mut app, 90, 16));
    }

    #[test]
    fn snapshot_settings_overlay() {
        let (mut app, _rx) = test_app();
        pin_clock(&mut app);
        // Default config (no base set) reads as the first cycle entry, "gruvbox".
        app.settings = Some(crate::app::SettingsState::default());
        insta::assert_snapshot!("settings_overlay", render_lines(&mut app, 90, 16));
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
        app.connections[0].browser.phase = ScanPhase::Complete;
        app.connections[0].browser.table.select(Some(0));
        // Show the Console tab (Server Details is the leftmost/default tab now).
        app.connections[0].panel_tab = 1;
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
    async fn snapshot_browser_amqp() {
        // The AMQP browser: a curated destination list (left), the queue-peek
        // pane showing the selected queue's messages (right), and a tail-only
        // bottom panel (just the Tail anchor, no Redis anchors).
        let (mut app, _rx) = app_with_amqp_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        insta::assert_snapshot!("browser_amqp", render_lines(&mut app, 90, 24));
    }

    #[tokio::test]
    async fn snapshot_browser_amqp_message_preview() {
        // The AMQP message list (upper-right) over the body preview (lower-right):
        // selecting the second (JSON) message shows its metadata (here a
        // content-type + an application property) and its pretty-printed JSON body
        // in the preview — the analog of the Redis key list + value pane.
        let (mut app, _rx) = app_with_amqp_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        // Give the second (JSON) message some metadata and select it.
        {
            let peek = &mut app.connections[0].peek;
            peek.events[1].meta = vec![
                ("id".into(), "m-42".into()),
                ("content-type".into(), "application/json".into()),
                ("app.region".into(), "eu".into()),
            ];
            peek.focused = true;
            peek.selected = 1;
        }
        insta::assert_snapshot!(
            "browser_amqp_message_preview",
            render_lines(&mut app, 90, 24)
        );
    }

    /// An app on the Browser for a RabbitMQ connection named `rabbit` (with a
    /// saved profile on vhost `shop`), whose listing returned `result`.
    async fn rabbitmq_browser(
        result: Option<Result<DestinationList, String>>,
    ) -> (App, Receiver<AppEvent>) {
        rabbitmq_browser_in(crate::config::PeekMode::Browse, result).await
    }

    /// [`rabbitmq_browser`] with the peek mode set to `mode`.
    async fn rabbitmq_browser_in(
        mode: crate::config::PeekMode,
        result: Option<Result<DestinationList, String>>,
    ) -> (App, Receiver<AppEvent>) {
        use crate::config::{ConnectionConfig, RabbitmqProfile};
        let mut config = Config {
            connections: vec![ConnectionConfig::Rabbitmq(RabbitmqProfile {
                name: "rabbit".into(),
                host: "127.0.0.1".into(),
                port: 5672,
                vhost: "shop".into(),
                username: None,
                password: None,
                tls: false,
                management_url: None,
                management_username: None,
                management_password: None,
            })],
            ..Default::default()
        };
        config.settings.peek_mode = mode;
        let (tx, rx) = mpsc::channel(64);
        let mut app = App::new(
            config,
            std::path::PathBuf::from("/tmp/keyhole-ui-test.toml"),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        let handle = mock::rabbitmq_handle(1, "rabbit").await;
        app.connections.push(Connection::new(handle));
        app.active = Some(0);
        app.screen = Screen::Browser;
        pin_clock(&mut app);
        if let Some(result) = result {
            app.handle_event(AppEvent::DestinationsListed {
                id: ConnId(1),
                result,
            });
        }
        (app, rx)
    }

    /// Put the RabbitMQ list's cursor on the row named `name`.
    fn select_destination(app: &mut App, name: &str) {
        let conn = app.active_conn_mut().unwrap();
        let i = conn
            .destinations
            .items
            .iter()
            .position(|d| d.name == name)
            .unwrap();
        conn.destinations.table.select(Some(i));
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq() {
        // The full RabbitMQ list on a wide terminal: queues then exchanges with
        // every column, the selected queue's details on the right, and the
        // footer's r/t keys (none of AMQP's curated-list keys).
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        insta::assert_snapshot!("browser_rabbitmq", render_lines(&mut app, 160, 30));
    }

    #[tokio::test]
    async fn the_tail_anchor_shows_why_it_refused_the_last_spec() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        app.connections[0].focus = PaneFocus::Bottom;
        app.mode = InputMode::Subscribe;
        let refusal = "binding key too long (max 255 bytes)";
        assert!(!render_lines(&mut app, 160, 30).contains(refusal));

        app.subscribe_error = Some(refusal.into());
        let text = render_lines(&mut app, 160, 30);
        let lines: Vec<&str> = text.lines().collect();
        let prompt = lines
            .iter()
            .position(|l| l.contains("exchange:name or exchange:name/binding-key:"))
            .expect("the Tail anchor's prompt");
        assert!(
            lines[prompt + 1].contains(refusal),
            "under the prompt:\n{text}"
        );
        assert!(
            lines[prompt + 2..]
                .iter()
                .any(|l| l.contains("Enter taps it")),
            "the hint stays:\n{text}"
        );
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_narrow() {
        // A narrow terminal drops the stat columns from the right, keeping Name,
        // Kind and Msgs; the selected topic exchange's details say how `t` taps
        // it.
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        select_destination(&mut app, "keyhole.demo");
        insta::assert_snapshot!("browser_rabbitmq_narrow", render_lines(&mut app, 90, 24));
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_unavailable() {
        // The management API couldn't be reached: the pane says where and why,
        // that tails still work, and how to retry or point it elsewhere.
        let (mut app, _rx) = rabbitmq_browser(Some(Err(
            "management API unavailable at http://127.0.0.1:15672: could not reach the \
             management API: Connection refused (os error 111)"
                .into(),
        )))
        .await;
        insta::assert_snapshot!(
            "browser_rabbitmq_unavailable",
            render_lines(&mut app, 90, 24)
        );
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_no_metrics() {
        // Metrics disabled on the broker: every count and rate reads `—`, and a
        // notice above the table says why.
        let mut list = mock::sample_destinations();
        for d in &mut list.items {
            (d.messages, d.ready, d.unacked, d.consumers, d.publish_rate) =
                (None, None, None, None, None);
        }
        list.stats_disabled = true;
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(list))).await;
        insta::assert_snapshot!(
            "browser_rabbitmq_no_metrics",
            render_lines(&mut app, 160, 20)
        );
    }

    #[tokio::test]
    async fn rabbitmq_pane_states_before_and_without_rows() {
        let (mut app, _rx) = rabbitmq_browser(None).await;
        assert!(screen_text(&mut app).contains("Listing queues and exchanges…"));

        let (mut app, _rx) = rabbitmq_browser(Some(Ok(DestinationList::default()))).await;
        assert!(
            screen_text(&mut app).contains("No queues or exchanges in vhost shop."),
            "the empty list names the profile's vhost"
        );
    }

    #[tokio::test]
    async fn rabbitmq_pane_notes_a_truncated_list_and_a_failed_refresh() {
        let mut list = mock::sample_destinations();
        list.truncated = true;
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(list))).await;
        let text = render_lines(&mut app, 160, 24);
        assert!(
            text.contains("Showing the first 5,000 of each kind."),
            "{text}"
        );

        // A later refresh fails: the rows stay, under the reason.
        app.handle_event(AppEvent::DestinationsListed {
            id: ConnId(1),
            result: Err("management API unavailable at http://h:15672: timed out".into()),
        });
        let text = render_lines(&mut app, 160, 24);
        assert!(
            text.contains("Refresh failed: management API unavailable at http://h:15672"),
            "{text}"
        );
        assert!(text.contains("Press r to retry."), "{text}");
        assert!(text.contains("keyhole.demo"), "the rows stay: {text}");
    }

    /// The first rendered pane row (not a title) that holds `prefix`.
    fn row_with<'a>(text: &'a str, prefix: &str) -> &'a str {
        text.lines()
            .find(|l| l.starts_with('│') && l.contains(prefix))
            .unwrap_or_else(|| panic!("no row holds {prefix:?}: {text}"))
    }

    #[tokio::test]
    async fn rabbitmq_list_marks_a_cut_off_name_with_an_ellipsis() {
        // A name wider than the Name column ends in `…` at the column's edge
        // and the columns beside it stay put, whether the terminal is wide,
        // narrow enough to drop most stat columns, or too narrow for any.
        let long = "orders.priority.eu-west-1.fulfilment.retry.dead-letter";
        let mut list = mock::sample_destinations();
        list.items.insert(
            0,
            mock::queue_row(long, crate::broker::QueueType::Classic, 7),
        );
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(list))).await;
        insta::assert_snapshot!("browser_rabbitmq_long_name", render_lines(&mut app, 90, 16));
        for (width, cut) in [
            (
                160,
                "│▶ orders.priority.eu-we…  queue          7            7/0",
            ),
            (90, "│▶ orders.priority.eu-wes…  queue          7│"),
            (60, "│▶ orders.priority.eu-west-1…│"),
        ] {
            let text = render_lines(&mut app, width, 12);
            let row = row_with(&text, "orders.priority");
            assert!(row.contains(cut), "at {width} columns: {row:?}");
        }
        // A name that fits is left whole.
        let text = render_lines(&mut app, 160, 12);
        assert!(row_with(&text, "1960/20").contains("  orders  "), "{text}");
    }

    /// The cells of the first pane row (not a title) holding `prefix`, one
    /// symbol per screen column.
    fn row_cells(app: &mut App, width: u16, height: u16, prefix: &str) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let frame = terminal.draw(|f| render(f, app)).expect("render");
        frame
            .buffer
            .content()
            .chunks(usize::from(width))
            .map(|row| row.iter().map(|c| c.symbol().to_string()).collect())
            .find(|cells: &Vec<String>| cells[0] == "│" && cells.concat().contains(prefix))
            .unwrap_or_else(|| panic!("no row holds {prefix:?}"))
    }

    #[tokio::test]
    async fn rabbitmq_list_cuts_names_at_the_name_columns_edge() {
        use crate::broker::QueueType;
        let names = [
            "orders.priority.eu-west",
            "orders.priority.eu-west1",
            "alerts.⚠️.warning.⚠️.critical.dead",
            "注文.優先.欧州西部.再試行",
        ];
        let list = DestinationList {
            items: names
                .iter()
                .map(|n| mock::queue_row(n, QueueType::Classic, 7))
                .collect(),
            ..Default::default()
        };
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(list))).await;
        // At 90 columns the Name column is the 23 cells from 3; Kind starts
        // at 28 on every row.
        let row = |app: &mut App, prefix: &str| {
            let cells = row_cells(app, 90, 16, prefix);
            assert_eq!(cells[28..33].concat(), "queue", "Kind stays in line");
            (cells[3..26].concat(), cells[25].clone())
        };
        let (name, _) = row(&mut app, "eu-west  ");
        assert_eq!(name, "orders.priority.eu-west", "exactly the column: whole");
        let (name, _) = row(&mut app, "eu-wes…");
        assert_eq!(name, "orders.priority.eu-wes…", "a column over: cut");
        // `⚠️` takes two columns (the second cell blank), though its base
        // character alone is one wide.
        let (_, last) = row(&mut app, "alerts.");
        assert_eq!(last, "…", "the emoji name's ellipsis is on the last column");
        // A wide character that would straddle the edge is cut too.
        let (name, last) = row(&mut app, "注");
        assert!(name.contains("再 …"), "{name:?}");
        assert_eq!(last, " ");
    }

    #[tokio::test]
    async fn amqp_list_marks_a_cut_off_name_with_an_ellipsis() {
        use crate::app::Destination;
        let (mut app, _rx) = app_with_amqp_connection().await;
        app.screen = Screen::Browser;
        let long = "orders.priority.eu-west-1.fulfilment.retry.dead-letter";
        app.connections[0].add_destination(Destination::new(long, DestKind::Queue));
        for (width, cut) in [
            (90, "│▶ orders.priority.eu-west-1.fulf…  [queue] │"),
            (60, "│▶ orders.priority…  [queue] │"),
        ] {
            let text = render_lines(&mut app, width, 24);
            let row = row_with(&text, "orders.priority");
            assert!(row.contains(cut), "at {width} columns: {row:?}");
        }
    }

    /// RabbitMQ stats like the 3.13 fixtures': an administrator's view of a
    /// one-node cluster with live rates.
    fn sample_rabbitmq_stats() -> RabbitmqStats {
        RabbitmqStats {
            rabbitmq_version: Some("3.13.7".into()),
            erlang_version: Some("26.2.5.16".into()),
            cluster_name: Some("rabbit@19942ffdc832".into()),
            stats_disabled: false,
            cluster_wide: true,
            publish_rate: Some(9.6),
            deliver_rate: Some(14.4),
            ack_rate: Some(4.8),
            messages: Some(1998),
            messages_ready: Some(1977),
            messages_unacked: Some(21),
            queues: Some(18),
            connections: Some(2),
            consumers: Some(4),
            nodes: vec![NodeHealth {
                name: "rabbit@19942ffdc832".into(),
                running: Some(true),
                uptime: Some(653),
                mem_used: Some(201_236_480),
                mem_limit: Some(13_226_541_056),
                mem_alarm: Some(false),
                fd_used: Some(52),
                fd_total: Some(1_048_576),
                disk_free: Some(134_650_884_096),
                disk_free_limit: Some(50_000_000),
                disk_free_alarm: Some(false),
                partitions: Vec::new(),
            }],
        }
    }

    /// [`rabbitmq_browser`] whose management API answered the probe (so it
    /// has the Server band and the Server Details tab), after a few stats
    /// refreshes ending in `stats`.
    async fn rabbitmq_dashboard(stats: RabbitmqStats) -> (App, Receiver<AppEvent>) {
        let (mut app, rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        let conn = &mut app.connections[0];
        conn.caps.can_dashboard = true;
        for (publish, deliver) in [(4.0, 6.0), (7.2, 9.0), (12.0, 15.4), (8.0, 11.0)] {
            conn.dashboard.record(BrokerStats::Rabbitmq(RabbitmqStats {
                publish_rate: Some(publish),
                deliver_rate: Some(deliver),
                ..Default::default()
            }));
        }
        conn.dashboard.record(BrokerStats::Rabbitmq(stats));
        (app, rx)
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_server_details() {
        // The Server band names the vhost (not a Redis db), and Server Details
        // (the first tab) carries the versions, totals and node health over
        // the publish and deliver graphs, across the full width.
        let (mut app, _rx) = rabbitmq_dashboard(sample_rabbitmq_stats()).await;
        insta::assert_snapshot!(
            "browser_rabbitmq_server_details",
            render_lines(&mut app, 160, 40)
        );
    }

    #[tokio::test]
    async fn rabbitmq_band_names_the_vhost_and_warns_on_an_alarm() {
        let (mut app, _rx) = rabbitmq_dashboard(sample_rabbitmq_stats()).await;
        let text = screen_text(&mut app);
        assert!(
            text.contains("rabbit · 127.0.0.1:5672 · vhost shop"),
            "{text}"
        );
        assert!(!text.contains("db0"), "RabbitMQ has no db");
        assert!(text.contains("connected"));
        assert!(text.contains("no alarms"));

        let mut alarmed = sample_rabbitmq_stats();
        alarmed.nodes[0].mem_alarm = Some(true);
        let (mut app, _rx) = rabbitmq_dashboard(alarmed).await;
        let text = screen_text(&mut app);
        assert!(
            text.contains("● alarm"),
            "the dot turns to a warning: {text}"
        );
        assert!(text.contains("memory alarm"), "the node line says which");
        let band = app.connections[0].dashboard.stats.clone();
        assert!(band
            .as_ref()
            .and_then(BrokerStats::rabbitmq)
            .unwrap()
            .has_alarm());
    }

    #[tokio::test]
    async fn rabbitmq_band_dot_is_a_warning_colour_on_an_alarm() {
        let mut alarmed = sample_rabbitmq_stats();
        alarmed.nodes[0].disk_free_alarm = Some(true);
        let (mut app, _rx) = rabbitmq_dashboard(alarmed).await;
        app.health = ConnHealth::Connected;
        // Steady in the warning colour, at any instant (no breathing).
        let warning = app.theme.warning;
        assert_eq!(dot_at(&mut app, 0).fg, warning.fg);
        assert_eq!(dot_at(&mut app, 1).fg, warning.fg);
    }

    #[tokio::test]
    async fn rabbitmq_server_details_for_a_management_user() {
        // No `monitoring` tag: no node health, and the totals are the user's
        // vhosts' (its own connections aren't a server total).
        let stats = RabbitmqStats {
            cluster_wide: false,
            connections: None,
            consumers: None,
            nodes: Vec::new(),
            ..sample_rabbitmq_stats()
        };
        let (mut app, _rx) = rabbitmq_dashboard(stats).await;
        let text = screen_text(&mut app);
        assert!(
            text.contains("your vhosts: 1998 msgs · 1977 ready · 21 unacked · 18 queues"),
            "{text}"
        );
        assert!(!text.contains("connections"));
        assert!(text.contains("node health needs the monitoring tag"));
        // The rates still come through, and cover the same vhosts.
        assert!(text.contains("your vhosts: Publish/s 9.6"), "{text}");
        assert!(text.contains("your vhosts: Deliver/s 14.4"), "{text}");

        let (mut app, _rx) = rabbitmq_dashboard(sample_rabbitmq_stats()).await;
        let text = screen_text(&mut app);
        assert!(
            !text.contains("your vhosts"),
            "a monitoring user's are cluster-wide"
        );
    }

    #[tokio::test]
    async fn rabbitmq_server_details_with_metrics_disabled() {
        // The overview keeps its versions and counts; rates, message totals and
        // node figures are gone, and the tab says why.
        let stats = RabbitmqStats {
            stats_disabled: true,
            publish_rate: None,
            deliver_rate: None,
            ack_rate: None,
            messages: None,
            messages_ready: None,
            messages_unacked: None,
            consumers: None,
            nodes: vec![NodeHealth {
                name: "rabbit@f19b542c61db".into(),
                running: Some(true),
                ..Default::default()
            }],
            ..sample_rabbitmq_stats()
        };
        let (mut app, _rx) = rabbitmq_dashboard(stats).await;
        let text = screen_text(&mut app);
        assert!(text.contains("metrics collector off"), "{text}");
        assert!(text.contains("18 queues · 2 connections"), "{text}");
        assert!(!text.contains("msgs"));
        assert!(text.contains("rabbit@f19b542c61db · running"), "{text}");
        assert!(text.contains("No message rates"), "{text}");
        assert!(!text.contains("Publish/s"));
    }

    #[tokio::test]
    async fn rabbitmq_server_details_show_a_failed_refresh() {
        let (mut app, _rx) = rabbitmq_dashboard(sample_rabbitmq_stats()).await;
        app.connections[0].dashboard.error =
            Some("management API unavailable at http://127.0.0.1:15672".into());
        let text = screen_text(&mut app);
        assert!(
            text.contains("Server details unavailable: management API unavailable"),
            "{text}"
        );
        assert!(text.contains("r in the list retries"));
        assert!(!text.contains("Publish/s"), "no stale figures");
    }

    #[tokio::test]
    async fn a_failed_rabbitmq_refresh_leaves_the_band_dot_neutral() {
        // The last good stats had an alarm, then a refresh failed: like the
        // Details tab, the band claims neither that stale alarm nor health.
        let mut alarmed = sample_rabbitmq_stats();
        alarmed.nodes[0].mem_alarm = Some(true);
        let (mut app, _rx) = rabbitmq_dashboard(alarmed.clone()).await;
        app.handle_event(AppEvent::StatsUpdated {
            id: ConnId(1),
            result: Err("management API unavailable at http://127.0.0.1:15672".into()),
        });
        let text = screen_text(&mut app);
        assert!(text.contains("● stats unavailable"), "{text}");
        assert!(!text.contains("● alarm") && !text.contains("● connected"));
        // Steady and dim, at any instant (no breathing).
        let dim = app.theme.dim;
        assert_eq!(dot_at(&mut app, 0).fg, dim.fg);
        assert_eq!(dot_at(&mut app, 1).fg, dim.fg);

        // The next good refresh shows the alarm again.
        app.handle_event(AppEvent::StatsUpdated {
            id: ConnId(1),
            result: Ok(BrokerStats::Rabbitmq(alarmed)),
        });
        assert!(screen_text(&mut app).contains("● alarm"));
    }

    // -- rosters ------------------------------------------------------------

    /// `secs` before the pinned test clock.
    fn ago(secs: i64) -> Option<OffsetDateTime> {
        Some(time::macros::datetime!(2026 - 06 - 19 12:34:56 UTC) - time::Duration::seconds(secs))
    }

    /// A broker's connections: keyhole's own (named), an unnamed publisher, and
    /// a blocked client whose rates the broker didn't report.
    fn sample_connections(own_only: bool) -> Roster {
        let conn = |name: &str, client: Option<&str>, user: &str, channels, state: &str| {
            ClientConnection {
                name: name.into(),
                client_name: client.map(str::to_string),
                user: Some(user.into()),
                peer: name.split(" -> ").next().map(str::to_string),
                channels: Some(channels),
                state: Some(state.into()),
                ..Default::default()
            }
        };
        let rows = vec![
            ClientConnection {
                connected_at: ago(185),
                recv_rate: Some(182.4),
                send_rate: Some(2_048.0),
                ..conn(
                    "172.18.0.1:40112 -> 172.18.0.4:5672",
                    Some("keyhole-dev-rabbitmq-0"),
                    "keyhole",
                    1,
                    "running",
                )
            },
            ClientConnection {
                connected_at: ago(7_400),
                recv_rate: Some(1_843.2),
                send_rate: Some(-0.4),
                ..conn(
                    "172.18.0.1:40098 -> 172.18.0.4:5672",
                    None,
                    "keyhole",
                    1,
                    "running",
                )
            },
            ClientConnection {
                connected_at: ago(190_000),
                ..conn(
                    "10.0.4.17:51234 -> 172.18.0.4:5672",
                    Some("orders-api"),
                    "orders",
                    4,
                    "blocked",
                )
            },
        ];
        Roster {
            rows: RosterRows::Connections(rows),
            own_only,
            stats_disabled: false,
        }
    }

    /// Consumers: a manual-ack worker, keyhole's own tap (auto-ack, on a
    /// server-named queue) and a single-active consumer's standby.
    fn sample_consumers() -> Roster {
        let rows = vec![
            QueueConsumer {
                queue: "keyhole.demo.orders".into(),
                tag: "orders-worker-1".into(),
                channel: Some("10.0.4.17:51234 (1)".into()),
                ack_required: Some(true),
                prefetch: Some(10),
                active: Some(true),
                activity_status: Some("up".into()),
            },
            QueueConsumer {
                queue: "amq.gen-JzTY20BRgKO-HjmUJj0wLg".into(),
                tag: "keyhole-3".into(),
                channel: Some("172.18.0.1:40130 (1)".into()),
                ack_required: Some(false),
                prefetch: Some(0),
                active: Some(true),
                activity_status: Some("up".into()),
            },
            QueueConsumer {
                queue: "payments".into(),
                tag: "payments-standby".into(),
                channel: Some("10.0.4.17:51234 (2)".into()),
                ack_required: Some(true),
                prefetch: Some(1),
                active: Some(false),
                activity_status: Some("waiting".into()),
            },
        ];
        Roster {
            rows: RosterRows::Consumers(rows),
            own_only: false,
            stats_disabled: false,
        }
    }

    /// [`rabbitmq_dashboard`] with the keyboard on the `tab` roster tab, which
    /// shows `roster` (`None`: nothing has arrived yet).
    async fn rabbitmq_roster(tab: PanelTab, roster: Option<Roster>) -> (App, Receiver<AppEvent>) {
        let (mut app, rx) = rabbitmq_dashboard(sample_rabbitmq_stats()).await;
        let conn = &mut app.connections[0];
        conn.panel_tab = conn
            .panel_slots()
            .iter()
            .position(|t| *t == tab)
            .expect("a roster tab");
        conn.focus = PaneFocus::Bottom;
        let kind = tab.roster_kind().expect("a roster tab");
        conn.rosters.get_mut(kind).roster = roster;
        (app, rx)
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_connections() {
        // The Connections tab after Server Details: the client's own name, or
        // the broker's address pair when it gave none; a state other than
        // running stands out; missing rates read `—`.
        let (mut app, _rx) =
            rabbitmq_roster(PanelTab::Connections, Some(sample_connections(false))).await;
        insta::assert_snapshot!(
            "browser_rabbitmq_connections",
            render_lines(&mut app, 160, 40)
        );
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_consumers() {
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Consumers, Some(sample_consumers())).await;
        insta::assert_snapshot!(
            "browser_rabbitmq_consumers",
            render_lines(&mut app, 160, 40)
        );
    }

    #[tokio::test]
    async fn roster_tabs_show_their_figures() {
        let (mut app, _rx) =
            rabbitmq_roster(PanelTab::Connections, Some(sample_connections(false))).await;
        let text = render_lines(&mut app, 160, 40);
        assert!(text.contains("Connections (3)"), "{text}");
        assert!(text.contains("keyhole-dev-rabbitmq-0"));
        assert!(
            text.contains("172.18.0.1:40098 -> 172.18.0.4:5672"),
            "unnamed"
        );
        assert!(
            text.contains("182 B/s") && text.contains("2.0 KiB/s"),
            "{text}"
        );
        assert!(text.contains("0 B/s"), "a negative rate reads as zero");
        for age in ["3m", "2h", "2d"] {
            assert!(text.contains(&format!(" {age} │")), "{age} in {text}");
        }
        assert!(!text.contains("Your connections"));
        // Like Server Details, only Tab and help in the footer.
        assert!(text.contains("Tab nav · ? help"), "{text}");
        assert!(!text.contains("play/pause"));

        let (mut app, _rx) = rabbitmq_roster(PanelTab::Consumers, Some(sample_consumers())).await;
        let text = render_lines(&mut app, 160, 40);
        assert!(text.contains("Consumers (3)"), "{text}");
        assert!(text.contains("manual") && text.contains("auto"));
        assert!(text.contains("waiting"), "the standby says why it's idle");
        assert!(text.contains("10.0.4.17:51234 (2)"));
    }

    #[tokio::test]
    async fn a_management_users_roster_is_labelled_as_its_own() {
        let (mut app, _rx) =
            rabbitmq_roster(PanelTab::Connections, Some(sample_connections(true))).await;
        let text = render_lines(&mut app, 160, 40);
        assert!(text.contains("Your connections"), "the tab says so: {text}");
        assert!(
            text.contains("only your own: listing everyone's connections needs the monitoring tag"),
            "{text}"
        );
        let mut own = sample_consumers();
        own.own_only = true;
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Consumers, Some(own)).await;
        assert!(render_lines(&mut app, 160, 40).contains("Your consumers"));
    }

    #[tokio::test]
    async fn roster_tabs_explain_an_empty_or_missing_list() {
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Connections, None).await;
        assert!(screen_text(&mut app).contains("Listing connections…"));

        let none = Roster::empty(RosterKind::Consumers, false, false);
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Consumers, Some(none)).await;
        let text = screen_text(&mut app);
        assert!(
            text.contains("Consumers (0)") && text.contains("no consumers"),
            "{text}"
        );

        // With metrics off the broker lists nothing, and an empty list would
        // mislead.
        let off = Roster::empty(RosterKind::Connections, false, true);
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Connections, Some(off)).await;
        let text = render_lines(&mut app, 160, 40);
        assert!(
            text.contains("No connections: the broker's metrics collector is disabled"),
            "{text}"
        );
        assert!(!text.contains("Connections ("));

        // A failed refresh shows why, not the stale rows.
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Consumers, Some(sample_consumers())).await;
        app.connections[0].rosters.consumers.error =
            Some("management API unavailable at http://127.0.0.1:15672".into());
        let text = render_lines(&mut app, 160, 40);
        assert!(
            text.contains("Can't list consumers: management API unavailable"),
            "{text}"
        );
        assert!(text.contains("r in the list retries"));
        assert!(!text.contains("orders-worker-1"), "no stale rows");
    }

    #[tokio::test]
    async fn a_roster_scrolls_and_an_over_scroll_rests_at_the_end() {
        let rows = (0..40)
            .map(|i| ClientConnection {
                name: format!("client-{i:02}"),
                ..Default::default()
            })
            .collect();
        let roster = Roster {
            rows: RosterRows::Connections(rows),
            own_only: false,
            stats_disabled: false,
        };
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Connections, Some(roster)).await;
        let text = render_lines(&mut app, 160, 40);
        assert!(text.contains("client-00") && !text.contains("client-39"));

        app.connections[0].rosters.connections.scroll = 5;
        let text = render_lines(&mut app, 160, 40);
        assert!(!text.contains("client-04") && text.contains("client-05"));

        app.connections[0].rosters.connections.scroll = u16::MAX;
        let text = render_lines(&mut app, 160, 40);
        assert!(text.contains("client-39"), "the last row shows: {text}");
        assert!(!text.contains("client-00"));
    }

    #[tokio::test]
    async fn roster_tabs_fit_a_narrow_terminal() {
        let (mut app, _rx) =
            rabbitmq_roster(PanelTab::Connections, Some(sample_connections(false))).await;
        let text = render_lines(&mut app, 90, 24);
        assert!(text.contains("Name") && text.contains("Peer") && text.contains("Age"));
        assert!(
            text.contains("keyhole-d… 172.18.0.1:40112"),
            "the name gives way: {text}"
        );
        let (mut app, _rx) = rabbitmq_roster(PanelTab::Consumers, Some(sample_consumers())).await;
        let text = render_lines(&mut app, 90, 24);
        assert!(
            text.contains("Prefetch") && text.contains("Active"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_failed_redis_refresh_keeps_the_band_connected() {
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.handle_event(AppEvent::StatsUpdated {
            id: ConnId(1),
            result: Err("NOPERM INFO".into()),
        });
        let text = screen_text(&mut app);
        assert!(text.contains("● connected"), "{text}");
        assert!(!text.contains("stats unavailable"));
    }

    #[tokio::test]
    async fn rabbitmq_server_details_on_a_fresh_node() {
        // A node that just started (as `overview.fresh.v3` / `nodes.fresh.v3`):
        // versions and zero counts, no message totals, rates or node figures.
        let stats = RabbitmqStats {
            publish_rate: None,
            deliver_rate: None,
            ack_rate: None,
            messages: None,
            messages_ready: None,
            messages_unacked: None,
            queues: Some(0),
            connections: Some(0),
            consumers: Some(0),
            nodes: vec![NodeHealth {
                name: "rabbit@e71afc9e7c51".into(),
                running: Some(true),
                ..Default::default()
            }],
            ..sample_rabbitmq_stats()
        };
        let (mut app, _rx) = rabbitmq_dashboard(stats).await;
        let text = screen_text(&mut app);
        assert!(text.contains("RabbitMQ 3.13.7"), "{text}");
        assert!(
            text.contains("0 queues · 0 connections · 0 consumers"),
            "{text}"
        );
        assert!(!text.contains("msgs") && !text.contains("mem "), "{text}");
        assert!(text.contains("rabbit@e71afc9e7c51 · running"), "{text}");
        assert!(text.contains("Publish/s — · peak"), "{text}");
        assert!(text.contains("Deliver/s — · ack — · peak"), "{text}");
        assert!(text.contains("● connected"), "no alarm: {text}");
    }

    #[tokio::test]
    async fn rabbitmq_graph_captions_show_a_dash_for_a_missing_rate() {
        let stats = RabbitmqStats {
            publish_rate: None,
            deliver_rate: Some(-0.2),
            ack_rate: None,
            ..sample_rabbitmq_stats()
        };
        let (mut app, _rx) = rabbitmq_dashboard(stats).await;
        let text = screen_text(&mut app);
        assert!(text.contains("Publish/s — · peak 12"), "{text}");
        assert!(text.contains("Deliver/s 0.0 · ack — · peak 15"), "{text}");
    }

    #[tokio::test]
    async fn rabbitmq_details_say_what_t_does_for_each_kind() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        for (name, expect) in [
            ("orders", "To watch them, tap an exchange"),
            ("amq.direct", "routing key after exchange:name/"),
            ("amq.fanout", "copies every message"),
            ("amq.headers", "the tap receives every message"),
            ("amq.rabbitmq.trace", "topic exchange (internal)"),
        ] {
            select_destination(&mut app, name);
            let text = render_lines(&mut app, 160, 30);
            assert!(text.contains(expect), "{name}: {text}");
        }
    }

    #[tokio::test]
    async fn rabbitmq_footer_offers_refresh_and_tap_but_not_add_or_remove() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        let text = render_lines(&mut app, 160, 30);
        let footer = text.lines().last().unwrap();
        assert!(
            footer.contains("r refresh") && footer.contains("t tap"),
            "{footer}"
        );
        assert!(
            !footer.contains("a add") && !footer.contains("remove"),
            "{footer}"
        );
    }

    /// The words in the right half of each rendered line (the browser's right
    /// pane), joined by single spaces, so text the pane wraps reads as one
    /// sentence.
    fn right_half_words(text: &str) -> String {
        text.lines()
            .flat_map(|line| {
                let half = line.chars().count() / 2;
                line.chars()
                    .skip(half)
                    .collect::<String>()
                    .replace('│', " ")
                    .split_whitespace()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[tokio::test]
    async fn rabbitmq_details_say_what_p_does_for_each_kind() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        for (name, expect) in [
            ("orders", "Press P to publish a message to it."),
            ("keyhole.demo", "give its routing key, then its body"),
            ("amq.direct", "give its routing key, then its body"),
            ("amq.fanout", "to every queue bound to it"),
            ("amq.headers", "which this publish doesn't set"),
            (
                "amq.rabbitmq.trace",
                "Clients can't publish to an internal exchange.",
            ),
        ] {
            select_destination(&mut app, name);
            let text = render_lines(&mut app, 160, 30);
            assert!(right_half_words(&text).contains(expect), "{name}: {text}");
        }
    }

    #[tokio::test]
    async fn rabbitmq_footer_offers_publish_except_on_an_internal_exchange() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        for (name, offered) in [
            ("orders", true),
            ("keyhole.demo", true),
            ("amq.rabbitmq.trace", false),
        ] {
            select_destination(&mut app, name);
            let text = render_lines(&mut app, 160, 30);
            let footer = text.lines().last().unwrap();
            assert_eq!(footer.contains("P publish"), offered, "{name}: {footer}");
        }
    }

    #[tokio::test]
    async fn rabbitmq_details_show_the_last_publish_to_that_destination() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        app.handle_event(AppEvent::Published {
            id: ConnId(1),
            target: "exchange:keyhole.demo".into(),
            result: Ok(()),
        });
        select_destination(&mut app, "keyhole.demo");
        let text = render_lines(&mut app, 160, 30);
        assert!(
            text.contains("Last publish confirmed by the broker."),
            "{text}"
        );
        select_destination(&mut app, "amq.direct");
        let text = render_lines(&mut app, 160, 30);
        assert!(!text.contains("Last publish"), "another exchange's: {text}");

        // A failure shows in full, beyond what the footer has room for.
        app.handle_event(AppEvent::Published {
            id: ConnId(1),
            target: "exchange:amq.direct".into(),
            result: Err("unroutable: no queue bound for key `order.created.eu-west-1`".into()),
        });
        // The pane wraps it: compare the words.
        let text = render_lines(&mut app, 160, 30);
        assert!(
            right_half_words(&text).contains(
                "Last publish failed — unroutable: no queue bound for key \
                 `order.created.eu-west-1`"
            ),
            "{text}"
        );
    }

    #[tokio::test]
    async fn rabbitmq_publish_prompts_say_where_the_message_goes() {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        let footer = |app: &mut App| {
            render_lines(app, 160, 30)
                .lines()
                .last()
                .unwrap()
                .to_string()
        };

        select_destination(&mut app, "keyhole.demo");
        press(&mut app, KeyEvent::from(KeyCode::Char('P')));
        let prompt = footer(&mut app);
        assert!(
            prompt.contains(" routing key ▏   publish to keyhole.demo · Enter next"),
            "{prompt}"
        );
        for c in "order.test".chars() {
            press(&mut app, KeyEvent::from(KeyCode::Char(c)));
        }
        press(&mut app, KeyEvent::from(KeyCode::Enter));
        let prompt = footer(&mut app);
        assert!(
            prompt.contains(" publish ▏   to keyhole.demo with key order.test   Enter send"),
            "{prompt}"
        );
        press(&mut app, KeyEvent::from(KeyCode::Esc));

        for (name, expect) in [
            ("orders", "to queue orders"),
            ("amq.fanout", "to amq.fanout (fanout)"),
            (
                "amq.headers",
                "to amq.headers · no headers, so only header-less bindings match",
            ),
        ] {
            select_destination(&mut app, name);
            press(&mut app, KeyEvent::from(KeyCode::Char('P')));
            let prompt = footer(&mut app);
            assert!(prompt.contains(expect), "{name}: {prompt}");
            press(&mut app, KeyEvent::from(KeyCode::Esc));
        }
    }

    #[tokio::test]
    async fn amqp_publish_prompt_names_no_target() {
        let (mut app, _rx) = app_with_amqp_connection().await;
        app.screen = Screen::Browser;
        app.handle_event(AppEvent::Input(crossterm::event::Event::Key(
            KeyEvent::from(KeyCode::Char('P')),
        )));
        let text = render_lines(&mut app, 90, 24);
        assert_eq!(
            text.lines().last().unwrap().trim_end(),
            " publish ▏   Enter send"
        );
    }

    /// Press `key`, as the terminal would deliver it.
    fn press(app: &mut App, key: KeyEvent) {
        app.handle_event(AppEvent::Input(crossterm::event::Event::Key(key)));
    }

    /// A plain-text message with no metadata.
    fn broker_event(body: &str) -> BrokerEvent {
        rabbitmq_message(Payload::Utf8(body.into()), &[])
    }

    /// A message as a RabbitMQ peek reports it.
    fn rabbitmq_message(body: Payload, meta: &[(&str, &str)]) -> BrokerEvent {
        BrokerEvent {
            ts: OffsetDateTime::UNIX_EPOCH,
            source: "payments".into(),
            payload: body,
            meta: meta
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// The RabbitMQ browser with `name` selected and a peek of it returned
    /// (through the real keys and reply path), the listing's `payments` being a
    /// 3.x quorum queue whose peeks count toward a delivery limit of 3.
    async fn rabbitmq_peeked(name: &str, result: Result<Vec<BrokerEvent>, String>) -> App {
        rabbitmq_peeked_with_limit(name, 3, result).await
    }

    /// [`rabbitmq_peeked`] with `payments`' delivery limit at `limit`.
    async fn rabbitmq_peeked_with_limit(
        name: &str,
        limit: i64,
        result: Result<Vec<BrokerEvent>, String>,
    ) -> App {
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(payments_limited_to(limit)))).await;
        select_destination(&mut app, name);
        press(&mut app, KeyEvent::from(KeyCode::Enter));
        app.handle_event(AppEvent::Peeked {
            id: ConnId(1),
            spec: SubSpec::Queue(name.into()),
            result,
        });
        app
    }

    #[tokio::test]
    async fn snapshot_browser_rabbitmq_peek() {
        // A peeked quorum queue: its messages over the selected one's body and
        // metadata, and the title warning that each peek counts toward the
        // delivery limit (RabbitMQ 3.x).
        let mut app = rabbitmq_peeked(
            "payments",
            Ok(vec![
                rabbitmq_message(
                    Payload::Json(r#"{"order":1001,"total":42}"#.into()),
                    &[
                        ("exchange", "keyhole.demo"),
                        ("routing_key", "order.created"),
                        ("redelivered", "true"),
                        ("content_type", "application/json"),
                        ("message_id", "msg-0001"),
                        ("delivery_mode", "2 (persistent)"),
                        ("headers.x-delivery-count", "1"),
                    ],
                ),
                rabbitmq_message(
                    Payload::Utf8("plain text".into()),
                    &[
                        ("exchange", "(default)"),
                        ("routing_key", "payments"),
                        ("redelivered", "false"),
                    ],
                ),
            ]),
        )
        .await;
        // Ctrl-→ steps into the messages.
        press(
            &mut app,
            KeyEvent::new(KeyCode::Right, KeyModifiers::CONTROL),
        );
        assert!(app.active_conn().unwrap().peek.focused);
        let text = render_lines(&mut app, 160, 44);
        assert!(
            text.contains("payments · 2 msgs · each peek counts toward delivery-limit 3"),
            "{text}"
        );
        assert!(text.lines().last().unwrap().contains("/ filter"), "{text}");
        insta::assert_snapshot!("browser_rabbitmq_peek", text);
    }

    #[tokio::test]
    async fn rabbitmq_queue_details_say_what_enter_does_in_each_peek_mode() {
        use crate::config::PeekMode;
        for (mode, expect) in [
            (PeekMode::Browse, "flagged redelivered"),
            (PeekMode::Destructive, "they are removed"),
            (PeekMode::Skip, "Peek mode is skip"),
        ] {
            let (mut app, _rx) =
                rabbitmq_browser_in(mode, Some(Ok(mock::sample_destinations()))).await;
            select_destination(&mut app, "orders");
            let text = render_lines(&mut app, 200, 30);
            assert!(text.contains(expect), "{mode:?}: {text}");
        }
    }

    /// The sample listing with `payments` a 3.x quorum queue whose peeks
    /// count toward a delivery limit of `limit`.
    fn payments_limited_to(limit: i64) -> DestinationList {
        let mut list = mock::sample_destinations();
        let payments = list
            .items
            .iter_mut()
            .find(|d| d.name == "payments")
            .unwrap();
        payments.peek_delivery_limit = Some(limit);
        list
    }

    #[tokio::test]
    async fn rabbitmq_quorum_warning_shows_only_when_a_peek_counts_toward_a_limit() {
        use crate::config::PeekMode;
        const WARNING: &str = "delivery-limit of 3";
        let list = payments_limited_to(3);
        // Before the peek: payments' details warn in browse mode only (a
        // destructive read doesn't requeue).
        for (mode, warns) in [(PeekMode::Browse, true), (PeekMode::Destructive, false)] {
            let (mut app, _rx) = rabbitmq_browser_in(mode, Some(Ok(list.clone()))).await;
            select_destination(&mut app, "payments");
            let text = render_lines(&mut app, 200, 30);
            assert_eq!(text.contains(WARNING), warns, "{mode:?}: {text}");
        }
        // A queue whose peeks count toward nothing (no limit, or 4.x): no
        // warning, before or after its peek.
        let mut app = rabbitmq_peeked("orders", Ok(vec![broker_event("m")])).await;
        let text = render_lines(&mut app, 200, 30);
        assert!(text.contains(" orders · 1 msg "), "{text}");
        assert!(!text.contains("delivery-limit"), "{text}");
    }

    #[tokio::test]
    async fn a_peeked_queues_short_name_stays_whole_beside_the_warning() {
        // At 90 columns the message pane is 45 wide: payments keeps its whole
        // name, and the delivery-limit warning is cut instead.
        let mut app = rabbitmq_peeked_with_limit("payments", 3, Ok(vec![broker_event("m")])).await;
        let text = render_lines(&mut app, 90, 24);
        assert!(
            text.contains("┌ payments · 1 msg · each peek counts towar…┐"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_rabbitmq_limit_below_one_warns_that_each_peek_drops_what_it_reads() {
        // On 3.x a limit of 0 or less drops or dead-letters a message on its
        // first requeue, so a browse peek is as good as destructive.
        for limit in [0, -1] {
            let (mut app, _rx) = rabbitmq_browser(Some(Ok(payments_limited_to(limit)))).await;
            select_destination(&mut app, "payments");
            let text = render_lines(&mut app, 200, 30);
            assert!(
                text.contains("Each peek drops or dead-letters the messages it reads"),
                "{text}"
            );
            assert!(
                text.contains(&format!("delivery-limit is {limit}")),
                "{text}"
            );

            let mut app =
                rabbitmq_peeked_with_limit("payments", limit, Ok(vec![broker_event("m")])).await;
            let text = render_lines(&mut app, 200, 30);
            assert!(
                text.contains("payments · 1 msg · peeking drops or dead-letters what it reads"),
                "{text}"
            );
        }
    }

    #[tokio::test]
    async fn rabbitmq_stream_queue_shows_why_it_isnt_peeked() {
        // Known from the listing's type: Enter sends nothing, the pane says why.
        let mut app = rabbitmq_peeked("orders", Ok(Vec::new())).await;
        select_destination(&mut app, "audit");
        press(&mut app, KeyEvent::from(KeyCode::Enter));
        let text = render_lines(&mut app, 200, 30);
        assert!(text.contains("Stream queues can't be peeked"), "{text}");
        assert!(text.contains("tap an exchange that routes to it"), "{text}");

        // A stream the listing didn't know about: the broker's refusal comes
        // back as the same hint.
        let mut app =
            rabbitmq_peeked("orders", Err(crate::broker::STREAM_PEEK_HINT.to_string())).await;
        let text = render_lines(&mut app, 200, 30);
        assert!(text.contains("Stream queues can't be peeked"), "{text}");
    }

    #[tokio::test]
    async fn rabbitmq_pane_states_of_a_peek() {
        let mut app = rabbitmq_peeked("orders", Ok(Vec::new())).await;
        assert!(render_lines(&mut app, 160, 30).contains("The queue was empty when peeked."));

        let mut app = rabbitmq_peeked(
            "orders",
            Err("reading queue `orders`: permission denied (HTTP 401)".into()),
        )
        .await;
        let text = render_lines(&mut app, 160, 30);
        assert!(text.contains("permission denied (HTTP 401)"), "{text}");
        assert!(text.contains("Press Enter to peek again."), "{text}");

        // In flight.
        let (mut app, _rx) = rabbitmq_browser(Some(Ok(mock::sample_destinations()))).await;
        press(&mut app, KeyEvent::from(KeyCode::Enter));
        assert!(render_lines(&mut app, 160, 30).contains("Peeking…"));
    }

    #[tokio::test]
    async fn rabbitmq_pane_shows_a_peek_only_while_its_queue_is_selected() {
        let mut app = rabbitmq_peeked("orders", Ok(vec![broker_event("from orders")])).await;
        assert!(render_lines(&mut app, 160, 30).contains("from orders"));
        // Another queue shows its own details, not orders' messages.
        select_destination(&mut app, "payments");
        let text = render_lines(&mut app, 160, 30);
        assert!(!text.contains("from orders"), "{text}");
        assert!(text.contains("quorum queue"), "{text}");
        // Back on orders, its peek is still there: nothing was read again.
        select_destination(&mut app, "orders");
        assert!(render_lines(&mut app, 160, 30).contains("from orders"));
    }

    #[tokio::test]
    async fn amqp_pane_never_shows_one_queues_peek_under_another() {
        let (mut app, _rx) = app_with_amqp_connection().await;
        app.screen = Screen::Browser;
        app.connections[0]
            .add_destination(crate::app::Destination::new("billing", DestKind::Queue));
        let text = render_lines(&mut app, 90, 24);
        assert!(text.contains("billing"), "{text}");
        assert!(text.contains("Press Enter to peek its messages."), "{text}");
        assert!(!text.contains("order #1"), "{text}");
    }

    #[tokio::test]
    async fn amqp_message_pane_border_follows_border_focused_not_accent() {
        // F1: the focused message-list border uses the shared `border_focused`
        // tint like every other Ctrl-arrow pane — not `accent`. The two coincide
        // in the built-in palettes, so drive them apart and check the border.
        let (mut app, _rx) = app_with_amqp_connection().await;
        app.screen = Screen::Browser;
        app.theme.border_focused = Style::new().fg(Color::Magenta);
        app.theme.accent = Style::new().fg(Color::Cyan);
        // Ctrl-→ focuses the message list, so its border should light up.
        app.connections[0].peek.focused = true;

        let mut terminal = Terminal::new(TestBackend::new(90, 24)).unwrap();
        let frame = terminal.draw(|f| render(f, &mut app)).expect("render");
        let buf = frame.buffer;
        let w = buf.area.width as usize;
        // The message list is the upper-right pane; its top-left corner is the
        // rightmost `┌` on row 0 (the left one is the unfocused Destinations box).
        let corner = (0..w)
            .filter(|&c| buf.content()[c].symbol() == "┌")
            .max()
            .expect("the message pane has a top-left corner");
        assert_eq!(
            buf.content()[corner].style().fg,
            Some(Color::Magenta),
            "the focused message border uses border_focused, not accent"
        );
    }

    #[tokio::test]
    async fn amqp_message_cursor_follows_focus() {
        // F2/F4: the selected message's `▶` cursor carries the row selection
        // style while focused (not `accent`), and parks to a dim cursor when the
        // keyboard is elsewhere — the keys/destinations/recordings convention.
        let (mut app, _rx) = app_with_amqp_connection().await;
        app.screen = Screen::Browser;
        app.theme.selected = Style::new().fg(Color::Green);
        app.theme.dim = Style::new().fg(Color::DarkGray);
        app.theme.accent = Style::new().fg(Color::Cyan);
        app.connections[0].peek.selected = 0;

        // The `▶` glyph in the right half is the message cursor (the left-half
        // one is the destination cursor).
        let cursor_fg = |app: &mut App| {
            let mut terminal = Terminal::new(TestBackend::new(90, 24)).unwrap();
            let frame = terminal.draw(|f| render(f, app)).expect("render");
            let buf = frame.buffer;
            let w = buf.area.width as usize;
            (0..buf.content().len())
                .find(|&i| buf.content()[i].symbol() == "▶" && i % w >= w / 2)
                .map(|i| buf.content()[i].style().fg)
                .expect("the message list shows a cursor")
        };

        app.connections[0].peek.focused = true;
        assert_eq!(
            cursor_fg(&mut app),
            Some(Color::Green),
            "the focused message cursor uses the selection style, not accent"
        );

        app.connections[0].peek.focused = false;
        assert_eq!(
            cursor_fg(&mut app),
            Some(Color::DarkGray),
            "the unfocused message list keeps a dim parked cursor"
        );
    }

    #[tokio::test]
    async fn key_list_selection_dims_when_bottom_panel_focused() {
        // F4: the key list parks its selection to `dim` when the bottom panel
        // holds the keyboard (and brightens when it has it back), so the focused
        // pane is unambiguous — the shared list convention via `selection_style`.
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.theme.selected = Style::new().fg(Color::Green);
        app.theme.dim = Style::new().fg(Color::DarkGray);
        app.connections[0].browser.keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].rebuild_view();
        app.connections[0].browser.phase = ScanPhase::Complete;
        app.connections[0].browser.table.select(Some(0));

        // The key cursor `▶` is in the left half (the value pane is the right).
        let cursor_fg = |app: &mut App| {
            let mut terminal = Terminal::new(TestBackend::new(90, 24)).unwrap();
            let frame = terminal.draw(|f| render(f, app)).expect("render");
            let buf = frame.buffer;
            let w = buf.area.width as usize;
            (0..buf.content().len())
                .find(|&i| buf.content()[i].symbol() == "▶" && i % w < w / 2)
                .map(|i| buf.content()[i].style().fg)
                .expect("the key list shows a cursor")
        };

        app.connections[0].focus = PaneFocus::Keys;
        assert_eq!(
            cursor_fg(&mut app),
            Some(Color::Green),
            "the focused key list shows the bright selection"
        );

        app.connections[0].focus = PaneFocus::Bottom;
        assert_eq!(
            cursor_fg(&mut app),
            Some(Color::DarkGray),
            "the key list parks to a dim cursor when the bottom panel is focused"
        );
    }

    #[tokio::test]
    async fn amqp_body_footer_advertises_shared_globals() {
        // F3: the AMQP body (destinations ⇄ messages) advertises the same global
        // cluster as the Redis keys pane and the Recordings tab — the tails route
        // (Tab), `: palette` and `? help` — on both panes. Basic navigation (the
        // Ctrl-↑↓ focus moves and Esc back) is omitted, as everywhere else.
        let (mut app, _rx) = app_with_amqp_connection().await;
        app.screen = Screen::Browser;
        for on_messages in [false, true] {
            app.connections[0].peek.focused = on_messages;
            let pairs = hint_sections(&app);
            let has = |k: &str, a: &str| pairs.iter().any(|(key, act)| *key == k && act == a);
            let pane = if on_messages {
                "messages"
            } else {
                "destinations"
            };
            assert!(
                has(":", "palette"),
                "AMQP {pane} footer advertises the palette: {pairs:?}"
            );
            assert!(
                has("?", "help"),
                "AMQP {pane} footer offers help: {pairs:?}"
            );
            assert!(
                has("Tab", "tails"),
                "AMQP {pane} footer routes to the tails with Tab: {pairs:?}"
            );
            assert!(
                !pairs
                    .iter()
                    .any(|(k, _)| k.contains('↑') || k.contains("Ctrl-") || *k == "Esc"),
                "AMQP {pane} footer drops basic nav: {pairs:?}"
            );
        }
    }

    #[test]
    fn recordings_list_box_border_follows_focus() {
        // F5: the recordings list is now a box whose border brightens
        // (border_focused) while it holds the keyboard and dims (border) when
        // the viewer does — the same focus cue as the Browser panes, via
        // `border_style`. (Text snapshots show the box glyphs but not the
        // focus colour, so assert the colour here.)
        use crate::recording::{RecordingView, ViewRecord};
        let (mut app, _rx) = test_app();
        app.screen = Screen::Recordings;
        app.theme.border_focused = Style::new().fg(Color::Magenta);
        app.theme.border = Style::new().fg(Color::DarkGray);
        app.recordings = vec![crate::app::RecordingFile {
            name: "rec.jsonl".into(),
            size: 16,
            modified: None,
        }];
        app.recordings_state.select(Some(0));
        app.recording_view = Some((
            "rec.jsonl".into(),
            RecordingView {
                connection: None,
                source_type: None,
                records: vec![ViewRecord {
                    seq: 0,
                    time: "0".into(),
                    source: "s".into(),
                    payload: "p".into(),
                }],
                error: None,
            },
        ));

        // The list box's top-left `┌` is the first corner on the row below the
        // home frame's top border (the viewer box's corner sits further right).
        let list_corner_fg = |app: &mut App| {
            let mut terminal = Terminal::new(TestBackend::new(100, 14)).unwrap();
            let frame = terminal.draw(|f| render(f, app)).expect("render");
            let buf = frame.buffer;
            let w = buf.area.width as usize;
            (w..2 * w)
                .find(|&i| buf.content()[i].symbol() == "┌")
                .map(|i| buf.content()[i].style().fg)
                .expect("the recordings list is boxed")
        };

        app.recordings_focus = RecordingsFocus::List;
        assert_eq!(
            list_corner_fg(&mut app),
            Some(Color::Magenta),
            "the focused list border uses border_focused"
        );

        app.recordings_focus = RecordingsFocus::Viewer;
        assert_eq!(
            list_corner_fg(&mut app),
            Some(Color::DarkGray),
            "the list border dims to border when the viewer is focused"
        );
    }

    #[tokio::test]
    async fn keys_value_pane_is_one_box_with_a_shared_focus_tint() {
        // The value inspector is the right-hand section of the Keys box, not a
        // separate (Ctrl-→-navigable) pane: a single bordered region split by one
        // divider, whose border AND divider take the keys-pane focus tint together
        // — so the whole unit brightens/dims as one part.
        let (mut app, _rx) = app_with_connection().await;
        app.screen = Screen::Browser;
        app.theme.border_focused = Style::new().fg(Color::Magenta);
        app.theme.border = Style::new().fg(Color::DarkGray);
        app.connections[0].browser.keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].rebuild_view();
        app.connections[0].browser.phase = ScanPhase::Complete;
        // Rows: [user hdr, user:1] — select the key so the value section's title is
        // the key name, riding the divider's top border.
        app.connections[0].browser.table.select(Some(1));
        app.connections[0].inspector.value_key = Some("user:1".into());
        app.connections[0].inspector.value = Some(ValueView::Str {
            total_bytes: 5,
            shown_bytes: 5,
            text: "alice".into(),
            encoding: PayloadEncoding::Utf8,
        });

        // The Keys box top-border row: its `┌` corner and the `┬` divider junction.
        let border_tint = |app: &mut App| -> (Option<Color>, Option<Color>) {
            let mut terminal = Terminal::new(TestBackend::new(90, 20)).unwrap();
            let frame = terminal.draw(|f| render(f, app)).expect("render");
            let buf = frame.buffer;
            let w = buf.area.width as usize;
            let syms: Vec<String> = buf
                .content()
                .iter()
                .map(|c| c.symbol().to_string())
                .collect();
            let keys_row = (0..buf.area.height as usize)
                .find(|&r| syms[r * w..r * w + 7].concat() == "┌ Keys ")
                .expect("the Keys box is drawn");
            let row = &buf.content()[keys_row * w..(keys_row + 1) * w];
            let fg = |glyph: &str| {
                row.iter()
                    .find(|c| c.symbol() == glyph)
                    .and_then(|c| c.style().fg)
            };
            (fg("┌"), fg("┬"))
        };

        // Structure: exactly one box (no `┐┌` double border between two panes),
        // and the value section's title rides the divider's top border.
        let text = render_lines(&mut app, 90, 20);
        assert!(
            !text.lines().any(|l| l.contains("┐┌")),
            "the value is not a separate box: {text}"
        );
        assert!(
            text.lines().any(|l| l.contains("┬ user:1")),
            "the value's title rides the shared divider: {text}"
        );

        // Keys focused: the corner and the divider both light up (border_focused).
        app.connections[0].focus = PaneFocus::Keys;
        assert_eq!(
            border_tint(&mut app),
            (Some(Color::Magenta), Some(Color::Magenta)),
            "border and divider brighten together when the keys pane is focused"
        );

        // Bottom panel focused: both dim back to `border`, still in lockstep.
        app.connections[0].focus = PaneFocus::Bottom;
        assert_eq!(
            border_tint(&mut app),
            (Some(Color::DarkGray), Some(Color::DarkGray)),
            "border and divider dim together when the bottom panel is focused"
        );
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
        app.connections[0].browser.phase = ScanPhase::Complete;
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
        // Keep the bottom panel on the Console (this snapshot covers the Server
        // band up top, not the new leftmost Server Details tab).
        app.connections[0].panel_tab = 1;
        app.connections[0].dashboard.stats = Some(BrokerStats::Redis(ServerStats {
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
        }));
        insta::assert_snapshot!("browser_with_stats", render_lines(&mut app, 90, 20));
    }

    #[tokio::test]
    async fn snapshot_browser_panel_keyspace_notice() {
        // The keyspace feed focused under its fixed anchor (slot 3), showing the
        // tab strip (the six anchors) and the advisory notice banner.
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].browser.keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].rebuild_view();
        app.connections[0].browser.phase = ScanPhase::Complete;
        app.connections[0].browser.table.select(Some(0));
        let mut sub = Subscription::new(1, SubSpec::Keyspace { db: 0 }, 100);
        sub.state = SubState::Active;
        sub.notice =
            Some("keyspace notifications are disabled (notify-keyspace-events is empty)".into());
        app.connections[0].subs.push(sub);
        app.connections[0].panel_tab = 3;
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
        app.connections[0].browser.phase = ScanPhase::Complete;
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
        app.connections[0].panel_tab = 5; // the pub/sub tail tab
        insta::assert_snapshot!(
            "browser_panel_tail_recording",
            render_lines(&mut app, 90, 26)
        );
    }

    #[tokio::test]
    async fn snapshot_browser_server_details() {
        // The leftmost bottom-panel tab: a server-facts strip, the ops/keys
        // time-series graphs, and the connected-client list. Pinned data keeps
        // the sparklines and rows deterministic.
        let (mut app, _rx) = app_with_connection().await;
        pin_clock(&mut app);
        app.screen = Screen::Browser;
        app.connections[0].browser.keys = vec![entry("user:1", ValueType::String)];
        app.connections[0].rebuild_view();
        app.connections[0].browser.phase = ScanPhase::Complete;
        app.connections[0].browser.table.select(Some(0));

        let client = |id, name: &str, addr: &str, idle, cmd: &str| ClientInfo {
            id,
            name: name.into(),
            addr: addr.into(),
            age: 0,
            idle,
            last_cmd: cmd.into(),
        };
        let mut stats = ServerStats {
            redis_version: Some("7.4.0".into()),
            uptime_seconds: Some(3661),
            connected_clients: Some(7),
            instantaneous_ops_per_sec: Some(31),
            instantaneous_input_kbps: Some(12.0),
            instantaneous_output_kbps: Some(4.0),
            used_memory: Some(1024 * 1024),
            maxmemory: Some(4 * 1024 * 1024),
            keyspace_hits: Some(900),
            keyspace_misses: Some(100),
            db_keys: vec![(0, 412)],
            clients: vec![
                client(3, "web-1", "10.0.0.2:5051", 0, "get"),
                client(4, "", "10.0.0.9:6210", 2, "subscribe"),
                client(5, "cron", "10.0.0.3:4410", 3600, "client|list"),
                // A freshly opened connection that has run nothing yet: Redis
                // reports `cmd=NULL`, which renders as an em-dash.
                client(6, "", "10.0.0.7:7001", 45, "NULL"),
            ],
            ..Default::default()
        };
        for (k, v) in [
            ("redis_mode", "standalone"),
            ("role", "master"),
            ("total_commands_processed", "120345"),
            ("expired_keys", "42"),
            ("evicted_keys", "0"),
            ("maxclients", "10000"),
            ("connected_slaves", "2"),
            ("mem_fragmentation_ratio", "1.42"),
            ("maxmemory_policy", "allkeys-lru"),
            ("rdb_last_bgsave_status", "ok"),
            ("aof_enabled", "0"),
            ("rdb_changes_since_last_save", "1280"),
            ("blocked_clients", "1"),
            ("rejected_connections", "3"),
        ] {
            stats.raw.insert(k.into(), v.into());
        }
        // A short ops/network history so the sparklines have a visible shape
        // (network samples are bytes/sec).
        for ops in [2u64, 5, 9, 14, 22, 18, 27, 31] {
            app.connections[0].dashboard.primary_history.push_back(ops);
        }
        for net in [1024u64, 2048, 4096, 8192, 6000, 12000, 9000, 16384] {
            app.connections[0]
                .dashboard
                .secondary_history
                .push_back(net);
        }
        app.connections[0].dashboard.stats = Some(BrokerStats::Redis(stats));
        // Server Details is the leftmost tab (index 0), selected by default. A
        // taller frame gives the panel room for the metrics/facts/health lines,
        // the two graphs, and the client table (title + column header + rows).
        insta::assert_snapshot!("browser_server_details", render_lines(&mut app, 90, 36));
    }
}
