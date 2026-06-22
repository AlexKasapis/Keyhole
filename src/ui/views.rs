//! Per-screen rendering. Each function draws one screen (or overlay) from the
//! current [`App`] state into the given area.

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Cell, Clear, HighlightSpacing, List, ListItem, Padding, Paragraph, Row, Table,
    TableState, Wrap,
};
use ratatui::Frame;
use time::OffsetDateTime;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::app::{
    App, ConnForm, ConnHealth, Connection, InputMode, PanelTab, RecordState, SubState,
    Subscription, ViewRow,
};
use crate::broker::{BrokerEvent, BrokerKind, Payload, Ttl, ValueView};
use crate::theme::Theme;

/// Connections screen: the list of saved profiles.
pub fn connections(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    let block = Block::bordered()
        .title(" Connections ")
        .title_style(theme.heading)
        .border_style(theme.border);

    if app.profiles.is_empty() {
        let body = Paragraph::new(vec![
            Line::from(""),
            Line::styled("No saved connections.", theme.dim),
            Line::from(""),
            Line::from("Press 'a' to add one."),
        ])
        .alignment(Alignment::Center)
        .block(block);
        frame.render_widget(body, area);
        return;
    }

    // One row per saved profile, laid out in columns. The status dot is green
    // when the profile has an open connection (the same green as the status
    // bar's "Connected to …"), and a dim hollow circle otherwise. The INFO
    // column stays terse: a live connection surfaces a little broker state,
    // everything else just reads "offline".
    let mut rows: Vec<Row> = Vec::with_capacity(app.profiles.len());
    for p in &app.profiles {
        let conn = app.connections.iter().find(|c| c.name == p.name());
        let (dot, dot_style) = if app.is_connected(p.name()) {
            ("●", theme.success)
        } else {
            ("○", theme.dim)
        };
        let endpoint = match p.username() {
            Some(user) => format!("{user}@{}", p.endpoint()),
            None => p.endpoint(),
        };
        let (info, info_style) = match conn {
            Some(c) => (connection_info(c), Style::default()),
            None => ("offline".to_string(), theme.dim),
        };
        rows.push(Row::new(vec![
            Cell::from(Span::styled(dot, dot_style)),
            Cell::from(Span::raw(p.name().to_string())),
            Cell::from(Span::styled(p.kind_label(), theme.dim)),
            Cell::from(Span::styled(endpoint, theme.dim)),
            Cell::from(Span::styled(info, info_style)),
        ]));
    }

    // Fixed widths for the leading columns so the flexible INFO column absorbs
    // the remaining width (and shows the full live summary on wide terminals);
    // names/endpoints longer than their column are truncated by the table.
    let widths = [
        Constraint::Length(1),  // status dot
        Constraint::Length(18), // name
        Constraint::Length(8),  // kind
        Constraint::Length(26), // endpoint
        Constraint::Min(16),    // info
    ];
    let table = Table::new(rows, widths)
        .header(Row::new(["", "NAME", "KIND", "ENDPOINT", "INFO"]).style(theme.header))
        .column_spacing(2)
        .block(block)
        .row_highlight_style(theme.selected)
        .highlight_symbol("▶ ")
        .highlight_spacing(HighlightSpacing::Always);
    frame.render_stateful_widget(table, area, &mut app.profile_state);
}

/// A terse one-line summary of a live connection for the Connections list's
/// INFO column. Redis exposes server statistics (version, key count, clients,
/// throughput, memory); the AMQP brokers have no such endpoint, so they report
/// liveness plus how many tails are open. Kept short so the row stays readable.
fn connection_info(conn: &Connection) -> String {
    match conn.caps.kind {
        BrokerKind::Redis => {
            let mut parts: Vec<String> = Vec::new();
            if let Some(stats) = &conn.dashboard.stats {
                if let Some(version) = &stats.redis_version {
                    parts.push(format!("v{version}"));
                }
                let keys: u64 = stats.db_keys.iter().map(|(_, n)| n).sum();
                parts.push(format!("{keys} keys"));
                if let Some(clients) = stats.connected_clients {
                    parts.push(format!("{clients} clients"));
                }
                if let Some(ops) = stats.instantaneous_ops_per_sec {
                    parts.push(format!("{ops} ops/s"));
                }
                if let Some(mem) = stats.used_memory {
                    parts.push(human_bytes(mem));
                }
            }
            // Connected but stats not yet fetched (or unavailable).
            if parts.is_empty() {
                "live".to_string()
            } else {
                parts.join(" · ")
            }
        }
        BrokerKind::Amqp | BrokerKind::Rabbitmq => match conn.subs.len() {
            0 => "live".to_string(),
            1 => "live · 1 tail".to_string(),
            n => format!("live · {n} tails"),
        },
    }
}

/// The scroll offset for a viewport that shows `viewport` rows out of `total`,
/// given the previous offset and the selected row. The window stays put unless
/// the selection would fall outside it (then it scrolls the minimum needed to
/// bring the selection back into view), and is always clamped so the last row
/// can't sit above an empty viewport — mirroring ratatui's own follow logic,
/// which we replicate here because the table is fed a pre-sliced window.
fn visible_offset(prev: usize, selected: Option<usize>, viewport: usize, total: usize) -> usize {
    let max_offset = total.saturating_sub(viewport);
    let mut off = prev.min(max_offset);
    if let Some(sel) = selected {
        if sel < off {
            off = sel;
        } else if viewport > 0 && sel >= off + viewport {
            off = sel + 1 - viewport;
        }
    }
    off.min(max_offset)
}

/// Browser screen: key table + value pane for the active connection.
pub fn browser(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    // The panel's input tabs show a typing cursor and echo the typed text;
    // capture the mode and the shared subscribe buffer before the `&mut` borrow
    // of the active connection below.
    let mode = app.mode;
    let subscribe_buf = app.subscribe_buf.clone();
    // The connection-health indicator now lives in the Server band, and the
    // bottom panel's height tracks the window; both are read before the `&mut`
    // borrow of the active connection below.
    let health = app.conn_health();
    let panel_h = panel_band_height(frame.area().height);
    let Some(conn) = app.active_conn_mut() else {
        let body = Paragraph::new("No active connection. Press 'c', select a profile, and Enter.")
            .style(theme.dim)
            .block(Block::bordered().border_style(theme.border));
        frame.render_widget(body, area);
        return;
    };

    // The view (sorted/grouped rows) is a cache derived from `keys`, kept current
    // by the update phase: every SCAN page rebuilds it (see `App::on_keys_page`)
    // and a fresh scan rebuilds it empty. Render therefore never rebuilds — it
    // only reads — so drawing carries no hidden re-sort cost. Invariant: a
    // non-empty key set always has a built view.
    debug_assert!(
        conn.browser.keys.is_empty() || !conn.browser.view.is_empty(),
        "view must be rebuilt in the update phase before render"
    );

    // The Browser stacks, top to bottom: an optional server-stats band (Redis —
    // the former standalone Dashboard, now also carrying the connection-health
    // dot); the keys + value panes; and an optional, always-visible tabbed
    // bottom panel pinned to the bottom (Redis — the read-only console and one
    // tab per live tail). The former one-line info bar is gone: its key count
    // moved up to the header and its match/sort indicators down to the footer.
    let mut rows = Vec::new();
    if conn.caps.can_dashboard {
        rows.push(Constraint::Length(SERVER_BAND_HEIGHT));
    }
    rows.push(Constraint::Min(0));
    if conn.caps.can_console {
        rows.push(Constraint::Length(panel_h));
    }
    let chunks = Layout::vertical(rows).split(area);
    let band_area = conn.caps.can_dashboard.then(|| chunks[0]);
    // Body sits after the optional stats band.
    let body_idx = if conn.caps.can_dashboard { 1 } else { 0 };
    let body_area = chunks[body_idx];
    let panel_area = conn.caps.can_console.then(|| chunks[body_idx + 1]);

    if let Some(band_area) = band_area {
        server_stats_band(frame, conn, health, theme, band_area);
    }

    let [table_area, value_area] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(body_area);

    // The key list is a pure viewport: only the rows that fall inside the
    // visible window are turned into `Row`s each frame. With a large expanded
    // keyspace (tens of thousands of keys) building the entire list — and a
    // `format!`ed cell per column — on every frame is what makes the browser
    // crawl, even though ratatui only ever paints the handful of visible rows.
    // We therefore compute the scroll window ourselves and hand the widget just
    // that slice. The viewport excludes the two border rows and the one header
    // row.
    let total = conn.browser.view.len();
    let viewport = table_area.height.saturating_sub(3) as usize;
    let selected = conn.browser.table.selected();
    // Track the scroll offset on `conn.browser.table` so it persists across frames
    // (the window only shifts when the selection would leave it, matching
    // ratatui's own follow behaviour) and so a shrunken view can't strand rows.
    // This — and the value-pane scroll clamp below — are the *only* state writes
    // the render path performs: both derive from the rendered area's height,
    // which the update phase (which has no layout) cannot compute. Everything
    // else render touches is read-only.
    let offset = visible_offset(conn.browser.table.offset(), selected, viewport, total);
    *conn.browser.table.offset_mut() = offset;
    let end = offset.saturating_add(viewport).min(total);

    // One rendered row per *visible* view entry: a styled, key-count-bearing
    // header for a group (with a fold marker), or a key with its type / TTL /
    // size. Groups nest, so each row is indented two spaces per nesting level;
    // a key shows the segment relative to its parent group (the full key rides
    // in the value-pane title).
    let rows: Vec<Row> = conn.browser.view[offset..end]
        .iter()
        .filter_map(|vr| match vr {
            ViewRow::Group { path, depth, count } => {
                let marker = if conn.browser.collapsed.contains(path) {
                    "▸"
                } else {
                    "▾"
                };
                let name = group_label(path);
                Some(
                    Row::new(vec![
                        Cell::from(format!("{}{marker} {name} ({count})", indent(*depth))),
                        Cell::from(""),
                        Cell::from(""),
                        Cell::from(""),
                    ])
                    .style(theme.heading),
                )
            }
            // `get` rather than indexing: a stale view (keys mutated without a
            // rebuild) must skip the row, never panic.
            ViewRow::Entry { idx, depth } => conn.browser.keys.get(*idx).map(|e| {
                let ttl = match e.ttl {
                    Ttl::NoExpire => "—".to_string(),
                    Ttl::Seconds(s) => human_duration(s.max(0) as u64),
                    Ttl::Unknown => "?".to_string(),
                };
                let size = match e.size {
                    Some(n) => human_bytes(n),
                    None => "—".to_string(),
                };
                Row::new(vec![
                    Cell::from(format!("{}{}", indent(*depth), entry_label(&e.key))),
                    Cell::from(e.vtype.label().to_string()),
                    Cell::from(ttl),
                    Cell::from(size),
                ])
            }),
        })
        .collect();
    let widths = [
        Constraint::Min(10),
        Constraint::Length(6),
        Constraint::Length(8),
        Constraint::Length(7),
    ];
    let table = Table::new(rows, widths)
        .header(Row::new(["Key", "Type", "TTL", "Size"]).style(theme.header))
        .column_spacing(2)
        .block(
            Block::bordered()
                .title(" Keys ")
                .title_style(theme.heading)
                .border_style(theme.border),
        )
        .row_highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    // The rows are already sliced to the window, so the widget gets a
    // viewport-local state: the selection rebased onto the slice and a zero
    // offset. (The canonical selection/offset stay on `conn.browser.table` above.)
    let mut win = TableState::default();
    win.select(selected.map(|s| s.saturating_sub(offset)));
    frame.render_stateful_widget(table, table_area, &mut win);

    // The value pane mirrors the *current* selection. When the cursor sits on a
    // group header (not a key), show a neutral prompt rather than "loading…" or
    // the last-inspected key's stale value.
    let on_key = conn.selected().is_some();
    let title = match (on_key, &conn.inspector.value_key) {
        (true, Some(k)) => format!(" {k} "),
        _ => " Value ".to_string(),
    };
    let value_lines = if on_key {
        render_value(theme, conn.inspector.value.as_ref())
    } else {
        vec![Line::styled(
            "Select a key to inspect its value.",
            theme.dim,
        )]
    };
    // Clamp the scroll offset so paging can't run off the end of the value (the
    // second of the two deliberate viewport-derived render writes noted above).
    // The bound uses logical line count (wrapping may split lines further, as the
    // console's scroll does too); inner height excludes the two border rows.
    let inner_h = value_area.height.saturating_sub(2) as usize;
    let max_scroll = value_lines.len().saturating_sub(inner_h) as u16;
    conn.inspector.clamp_scroll(max_scroll);
    let value = Paragraph::new(value_lines)
        .block(
            Block::bordered()
                .title(title)
                .title_style(theme.heading)
                .border_style(theme.border),
        )
        .wrap(Wrap { trim: false })
        .scroll((conn.inspector.value_scroll, 0));
    frame.render_widget(value, value_area);

    if let Some(panel_area) = panel_area {
        panel_band(frame, conn, mode, &subscribe_buf, theme, panel_area);
    }
}

/// Total height (rows) reserved for the Browser's server-stats band: a border
/// (2) wrapping a gauges row (1) and a one-line metrics summary (1).
const SERVER_BAND_HEIGHT: u16 = 4;

/// The Browser's tabbed bottom panel sizes to a third of the terminal height,
/// clamped to `[PANEL_BAND_MIN, PANEL_BAND_MAX]`. The maximum is 1.5× the former
/// fixed height, so a tall terminal gives a tail a generous event window while a
/// short one still leaves the keys/value panes usable.
const PANEL_BAND_MIN: u16 = 6;
const PANEL_BAND_MAX: u16 = 18;

/// The bottom panel's height for a terminal `window_height` rows tall.
fn panel_band_height(window_height: u16) -> u16 {
    (window_height / 3).clamp(PANEL_BAND_MIN, PANEL_BAND_MAX)
}

/// Two spaces of indentation per nesting level, for the key browser's tree.
fn indent(depth: usize) -> String {
    "  ".repeat(depth)
}

/// A group header's display label: its last path segment, or "(no prefix)" for
/// the root bucket of separator-less keys.
fn group_label(path: &str) -> &str {
    if path.is_empty() {
        "(no prefix)"
    } else {
        path.rsplit_once(':').map_or(path, |(_, tail)| tail)
    }
}

/// A key entry's display label under its group: the segment after the last
/// separator (the full key would just repeat the group path indented above it).
fn entry_label(key: &str) -> &str {
    key.rsplit_once(':').map_or(key, |(_, tail)| tail)
}

/// The server-stats band shown atop the Browser for brokers that expose server
/// statistics (Redis) — the former standalone Dashboard, merged into the main
/// panel. A compact, full-width strip: a Memory and a Hit-ratio gauge over a
/// one-line metrics summary (version, uptime, clients, ops/sec, keys-per-DB).
fn server_stats_band(
    frame: &mut Frame,
    conn: &Connection,
    health: ConnHealth,
    theme: &Theme,
    area: Rect,
) {
    // A one-column inner margin keeps the content off the border on both sides,
    // so the band reads as a panel rather than text crammed against a frame.
    // The connection-health indicator (the former top-right header dot) rides in
    // the title here, beside the "Server" label.
    let (dot, hlabel, dot_style) = crate::ui::health_indicator(health, theme);
    let title = Line::from(vec![
        Span::styled(" Server ", theme.heading),
        Span::styled(format!(" {dot} "), dot_style),
        Span::styled(hlabel, theme.dim),
        Span::raw(" "),
    ]);
    let block = Block::bordered()
        .title(title)
        .border_style(theme.border)
        .padding(Padding::horizontal(1));

    // Stats arrive asynchronously after connect; until the first reply, hold the
    // band's height with a placeholder so the panes below don't jump.
    let Some(stats) = conn.dashboard.stats.as_ref() else {
        frame.render_widget(
            Paragraph::new(Line::styled("Loading server stats…", theme.dim)).block(block),
            area,
        );
        return;
    };

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [gauges, metrics] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(inner);
    // Split the gauges row in half, but carve a two-column gutter out of the
    // left half so the Memory meter's value doesn't butt against the Hit meter.
    let [g_mem_full, g_hit] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(gauges);
    let g_mem = Rect {
        width: g_mem_full.width.saturating_sub(2),
        ..g_mem_full
    };

    // Memory: used / maxmemory when a cap is set, else used / peak, else just used.
    let used = stats.used_memory.unwrap_or(0);
    let (mem_ratio, mem_label) = if let Some(max) = stats.maxmemory.filter(|m| *m > 0) {
        (
            used as f64 / max as f64,
            format!("{} / {}", human_bytes(used), human_bytes(max)),
        )
    } else if let Some(peak) = stats.used_memory_peak.filter(|p| *p > 0) {
        (
            used as f64 / peak as f64,
            format!("{} · peak {}", human_bytes(used), human_bytes(peak)),
        )
    } else {
        (0.0, human_bytes(used))
    };
    meter(frame, g_mem, theme, "Memory", mem_ratio, &mem_label);

    let hit = stats.hit_ratio().unwrap_or(0.0);
    meter(
        frame,
        g_hit,
        theme,
        "Hit",
        hit,
        &format!("{:.1}%", hit * 100.0),
    );

    // Metrics line, balanced like the info bar: server health flush left, the
    // keyspace totals pinned right. Values inherit the foreground while their
    // units and separators stay dim, so the figures lead.
    let sep = |spans: &mut Vec<Span<'static>>| {
        if !spans.is_empty() {
            spans.push(Span::styled(" · ", theme.dim));
        }
    };
    let mut left: Vec<Span<'static>> = Vec::new();
    if let Some(v) = &stats.redis_version {
        left.push(Span::raw(format!("v{v}")));
    }
    if let Some(up) = stats.uptime_seconds {
        sep(&mut left);
        left.push(Span::styled("up ", theme.dim));
        left.push(Span::raw(human_duration(up)));
    }
    if let Some(c) = stats.connected_clients {
        sep(&mut left);
        left.push(Span::raw(c.to_string()));
        left.push(Span::styled(" clients", theme.dim));
    }
    if let Some(ops) = stats.instantaneous_ops_per_sec {
        sep(&mut left);
        left.push(Span::raw(ops.to_string()));
        left.push(Span::styled(" ops/s", theme.dim));
    }

    // Right group: total keys, then the per-DB breakdown (`db0 42 · db1 7`).
    let mut right: Vec<Span<'static>> = Vec::new();
    if !stats.db_keys.is_empty() {
        let total: u64 = stats.db_keys.iter().map(|(_, n)| n).sum();
        right.push(Span::raw(total.to_string()));
        right.push(Span::styled(" keys", theme.dim));
        for (db, n) in &stats.db_keys {
            right.push(Span::styled(" · ", theme.dim));
            right.push(Span::styled(format!("db{db} "), theme.dim));
            right.push(Span::raw(n.to_string()));
        }
    }
    let right = Line::from(right);
    let [m_left, m_right] = Layout::horizontal([
        Constraint::Min(0),
        Constraint::Length(line_width(&right) as u16),
    ])
    .areas(metrics);
    frame.render_widget(Paragraph::new(Line::from(left)), m_left);
    frame.render_widget(Paragraph::new(right).alignment(Alignment::Right), m_right);
}

/// A one-line meter, sized to `width` columns: a dim `label`, a bracketed bar
/// (the filled portion in the gauge style, the remainder a faint track), then
/// the `value` flush to the right edge. Used by the Browser's server-stats
/// band, where reading the value beside the bar is far clearer than a
/// [`ratatui::widgets::Gauge`]'s percentage centred over a partial fill.
fn meter_line(theme: &Theme, label: &str, ratio: f64, value: &str, width: usize) -> Line<'static> {
    // Reserve the chrome (label + a space, the two bracket caps, a space, and
    // the value), then give the rest to the bar. Labels and values here are
    // ASCII/short, so byte length tracks display width closely enough.
    let value_w = UnicodeWidthStr::width(value);
    let chrome = label.len() + 1 + 2 + 1 + value_w;
    let bar = width.saturating_sub(chrome);
    let filled = (gauge_ratio(ratio) * bar as f64).round() as usize;

    let mut spans = vec![
        Span::styled(format!("{label} "), theme.dim),
        Span::styled("▕", theme.dim),
    ];
    if filled > 0 {
        spans.push(Span::styled("█".repeat(filled), theme.gauge));
    }
    if bar > filled {
        spans.push(Span::styled("░".repeat(bar - filled), theme.dim));
    }
    spans.push(Span::styled("▏", theme.dim));
    spans.push(Span::raw(" "));
    spans.push(Span::styled(value.to_string(), theme.accent));
    Line::from(spans)
}

/// Render a [`meter_line`] into `area`.
fn meter(frame: &mut Frame, area: Rect, theme: &Theme, label: &str, ratio: f64, value: &str) {
    let line = meter_line(theme, label, ratio, value, area.width as usize);
    frame.render_widget(Paragraph::new(line), area);
}

/// The Browser's tabbed bottom panel: one bordered box whose *title line is the
/// tab strip itself*, so there is a single frame rather than a title plus a
/// separate tab row. The five fixed anchors (Console, Monitor, Keyspace,
/// Pub/Sub, Tail) are always present and joined by one tab per live pub/sub or
/// stream tail; the active tab is highlighted. Tabs are reached only by cycling
/// with Tab / Shift-Tab. The Console and the Pub/Sub / Tail anchors carry
/// always-shown input prompts; Monitor/Keyspace and the per-tail tabs show a
/// live feed. Only drawn for brokers with a panel (Redis).
fn panel_band(
    frame: &mut Frame,
    conn: &Connection,
    mode: InputMode,
    subscribe_buf: &str,
    theme: &Theme,
    area: Rect,
) {
    let slots = conn.panel_slots();
    let selected = conn.panel_tab.min(slots.len().saturating_sub(1));

    // The tab strip is the panel's title (it rides on the top border), so the
    // box has a single frame. The active tab is highlighted; live feeds carry
    // their recording / paused / lifecycle marks. Short labels only — no
    // "read-only", connection name, or db, all of which read elsewhere now.
    let mut title_spans: Vec<Span> = vec![Span::raw(" ")];
    for (i, slot) in slots.iter().enumerate() {
        if i > 0 {
            title_spans.push(Span::styled(" │ ", theme.dim));
        }
        title_spans.extend(tab_spans(slot, conn, i == selected, theme));
    }
    title_spans.push(Span::raw(" "));

    let block = Block::bordered()
        .title(Line::from(title_spans))
        .border_style(theme.border);
    let content_area = block.inner(area);
    frame.render_widget(block, area);

    // Content follows the active tab.
    let active = conn.active_panel();
    match active {
        PanelTab::Console => console_content(frame, conn, mode, theme, content_area),
        PanelTab::PubSub => anchor_input(
            frame,
            theme,
            content_area,
            mode,
            subscribe_buf,
            "channel or pattern",
            "Enter subscribes · a glob (* ? [) makes it a pattern (PSUBSCRIBE)",
        ),
        PanelTab::Tail => anchor_input(
            frame,
            theme,
            content_area,
            mode,
            subscribe_buf,
            "stream key",
            "Enter tails · leave empty to tail the selected stream key",
        ),
        PanelTab::Monitor | PanelTab::Keyspace | PanelTab::Sub(_) => {
            match conn.panel_subscription() {
                Some(sub) => tail_content(frame, sub, theme, content_area),
                None => {
                    frame.render_widget(
                        Paragraph::new(Line::styled("starting feed…", theme.dim)),
                        content_area,
                    );
                }
            }
        }
    }
}

/// The spans for one tab in the panel's title strip: the label (highlighted when
/// active, otherwise dim) plus any live-feed marks.
fn tab_spans(
    slot: &PanelTab,
    conn: &Connection,
    active: bool,
    theme: &Theme,
) -> Vec<Span<'static>> {
    let base = if active { theme.selected } else { theme.dim };
    let mut spans = Vec::new();
    match slot {
        PanelTab::Console => spans.push(Span::styled("Console", base)),
        PanelTab::Monitor => {
            spans.push(Span::styled("Monitor", base));
            push_feed_marks(&mut spans, conn.monitor_sub(), theme);
        }
        PanelTab::Keyspace => {
            spans.push(Span::styled("Keyspace", base));
            push_feed_marks(&mut spans, conn.keyspace_sub(), theme);
        }
        PanelTab::PubSub => spans.push(Span::styled("Pub/Sub", base)),
        PanelTab::Tail => spans.push(Span::styled("Tail", base)),
        PanelTab::Sub(i) => match conn.subs.get(*i) {
            Some(sub) => {
                spans.push(Span::styled(sub.label.clone(), base));
                push_sub_marks(&mut spans, sub, theme);
            }
            None => spans.push(Span::styled("?", base)),
        },
    }
    spans
}

/// Append a fixed-anchor feed's recording (●) and paused (⏸) marks when it is live.
fn push_feed_marks(spans: &mut Vec<Span<'static>>, sub: Option<&Subscription>, theme: &Theme) {
    if let Some(sub) = sub {
        if sub.recording.is_on() {
            spans.push(Span::styled(" ●", theme.error));
        }
        if !sub.follow {
            spans.push(Span::styled(" ⏸", theme.accent));
        }
    }
}

/// Append a pub/sub or stream tail tab's recording (●) and lifecycle (… / ✗) marks.
fn push_sub_marks(spans: &mut Vec<Span<'static>>, sub: &Subscription, theme: &Theme) {
    if sub.recording.is_on() {
        spans.push(Span::styled(" ●", theme.error));
    }
    match &sub.state {
        SubState::Connecting => spans.push(Span::styled(" …", theme.dim)),
        SubState::Ended(_) => spans.push(Span::styled(" ✗", theme.dim)),
        SubState::Active => {}
    }
}

/// An input-anchor's content: the always-shown prompt echoing the typed text
/// (with a cursor while the tab is focused) over a one-line hint.
fn anchor_input(
    frame: &mut Frame,
    theme: &Theme,
    area: Rect,
    mode: InputMode,
    buf: &str,
    label: &str,
    hint: &str,
) {
    let [prompt_area, _gap, hint_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(area);
    let cursor = if mode == InputMode::Subscribe {
        "▏"
    } else {
        ""
    };
    let prompt = Line::from(vec![
        Span::styled(format!("{label}: "), theme.dim),
        Span::raw(buf.to_string()),
        Span::styled(cursor, theme.accent),
    ]);
    frame.render_widget(Paragraph::new(prompt), prompt_area);
    frame.render_widget(Paragraph::new(Line::styled(hint, theme.dim)), hint_area);
}

/// A tail tab's content: a one-line status row (live state, event tally, and the
/// recording / paused indicators) over the tail's event scrollback (preceded by
/// a wrapped advisory banner when the tail carries a notice). Ported from the
/// former standalone Realtime screen, now rendered inside the bottom panel.
fn tail_content(frame: &mut Frame, sub: &Subscription, theme: &Theme, area: Rect) {
    let [status_area, body_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);

    let state_span = match &sub.state {
        SubState::Connecting => Span::styled("connecting…", theme.dim),
        SubState::Active => Span::styled("live", theme.success),
        SubState::Ended(reason) => Span::styled(
            format!(
                "ended{}",
                reason
                    .as_ref()
                    .map(|r| format!(" ({r})"))
                    .unwrap_or_default()
            ),
            theme.error,
        ),
    };
    // Status row, balanced like the Browser's info bar: the live state and the
    // event tally sit flush left, while the recording and pause indicators are
    // pinned right so they don't shove the tally around as they come and go.
    let mut left = vec![
        Span::raw(" "),
        state_span,
        Span::styled(" · ", theme.dim),
        Span::raw(sub.received.to_string()),
        Span::styled(" events", theme.dim),
    ];
    if sub.received as usize > sub.events.len() {
        left.push(Span::styled(
            format!(" (last {})", sub.events.len()),
            theme.dim,
        ));
    }
    let mut right: Vec<Span> = Vec::new();
    if let RecordState::On { records, bytes, .. } = &sub.recording {
        right.push(Span::styled(
            format!("● REC {records} ({})", human_bytes(*bytes)),
            theme.error,
        ));
    }
    if !sub.follow {
        if !right.is_empty() {
            right.push(Span::styled(" · ", theme.dim));
        }
        right.push(Span::styled("⏸ paused", theme.accent));
    }
    if !right.is_empty() {
        right.push(Span::raw(" "));
    }
    let right = Line::from(right);
    let [st_left, st_right] = Layout::horizontal([
        Constraint::Min(0),
        Constraint::Length(line_width(&right) as u16),
    ])
    .areas(status_area);
    frame.render_widget(
        Paragraph::new(Line::from(left)).style(theme.status_bar),
        st_left,
    );
    frame.render_widget(Paragraph::new(right).style(theme.status_bar), st_right);

    // A keyspace/advisory notice takes a wrapped banner above the events.
    let events_area = match &sub.notice {
        Some(notice) => {
            let [banner, rest] =
                Layout::vertical([Constraint::Length(2), Constraint::Min(0)]).areas(body_area);
            frame.render_widget(
                Paragraph::new(Line::styled(format!("⚠ {notice}"), theme.error))
                    .wrap(Wrap { trim: true }),
                banner,
            );
            rest
        }
        None => body_area,
    };

    let height = events_area.height as usize;
    let len = sub.events.len();
    if len == 0 || height == 0 {
        frame.render_widget(
            Paragraph::new(Line::styled("waiting for events…", theme.dim)),
            events_area,
        );
        return;
    }
    // Window the ring buffer: `offset` events back from the newest, `height` tall.
    let bottom = len - 1 - sub.offset.min(len - 1);
    let top = (bottom + 1).saturating_sub(height);
    let lines: Vec<Line> = (top..=bottom)
        .filter_map(|i| sub.events.get(i))
        .map(|ev| event_line(ev, theme))
        .collect();
    frame.render_widget(Paragraph::new(lines), events_area);
}

/// Recordings screen: a list of the JSONL files in the recordings directory on
/// the left, and a read-only preview of the selected recording on the right.
pub fn recordings(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    if app.recordings.is_empty() {
        let block = Block::bordered()
            .title(" Recordings ")
            .title_style(theme.heading)
            .border_style(theme.border);
        let body = Paragraph::new(vec![
            Line::from(""),
            Line::styled("No recordings found.", theme.dim),
            Line::from(""),
            Line::from("Open a feed tab in the Browser, then 'r' to record — files land"),
            Line::from("in the recordings directory."),
        ])
        .alignment(Alignment::Center)
        .block(block);
        frame.render_widget(body, area);
        return;
    }
    let [list_area, preview_area] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(area);
    recordings_list(frame, app, theme, list_area);
    recording_preview(frame, app, theme, preview_area);
}

/// The left pane: one row per recording. The name column flexes to the pane
/// width; the size and modified-time columns are fixed-width tails.
fn recordings_list(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    let block = Block::bordered()
        .title(" Recordings ")
        .title_style(theme.heading)
        .border_style(theme.border);
    // Inner width minus the borders (2) and the highlight symbol (2).
    let inner_w = area.width.saturating_sub(4) as usize;
    const SIZE_COL: usize = 12; // right-aligned size + two trailing spaces
    const DATE_COL: usize = 16; // "YYYY-MM-DD HH:MM"
    let name_w = inner_w.saturating_sub(SIZE_COL + DATE_COL).max(8);
    let items: Vec<ListItem> = app
        .recordings
        .iter()
        .map(|f| {
            let when = f
                .modified
                .map(fmt_datetime)
                .unwrap_or_else(|| "?".to_string());
            ListItem::new(Line::from(vec![
                Span::raw(pad_end(&truncate(&f.name, name_w), name_w)),
                Span::styled(format!("{:>10}  ", human_bytes(f.size)), theme.dim),
                Span::styled(when, theme.dim),
            ]))
        })
        .collect();
    let list = List::new(items)
        .block(block)
        .highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    frame.render_stateful_widget(list, area, &mut app.recordings_state);
}

/// The right pane: a metadata header plus the head of the selected recording's
/// records (bounded by [`crate::recording::PREVIEW_CAP`]). Lines past the pane
/// height are clipped — this is a preview, not a full scrollable viewer.
fn recording_preview(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let block = Block::bordered()
        .title(" Preview ")
        .title_style(theme.heading)
        .border_style(theme.border);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some((name, preview)) = &app.recording_preview else {
        frame.render_widget(
            Paragraph::new(Line::styled("Select a recording to preview it.", theme.dim))
                .alignment(Alignment::Center),
            inner,
        );
        return;
    };

    let width = inner.width as usize;
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::styled(truncate(name, width), theme.heading));
    // Source type / connection, taken from the first record.
    let mut meta = String::new();
    if let Some(source_type) = &preview.source_type {
        meta.push_str(source_type);
    }
    if let Some(connection) = &preview.connection {
        if !meta.is_empty() {
            meta.push_str(" · ");
        }
        meta.push_str(connection);
    }
    if !meta.is_empty() {
        lines.push(Line::styled(meta, theme.dim));
    }
    // Record count · size · modified time.
    if let Some(file) = app.recordings.iter().find(|f| &f.name == name) {
        let n = preview.records.len();
        let count = match (preview.truncated, n) {
            (true, _) => format!("{n}+ records"),
            (false, 1) => "1 record".to_string(),
            (false, _) => format!("{n} records"),
        };
        let when = file
            .modified
            .map(fmt_datetime)
            .unwrap_or_else(|| "?".to_string());
        lines.push(Line::styled(
            format!("{count} · {} · {when}", human_bytes(file.size)),
            theme.dim,
        ));
    }
    lines.push(Line::from(""));

    if let Some(err) = &preview.error {
        lines.push(Line::styled(format!("error: {err}"), theme.error));
    } else if preview.records.is_empty() {
        lines.push(Line::styled("(empty recording)", theme.dim));
    }

    for rec in &preview.records {
        let seq = format!("#{:<4} ", rec.seq);
        let time = format!("{} ", rec.time);
        let source = format!("{}  ", rec.source);
        let used = UnicodeWidthStr::width(seq.as_str())
            + UnicodeWidthStr::width(time.as_str())
            + UnicodeWidthStr::width(source.as_str());
        let avail = width.saturating_sub(used).max(1);
        lines.push(Line::from(vec![
            Span::styled(seq, theme.dim),
            Span::styled(time, theme.dim),
            Span::styled(source, theme.accent),
            Span::raw(truncate(&rec.payload, avail)),
        ]));
    }
    if preview.truncated {
        lines.push(Line::styled(
            format!("… first {} records shown", preview.records.len()),
            theme.dim,
        ));
    }

    frame.render_widget(Paragraph::new(lines), inner);
}

/// The console tab's content: the read-only command output for the active
/// connection (windowed scrollback) with the input prompt pinned to the bottom.
/// The panel draws the surrounding border/title; this fills the content area.
fn console_content(
    frame: &mut Frame,
    conn: &Connection,
    mode: InputMode,
    theme: &Theme,
    area: Rect,
) {
    let console = &conn.console;

    let [output_area, prompt_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);

    // Flatten the executed commands + replies into display lines.
    let mut lines: Vec<Line> = Vec::new();
    if console.entries.is_empty() {
        lines.push(Line::styled("Read-only command console.", theme.dim));
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "Just type a command and press Enter to run. Writes and admin commands are refused.",
            theme.dim,
        ));
        lines.push(Line::styled(
            "Try: INFO server · CONFIG GET maxmemory · TYPE mykey · LRANGE mylist 0 -1",
            theme.dim,
        ));
    }
    for entry in &console.entries {
        lines.push(Line::from(vec![
            Span::styled("❯ ", theme.accent),
            Span::styled(entry.command.clone(), theme.heading),
        ]));
        let style = if entry.is_error {
            theme.error
        } else {
            theme.success
        };
        for line in entry.output.lines() {
            lines.push(Line::styled(line.to_string(), style));
        }
        lines.push(Line::from(""));
    }

    // Window the output: `scroll` is an offset back from the bottom (0 == tail).
    let total = lines.len();
    let height = output_area.height as usize;
    let max_off = total.saturating_sub(height);
    let off = (console.scroll as usize).min(max_off);
    let end = total - off;
    let start = end.saturating_sub(height);
    let visible: Vec<Line> = lines[start..end].to_vec();
    frame.render_widget(
        Paragraph::new(visible).wrap(Wrap { trim: false }),
        output_area,
    );

    // Prompt line: the in-flight command, or the editable input.
    let prompt = if let Some(pending) = &console.pending {
        Line::from(vec![
            Span::styled("… ", theme.accent),
            Span::styled(format!("running {pending}"), theme.dim),
        ])
    } else {
        let cursor = if mode == InputMode::Command {
            "▏"
        } else {
            ""
        };
        Line::from(vec![
            Span::styled("❯ ", theme.accent),
            Span::raw(format!("{}{cursor}", console.input)),
        ])
    };
    frame.render_widget(Paragraph::new(prompt), prompt_area);
}

/// The add-connection modal overlay.
pub fn conn_form(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(form) = app.form.as_ref() else {
        return;
    };
    let rect = centered(area, 60, 16);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Add connection ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let mut lines: Vec<Line> = Vec::new();
    for (i, base_label) in ConnForm::LABELS.iter().enumerate() {
        // Slot 3 is shared: a Redis DB index or a RabbitMQ vhost, relabelled to
        // suit. AMQP is not database-scoped, so the row is omitted entirely.
        if i == ConnForm::SLOT3_FIELD && !ConnForm::slot3_shown(form.kind) {
            continue;
        }
        let label = if i == ConnForm::SLOT3_FIELD {
            ConnForm::slot3_label(form.kind)
        } else {
            base_label
        };
        let focused = form.focus == i;
        let cursor = if focused { "▏" } else { "" };
        let label_style = if focused { theme.accent } else { theme.dim };
        lines.push(Line::from(vec![
            Span::styled(format!("{label:>9}: "), label_style),
            Span::raw(format!("{}{cursor}", form.fields[i])),
        ]));
    }
    let tls_focused = form.focus == ConnForm::TLS_FOCUS;
    lines.push(Line::from(vec![
        Span::styled(
            format!("{:>9}: ", "TLS"),
            if tls_focused { theme.accent } else { theme.dim },
        ),
        Span::raw(if form.tls { "[x]" } else { "[ ]" }),
        Span::styled("  (space toggles)", theme.dim),
    ]));
    let kind_focused = form.focus == ConnForm::KIND_FOCUS;
    let kind = match form.kind {
        BrokerKind::Redis => "redis",
        BrokerKind::Amqp => "amqp",
        BrokerKind::Rabbitmq => "rabbitmq",
    };
    lines.push(Line::from(vec![
        Span::styled(
            format!("{:>9}: ", "Kind"),
            if kind_focused {
                theme.accent
            } else {
                theme.dim
            },
        ),
        Span::raw(format!("[{kind}]")),
        Span::styled("  (space cycles)", theme.dim),
    ]));
    lines.push(Line::from(""));
    // One consolidated per-kind note (defined alongside each kind's defaults).
    lines.push(Line::styled(ConnForm::kind_note(form.kind), theme.dim));
    lines.push(Line::styled(
        "Password: env:VAR · keyring · prompt · or a literal (session only)",
        theme.dim,
    ));
    if let Some(err) = &form.error {
        lines.push(Line::styled(format!("⚠ {err}"), theme.error));
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "Tab/Shift-Tab move · Enter save & connect · Esc cancel",
        theme.dim,
    ));
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

/// The help overlay.
pub fn help(frame: &mut Frame, theme: &Theme, area: Rect) {
    let lines = vec![
        Line::styled("Navigation", theme.heading),
        Line::from("  ↑/k ↓/j move   g/G top/bottom   Ctrl-u/d page   mouse wheel scrolls"),
        Line::from("  Enter connect (Connections)   Esc step back / quit"),
        Line::from(""),
        Line::styled("Screens", theme.heading),
        Line::from("  b browser  R recordings"),
        Line::from(""),
        Line::styled("Browser", theme.heading),
        Line::from("  server stats (Redis) appear in a band above the panes"),
        Line::from("  / filter   [ ] change DB   o sort column   O direction"),
        Line::from("  keys nest into collapsible groups by each ':' (start folded)"),
        Line::from("  Enter/Space collapse/expand group   z fold/unfold all"),
        Line::from("  PgUp/PgDn scroll the value pane   keys auto-refresh"),
        Line::from(""),
        Line::styled("Bottom panel (Redis)", theme.heading),
        Line::from("  fixed tabs: Console · Monitor · Keyspace · Pub/Sub · Tail"),
        Line::from("  plus one tab per pub/sub or stream tail"),
        Line::from("  Tab / Shift-Tab cycle tabs (the only way to switch)"),
        Line::from("  Monitor/Keyspace run while focused, start paused · p play/pause"),
        Line::from("  Pub/Sub & Tail: type in the tab, Enter subscribes/tails"),
        Line::from("  (empty Tail = selected key · a glob makes a pattern)"),
        Line::from("  r record the focused feed   x close a pub/sub or tail tab"),
        Line::from(""),
        Line::styled("Console tab (read-only)", theme.heading),
        Line::from("  type a command, Enter runs   Ctrl-P/N history"),
        Line::from("  Ctrl-L clear   PgUp/PgDn scroll   writes/admin refused"),
        Line::from(""),
        Line::styled("General", theme.heading),
        Line::from("  a add connection   ? toggle help   Esc back   Ctrl-c quit"),
        Line::from("  m toggle mouse capture (off = select/copy text)"),
    ];
    // Grow the overlay with its content (lines + 2 borders) so no row is
    // clipped, capped to the available height.
    let height = (lines.len() as u16 + 2).clamp(6, area.height.max(6));
    let rect = centered(area, 66, height);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Help ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    frame.render_widget(Paragraph::new(lines).block(block), rect);
}

// -- helpers ----------------------------------------------------------------

fn render_value(theme: &Theme, view: Option<&ValueView>) -> Vec<Line<'static>> {
    let Some(view) = view else {
        return vec![Line::styled("loading…", theme.dim)];
    };
    match view {
        ValueView::Missing => vec![Line::styled("(key not found)", theme.dim)],
        ValueView::Str {
            total_bytes,
            shown_bytes,
            text,
            encoding,
        } => {
            let mut lines = vec![
                Line::styled(
                    format!("string · {encoding:?} · showing {shown_bytes} of {total_bytes} bytes"),
                    theme.dim,
                ),
                Line::from(""),
            ];
            lines.extend(text.lines().map(|l| Line::from(l.to_string())));
            if shown_bytes < total_bytes {
                lines.push(Line::from(""));
                lines.push(Line::styled("… (truncated)", theme.dim));
            }
            lines
        }
        ValueView::List { len, offset, items } => {
            let mut lines = vec![
                Line::styled(format!("list · {len} items"), theme.dim),
                Line::from(""),
            ];
            lines.extend(
                items
                    .iter()
                    .enumerate()
                    .map(|(i, it)| Line::from(format!("{:>5}  {it}", offset + i))),
            );
            lines
        }
        ValueView::Set { len, members } => {
            let mut lines = vec![
                Line::styled(format!("set · {len} members"), theme.dim),
                Line::from(""),
            ];
            lines.extend(members.iter().map(|m| Line::from(format!("• {m}"))));
            lines
        }
        ValueView::Hash { len, fields, .. } => {
            let mut lines = vec![
                Line::styled(format!("hash · {len} fields"), theme.dim),
                Line::from(""),
            ];
            lines.extend(fields.iter().map(|(k, v)| {
                Line::from(vec![
                    Span::styled(format!("{k}: "), theme.accent),
                    Span::raw(v.clone()),
                ])
            }));
            lines
        }
        ValueView::ZSet { len, items, .. } => {
            let mut lines = vec![
                Line::styled(format!("zset · {len} members"), theme.dim),
                Line::from(""),
            ];
            lines.extend(
                items
                    .iter()
                    .map(|(m, s)| Line::from(format!("{s:>12}  {m}"))),
            );
            lines
        }
        ValueView::Stream {
            len,
            last_id,
            entries,
        } => {
            let mut lines = vec![
                Line::styled(
                    format!("stream · {len} entries · last {last_id}"),
                    theme.dim,
                ),
                Line::from(""),
            ];
            for e in entries {
                lines.push(Line::styled(e.id.clone(), theme.accent));
                lines.extend(
                    e.fields
                        .iter()
                        .map(|(k, v)| Line::from(format!("    {k} = {v}"))),
                );
            }
            lines
        }
    }
}

/// Render one realtime event as a single log line: `time  source  [id] payload`.
fn event_line(ev: &BrokerEvent, theme: &Theme) -> Line<'static> {
    let ts = format!(
        "{:02}:{:02}:{:02}.{:03}",
        ev.ts.hour(),
        ev.ts.minute(),
        ev.ts.second(),
        ev.ts.millisecond()
    );
    let mut spans = vec![
        Span::styled(ts, theme.dim),
        Span::raw("  "),
        Span::styled(pad_end(&truncate(&ev.source, 18), 18), theme.accent),
        Span::raw(" "),
    ];
    if let Some(id) = ev.meta("id") {
        spans.push(Span::styled(format!("{id} "), theme.dim));
    }
    // A RabbitMQ exchange tap reports the exchange as the source; the per-message
    // routing key rides in meta and is shown here, like a stream entry's id.
    if let Some(rk) = ev.meta("routing_key") {
        spans.push(Span::styled(format!("{rk} "), theme.dim));
    }
    spans.push(Span::raw(payload_preview(&ev.payload, 400)));
    Line::from(spans)
}

/// A single-line, length-capped preview of a payload (whitespace collapsed).
fn payload_preview(payload: &Payload, max: usize) -> String {
    let raw = match payload {
        Payload::Utf8(s) | Payload::Json(s) => s.clone(),
        Payload::Binary(_) => format!("base64:{}", payload.as_text()),
    };
    let flat = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate(&flat, max)
}

/// Truncate to a display width of `max` columns, appending an ellipsis when
/// shortened. Width-aware so wide (CJK/emoji) characters don't overflow the
/// column and break alignment of whatever follows.
fn truncate(s: &str, max: usize) -> String {
    if UnicodeWidthStr::width(s) <= max {
        return s.to_string();
    }
    // Reserve one column for the ellipsis.
    let budget = max.saturating_sub(1);
    let mut width = 0;
    let mut out = String::new();
    for ch in s.chars() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width + w > budget {
            break;
        }
        width += w;
        out.push(ch);
    }
    out.push('…');
    out
}

/// Right-pad `s` with spaces to a display width of `width` columns. Pairs with
/// [`truncate`] for fixed-width columns: `pad_end(&truncate(s, n), n)` yields a
/// cell exactly `n` columns wide regardless of wide characters.
fn pad_end(s: &str, width: usize) -> String {
    let w = UnicodeWidthStr::width(s);
    let mut out = s.to_string();
    if w < width {
        out.push_str(&" ".repeat(width - w));
    }
    out
}

/// Format a timestamp as `YYYY-MM-DD HH:MM` for the recordings list.
fn fmt_datetime(t: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute()
    )
}

/// The display width (terminal columns) of a composed line — the sum of its
/// spans' widths. Used to size the right-pinned segment of a balanced toolbar
/// so the flexible left segment can take the rest of the row.
fn line_width(line: &Line) -> usize {
    line.spans
        .iter()
        .map(|s| UnicodeWidthStr::width(s.content.as_ref()))
        .sum()
}

/// Clamp a gauge ratio into `[0, 1]`, mapping non-finite values (NaN/∞) to 0.
/// `f64::clamp` passes NaN through unchanged, which would trip `Gauge::ratio`'s
/// internal `0.0..=1.0` assertion and panic the render.
fn gauge_ratio(r: f64) -> f64 {
    if r.is_finite() {
        r.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn human_duration(secs: u64) -> String {
    let (d, h, m, s) = (
        secs / 86400,
        (secs % 86400) / 3600,
        (secs % 3600) / 60,
        secs % 60,
    );
    if d > 0 {
        format!("{d}d {h}h {m}m")
    } else if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// A centered sub-rectangle of `area`, clamped to fit.
pub fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    Rect {
        x: area.x + (area.width.saturating_sub(w)) / 2,
        y: area.y + (area.height.saturating_sub(h)) / 2,
        width: w,
        height: h,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn visible_offset_keeps_selection_in_window() {
        // 200 rows, a 10-row viewport. From a settled top window, scrolling the
        // selection down past the bottom edge advances the window by one row…
        assert_eq!(
            visible_offset(0, Some(9), 10, 200),
            0,
            "last visible row stays"
        );
        assert_eq!(
            visible_offset(0, Some(10), 10, 200),
            1,
            "one past pushes down"
        );
        // …and the window otherwise stays put while the selection is inside it.
        assert_eq!(
            visible_offset(50, Some(55), 10, 200),
            50,
            "in-window: no move"
        );
        // Scrolling above the window pulls it straight up to the selection.
        assert_eq!(visible_offset(50, Some(40), 10, 200), 40, "above pulls up");
        // A stale/over-large offset (e.g. after a group collapse shrank the view)
        // is clamped so the last rows can't sit above an empty viewport.
        assert_eq!(
            visible_offset(190, None, 10, 30),
            20,
            "clamped to max_offset"
        );
        assert_eq!(visible_offset(0, Some(0), 10, 0), 0, "empty view");
        // A viewport taller than the list pins the window at the top.
        assert_eq!(visible_offset(0, Some(3), 50, 5), 0, "fits entirely");
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.0 GiB");
    }

    #[test]
    fn human_duration_picks_the_coarsest_units() {
        assert_eq!(human_duration(0), "0s");
        assert_eq!(human_duration(59), "59s");
        assert_eq!(human_duration(60), "1m 0s");
        assert_eq!(human_duration(3661), "1h 1m 1s");
        assert_eq!(human_duration(90061), "1d 1h 1m");
    }

    #[test]
    fn truncate_caps_and_marks_overflow() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(
            truncate("hello", 5),
            "hello",
            "exactly at the cap is unchanged"
        );
        assert_eq!(truncate("hello", 3), "he…");
        // Counts characters, not bytes, so multibyte text is handled safely.
        assert_eq!(truncate("héllo", 3), "hé…");
    }

    #[test]
    fn payload_preview_flattens_whitespace_and_tags_binary() {
        assert_eq!(
            payload_preview(&Payload::Utf8("a\n  b\tc".into()), 100),
            "a b c"
        );
        assert_eq!(
            payload_preview(&Payload::Json("{\"a\":\n1}".into()), 100),
            "{\"a\": 1}"
        );
        let bin = payload_preview(&Payload::Binary(vec![0x00, 0xff]), 100);
        assert!(
            bin.starts_with("base64:"),
            "binary previews are tagged: {bin}"
        );
    }

    #[test]
    fn payload_preview_truncates_long_text() {
        let long = "x".repeat(50);
        let preview = payload_preview(&Payload::Utf8(long), 10);
        assert_eq!(preview.chars().count(), 10);
        assert!(preview.ends_with('…'));
    }

    #[test]
    fn fmt_datetime_uses_minute_precision() {
        assert_eq!(
            fmt_datetime(datetime!(2026 - 06 - 19 09:08:07 UTC)),
            "2026-06-19 09:08"
        );
    }

    #[test]
    fn centered_positions_and_clamps() {
        let area = Rect::new(0, 0, 100, 40);
        let r = centered(area, 60, 16);
        assert_eq!((r.x, r.y, r.width, r.height), (20, 12, 60, 16));
        // An oversized request is clamped to the available area.
        let big = centered(area, 200, 100);
        assert_eq!((big.x, big.y, big.width, big.height), (0, 0, 100, 40));
    }

    #[test]
    fn truncate_and_pad_are_display_width_aware() {
        // CJK characters are 2 columns wide; "日本語" is 6 columns.
        assert_eq!(truncate("日本語", 4), "日…", "caps to the column budget");
        assert_eq!(
            pad_end("日", 4),
            "日  ",
            "pads by display width, not char count"
        );
        // ASCII is unchanged when it fits.
        assert_eq!(pad_end("ab", 4), "ab  ");
        assert_eq!(truncate("abcd", 10), "abcd");
    }

    /// Flatten a rendered line's spans into a plain string for content assertions.
    fn line_text(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn line_width_sums_display_columns_not_bytes() {
        // Three spans: ASCII, a multi-byte-but-single-column char, and a CJK
        // (two-column) char. Width is columns, not bytes or chars.
        let line = Line::from(vec![
            Span::raw("ab"), // 2 cols
            Span::raw("é"),  // 1 col, 2 bytes
            Span::raw("日"), // 2 cols
        ]);
        assert_eq!(line_width(&line), 5);
        assert_eq!(line_width(&Line::from("")), 0);
    }

    #[test]
    fn meter_line_fills_proportionally_with_brackets_and_value() {
        let theme = Theme::dark();
        // Count the filled cells the meter draws at a given ratio and width.
        let filled = |ratio: f64, width: usize| -> usize {
            meter_line(&theme, "Mem", ratio, "50%", width)
                .spans
                .iter()
                .map(|s| s.content.matches('█').count())
                .sum()
        };
        // chrome = "Mem" (3) + space + brackets (2) + space + "50%" (3) = 10,
        // so a width-30 meter has a 20-cell bar. Half full ⇒ 10 filled cells.
        assert_eq!(filled(0.5, 30), 10);
        assert_eq!(filled(0.0, 30), 0, "empty");
        assert_eq!(filled(1.0, 30), 20, "full bar uses every cell");
        // Out-of-range / non-finite ratios are clamped, never panicking.
        assert_eq!(filled(2.0, 30), 20, "over 100% clamps to full");
        assert_eq!(filled(f64::NAN, 30), 0, "NaN clamps to empty");

        // The composed line carries the label, both bracket caps, and the value.
        let text = line_text(&meter_line(&theme, "Hit", 0.9, "90.0%", 40));
        assert!(text.contains("Hit"), "label present: {text:?}");
        assert!(
            text.contains('▕') && text.contains('▏'),
            "bracket caps: {text:?}"
        );
        assert!(text.contains("90.0%"), "value present: {text:?}");
    }

    #[test]
    fn meter_line_degrades_when_too_narrow_for_a_bar() {
        let theme = Theme::dark();
        // Narrower than the chrome ⇒ no bar cells, but it must not panic and
        // still carries the value.
        let line = meter_line(&theme, "Memory", 0.5, "1.0 / 4.0 MiB", 4);
        let text = line_text(&line);
        assert_eq!(text.matches('█').count(), 0, "no room for a bar");
        assert!(text.contains("1.0 / 4.0 MiB"));
    }

    #[test]
    fn event_line_shows_source_id_and_payload() {
        let theme = Theme::dark();
        let ev = BrokerEvent {
            ts: OffsetDateTime::UNIX_EPOCH,
            source: "orders".into(),
            payload: Payload::Utf8("hello world".into()),
            meta: vec![("id".into(), "1-0".into())],
        };
        let text = line_text(&event_line(&ev, &theme));
        assert!(text.contains("orders"), "source rendered: {text:?}");
        assert!(
            text.contains("1-0"),
            "stream id from meta rendered: {text:?}"
        );
        assert!(text.contains("hello world"), "payload rendered: {text:?}");
    }

    #[test]
    fn render_value_marks_truncation_and_numbers_list_offsets() {
        let theme = Theme::dark();
        let truncated = render_value(
            &theme,
            Some(&ValueView::Str {
                total_bytes: 100,
                shown_bytes: 10,
                text: "abcdefghij".into(),
                encoding: crate::broker::PayloadEncoding::Utf8,
            }),
        );
        let text: String = truncated
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("showing 10 of 100 bytes"),
            "size header: {text:?}"
        );
        assert!(text.contains("truncated"), "truncation note: {text:?}");

        // List rows are numbered from the page offset, not from zero.
        let list = render_value(
            &theme,
            Some(&ValueView::List {
                len: 5,
                offset: 3,
                items: vec!["x".into(), "y".into()],
            }),
        );
        let ltext: String = list.iter().map(line_text).collect::<Vec<_>>().join("\n");
        assert!(ltext.contains("list · 5 items"));
        assert!(
            ltext.contains('3') && ltext.contains('4'),
            "offsets 3,4: {ltext:?}"
        );
    }
}
