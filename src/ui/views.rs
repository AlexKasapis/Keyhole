//! Per-screen rendering. Each function draws one screen (or overlay) from the
//! current [`App`] state into the given area.

use std::collections::VecDeque;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, HighlightSpacing, List, ListItem, ListState, Padding, Paragraph,
    RenderDirection, Row, Sparkline, Table, TableState, Wrap,
};
use ratatui::Frame;
use time::OffsetDateTime;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::app::{
    App, ConnForm, ConnHealth, Connection, DestKind, InputMode, PaletteCommand, PaneFocus,
    PanelTab, RecordState, Screen, SettingsRow, SubState, Subscription, ViewRow,
};
use crate::broker::{BrokerEvent, BrokerKind, ClientInfo, Payload, ServerStats, Ttl, ValueView};
use crate::config::AnimationSpeed;
use crate::theme::Theme;

/// The merged home area: a single bordered box whose title is the tab strip
/// (Connections │ Recordings), the active tab highlighted. The body shows the
/// active tab's content. One frame, no inner box for the tab chrome — the same
/// single-frame treatment as the Browser's bottom panel.
pub fn home(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    let on_recordings = app.screen == Screen::Recordings;
    // The home area is the primary surface, so it reads at full brightness: the
    // box and both tab labels use the main foreground (the dim border/labels made
    // it look perpetually unfocused). The active tab additionally carries the
    // selection highlight (with a bright foreground) so it stays the standout.
    let tab = |label: &'static str, active: bool| {
        Span::styled(
            label,
            if active {
                theme.tab_selected
            } else {
                Style::default()
            },
        )
    };
    let title = Line::from(vec![
        Span::raw(" "),
        tab("Connections", !on_recordings),
        Span::styled(" │ ", theme.dim),
        tab("Recordings", on_recordings),
        Span::raw(" "),
    ]);
    let block = Block::bordered()
        .title(title)
        .border_style(Style::default());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if on_recordings {
        recordings_body(frame, app, theme, inner);
    } else {
        connections_body(frame, app, theme, inner);
    }
}

/// The Connections tab body: the list of saved profiles. Borderless — the home
/// area's box is the only frame.
fn connections_body(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    if app.profiles.is_empty() {
        let body = Paragraph::new(vec![
            Line::from(""),
            Line::styled("No saved connections.", theme.dim),
            Line::from(""),
            Line::from("Press 'a' to add one."),
        ])
        .alignment(Alignment::Center);
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
            // A live connection's dot breathes so the row reads as alive (when
            // animation is on; steady when off).
            (
                "●",
                crate::ui::anim::pulse(theme.success, app.now, app.animation_speed()),
            )
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
    // The Server band's connected dot pulses on the tick clock; capture `now`
    // and the animation setting before the `&mut` borrow of the active
    // connection below.
    let now = app.now;
    let anim_speed = app.animation_speed();
    let panel_h = panel_band_height(frame.area().height);
    let Some(conn) = app.active_conn_mut() else {
        let body = Paragraph::new("No active connection. Press 'c', select a profile, and Enter.")
            .style(theme.dim)
            .block(Block::bordered().border_style(theme.border));
        frame.render_widget(body, area);
        return;
    };
    // Which pane owns the keyboard — used to tint the focused pane's border.
    let focus = conn.focus;

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
    // The bottom panel hosts the tails — present for Redis (alongside its
    // console) and AMQP (its only panel). See [`Capabilities::can_tail`].
    if conn.caps.can_tail() {
        rows.push(Constraint::Length(panel_h));
    }
    let chunks = Layout::vertical(rows).split(area);
    let band_area = conn.caps.can_dashboard.then(|| chunks[0]);
    // Body sits after the optional stats band.
    let body_idx = if conn.caps.can_dashboard { 1 } else { 0 };
    let body_area = chunks[body_idx];
    let panel_area = conn.caps.can_tail().then(|| chunks[body_idx + 1]);

    if let Some(band_area) = band_area {
        server_stats_band(frame, conn, health, now, anim_speed, theme, band_area);
    }

    let [table_area, value_area] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .areas(body_area);

    // AMQP browses a curated destination list (no key scan), so its body is a
    // destination list + queue-peek pane rather than the key table + value pane.
    if !conn.caps.uses_key_scan() {
        amqp_destination_pane(frame, conn, focus, theme, table_area);
        amqp_peek_pane(frame, conn, theme, value_area);
        if let Some(panel_area) = panel_area {
            panel_band(frame, conn, mode, &subscribe_buf, theme, panel_area);
        }
        return;
    }

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
                .border_style(border_style(theme, focus == PaneFocus::Keys)),
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

/// The AMQP browser's left pane: the curated destination list (the analog of the
/// Redis key table). Each row is a destination name with a dim `[topic]` /
/// `[queue]` tag; an empty list shows an add prompt instead.
fn amqp_destination_pane(
    frame: &mut Frame,
    conn: &mut Connection,
    focus: PaneFocus,
    theme: &Theme,
    area: Rect,
) {
    // The list is "focused" only when the keys pane has the keyboard *and* it
    // has not been handed onward to the message pane (the AMQP sub-focus).
    let list_focused = focus == PaneFocus::Keys && !conn.peek.focused;
    let block = Block::bordered()
        .title(" Destinations ")
        .title_style(theme.heading)
        .border_style(border_style(theme, list_focused));
    if conn.destinations.items.is_empty() {
        let hint = Paragraph::new(vec![
            Line::from(""),
            Line::styled("  No destinations yet.", theme.dim),
            Line::styled("  Press 'a' to add topic:name or queue:name.", theme.dim),
        ])
        .block(block);
        frame.render_widget(hint, area);
        return;
    }
    let rows: Vec<Row> = conn
        .destinations
        .items
        .iter()
        .map(|d| {
            Row::new(vec![
                Cell::from(d.name.clone()),
                Cell::from(Span::styled(format!("[{}]", d.kind.tag()), theme.dim)),
            ])
        })
        .collect();
    let widths = [Constraint::Min(10), Constraint::Length(8)];
    let table = Table::new(rows, widths)
        .header(Row::new(["Destination", "Kind"]).style(theme.header))
        .column_spacing(2)
        .block(block)
        .row_highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    frame.render_stateful_widget(table, area, &mut conn.destinations.table);
}

/// The AMQP browser's right pane: the messages from the last queue peek (the
/// analog of the Redis value pane). A topic shows a tail hint (topics don't
/// retain); a queue shows its peeked messages as a navigable, filterable list
/// with a count/limit title, an empty / pending state, or — when a message is
/// opened — the full single-message detail view (see [`amqp_detail_pane`]).
fn amqp_peek_pane(frame: &mut Frame, conn: &mut Connection, theme: &Theme, area: Rect) {
    let sel = conn.selected_destination().map(|d| (d.canonical(), d.kind));
    let queue_name = match sel.as_ref().map(|(name, kind)| (name.clone(), *kind)) {
        Some((name, DestKind::Topic)) => {
            return render_peek_hint(
                frame,
                theme,
                area,
                Some(&name),
                "Topics don't retain messages — press 't' or Enter to tail it live.",
            );
        }
        Some((name, DestKind::Queue)) => name,
        None => {
            return render_peek_hint(frame, theme, area, None, "Add a destination with 'a'.");
        }
    };

    if conn.peek.pending {
        return render_peek_hint(frame, theme, area, Some(&queue_name), "Peeking…");
    }
    if conn.peek.events.is_empty() {
        return render_peek_hint(
            frame,
            theme,
            area,
            Some(&queue_name),
            "No messages to show (empty queue, or peek mode is off).",
        );
    }

    // A message is opened: show its full body + metadata instead of the list.
    if conn.peek.detail {
        return amqp_detail_pane(frame, conn, theme, area, &queue_name);
    }

    // Otherwise the navigable message list, with a count/limit/filter title and a
    // selection highlight shown only while the pane holds the keyboard.
    let total = conn.peek.events.len();
    let shown = conn.peek.filtered_len();
    let title = peek_list_title(
        &queue_name,
        total,
        shown,
        &conn.peek.filter,
        conn.peek.limit_hit,
    );
    let focused = conn.peek.focused;
    let selected = conn.peek.selected;
    let indices = conn.peek.filtered_indices();

    let lines: Vec<Line> = if indices.is_empty() {
        vec![Line::styled(
            format!("No messages match \"{}\".", conn.peek.filter),
            theme.dim,
        )]
    } else {
        indices
            .iter()
            .enumerate()
            .map(|(row, &i)| {
                let mut line = event_line(&conn.peek.events[i], theme);
                if focused && row == selected {
                    line.spans.insert(0, Span::styled("▶ ", theme.accent));
                    line.style = theme.selected;
                } else {
                    line.spans.insert(0, Span::raw("  "));
                }
                line
            })
            .collect()
    };

    // Auto-follow the cursor when focused so the selection stays on screen;
    // otherwise clamp the manual scroll against the content height.
    let inner_h = area.height.saturating_sub(2) as usize;
    if focused && inner_h > 0 && !indices.is_empty() {
        let scroll = conn.peek.scroll as usize;
        if selected < scroll {
            conn.peek.scroll = selected as u16;
        } else if selected >= scroll + inner_h {
            conn.peek.scroll = (selected + 1 - inner_h) as u16;
        }
    }
    let max_scroll = lines.len().saturating_sub(inner_h) as u16;
    conn.peek.scroll = conn.peek.scroll.min(max_scroll);

    let border = if focused { theme.accent } else { theme.border };
    let para = Paragraph::new(lines)
        .block(
            Block::bordered()
                .title(title)
                .title_style(theme.heading)
                .border_style(border),
        )
        .scroll((conn.peek.scroll, 0));
    frame.render_widget(para, area);
}

/// Render the peek pane's non-list states (topic hint, empty/pending queue, no
/// selection) as a single dim line under the pane's border.
fn render_peek_hint(frame: &mut Frame, theme: &Theme, area: Rect, name: Option<&str>, msg: &str) {
    let title = match name {
        Some(n) => format!(" {n} "),
        None => " Messages ".to_string(),
    };
    let para = Paragraph::new(Line::styled(msg.to_owned(), theme.dim))
        .block(
            Block::bordered()
                .title(title)
                .title_style(theme.heading)
                .border_style(theme.border),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

/// The message-list pane title: destination plus a message count, a `+` when the
/// peek was truncated at the limit, or a `shown/total match "filter"` summary
/// while a filter is active.
fn peek_list_title(
    name: &str,
    total: usize,
    shown: usize,
    filter: &str,
    limit_hit: bool,
) -> String {
    if filter.is_empty() {
        let plus = if limit_hit { "+" } else { "" };
        format!(" {name} · {total}{plus} msgs ")
    } else {
        format!(" {name} · {shown}/{total} match \"{filter}\" ")
    }
}

/// The AMQP browser's single-message detail view: the selected message's
/// metadata (timestamp + every property/header/application property) followed by
/// its full body, pretty-printed when it is JSON. Scrollable, since a body can be
/// large.
fn amqp_detail_pane(
    frame: &mut Frame,
    conn: &mut Connection,
    theme: &Theme,
    area: Rect,
    queue_name: &str,
) {
    let position = conn.peek.selected + 1;
    let count = conn.peek.filtered_len();
    let mut lines: Vec<Line> = Vec::new();
    if let Some(ev) = conn.peek.selected_event() {
        let ts = format!(
            "{:02}:{:02}:{:02}.{:03}",
            ev.ts.hour(),
            ev.ts.minute(),
            ev.ts.second(),
            ev.ts.millisecond()
        );
        lines.push(Line::from(vec![
            Span::styled("received: ", theme.accent),
            Span::raw(ts),
        ]));
        for (k, v) in &ev.meta {
            lines.push(Line::from(vec![
                Span::styled(format!("{k}: "), theme.accent),
                Span::raw(v.clone()),
            ]));
        }
        lines.push(Line::raw(""));
        lines.push(Line::styled("── body ──", theme.dim));
        let body = match &ev.payload {
            Payload::Json(s) => pretty_json(s),
            other => other.as_text(),
        };
        // `str::lines` drops a trailing newline and never yields for an empty
        // body, so an empty payload renders as a single blank line.
        if body.is_empty() {
            lines.push(Line::raw(""));
        } else {
            for l in body.lines() {
                lines.push(Line::raw(l.to_owned()));
            }
        }
    } else {
        lines.push(Line::styled("No message selected.", theme.dim));
    }

    let inner_h = area.height.saturating_sub(2) as usize;
    let max_scroll = lines.len().saturating_sub(inner_h) as u16;
    conn.peek.scroll = conn.peek.scroll.min(max_scroll);
    let title = format!(" {queue_name} · message {position}/{count} ");
    let para = Paragraph::new(lines)
        .block(
            Block::bordered()
                .title(title)
                .title_style(theme.heading)
                .border_style(theme.accent),
        )
        .wrap(Wrap { trim: false })
        .scroll((conn.peek.scroll, 0));
    frame.render_widget(para, area);
}

/// Pretty-print a JSON string with two-space indentation; returns the original
/// text unchanged if it does not parse (it should, having been classified as
/// JSON, but this stays robust if that ever changes).
fn pretty_json(s: &str) -> String {
    serde_json::from_str::<serde_json::Value>(s)
        .ok()
        .and_then(|v| serde_json::to_string_pretty(&v).ok())
        .unwrap_or_else(|| s.to_owned())
}

/// Total height (rows) reserved for the Browser's Server band: a border (2)
/// wrapping a single identity line (name · address · db).
const SERVER_BAND_HEIGHT: u16 = 3;

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

/// The Server band shown atop the Browser for brokers that expose server
/// statistics (Redis). A compact strip identifying the live connection — its
/// name, `host:port` address, and the database in view — with the connection
/// health dot riding in the title beside the "Server" label. The fuller
/// statistics (memory, hit ratio, version, uptime, clients, ops/sec, keys) live
/// in the Server Details tab below.
fn server_stats_band(
    frame: &mut Frame,
    conn: &Connection,
    health: ConnHealth,
    now: OffsetDateTime,
    anim_speed: AnimationSpeed,
    theme: &Theme,
    area: Rect,
) {
    // A one-column inner margin keeps the content off the border on both sides,
    // so the band reads as a panel rather than text crammed against a frame.
    // The connection-health indicator (the former top-right header dot) rides in
    // the title here, beside the "Server" label.
    let (dot, hlabel, dot_style) = crate::ui::health_indicator(health, theme);
    // A connected dot breathes (when animation is on; steady when off);
    // transitional/offline states always hold steady.
    let dot_style = if health == ConnHealth::Connected {
        crate::ui::anim::pulse(dot_style, now, anim_speed)
    } else {
        dot_style
    };
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
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // One identifying line: the connection name leads, then its address and the
    // database currently in view. Separators and the `db` label stay dim so the
    // name and figures read.
    let line = Line::from(vec![
        Span::styled(conn.name.clone(), theme.heading),
        Span::styled(" · ", theme.dim),
        Span::raw(conn.handle.addr.clone()),
        Span::styled(" · ", theme.dim),
        Span::styled("db", theme.dim),
        Span::raw(conn.db.to_string()),
    ]);
    frame.render_widget(Paragraph::new(line), inner);
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
        .border_style(border_style(theme, conn.focus == PaneFocus::Bottom));
    let content_area = block.inner(area);
    frame.render_widget(block, area);

    // Content follows the active tab.
    let active = conn.active_panel();
    match active {
        PanelTab::ServerDetails => server_details_content(frame, conn, theme, content_area),
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
        // AMQP's single Tail anchor accepts a full destination spec.
        PanelTab::Tail if !conn.caps.uses_key_scan() => anchor_input(
            frame,
            theme,
            content_area,
            mode,
            subscribe_buf,
            "topic:name or queue:name",
            "Enter tails it · topic = live multicast · queue = non-destructive browse",
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
    // The active tab is the standout (selection highlight + bright foreground);
    // inactive tabs use the brighter `tab_inactive` foreground — readable yet
    // clearly secondary — so all tabs stay legible even while the panel is
    // unfocused, with the selected one still the most prominent.
    let base = if active {
        theme.tab_selected
    } else {
        theme.tab_inactive
    };
    let mut spans = Vec::new();
    match slot {
        PanelTab::ServerDetails => spans.push(Span::styled("Details", base)),
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

/// Append a fixed-anchor feed's recording (●) and paused (⏸) marks. The pause
/// mark tracks the explicit pause state, not a scrolled-up viewport.
fn push_feed_marks(spans: &mut Vec<Span<'static>>, sub: Option<&Subscription>, theme: &Theme) {
    if let Some(sub) = sub {
        if sub.recording.is_on() {
            spans.push(Span::styled(" ●", theme.error));
        }
        if sub.paused {
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
        // A paused feed is still connected but isn't tracking events, so it reads
        // as "paused" rather than "live" — they never show together.
        SubState::Active if sub.paused => Span::styled("paused", theme.accent),
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
    // A scrolled-up viewport (still live, events flowing into the buffer) is a
    // distinct state from an explicit pause: flag it without claiming "paused".
    if !sub.follow && !sub.paused {
        if !right.is_empty() {
            right.push(Span::styled(" · ", theme.dim));
        }
        right.push(Span::styled("↑ scrolled", theme.accent));
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

/// The Recordings tab body: a list of the JSONL files in the recordings
/// directory on the left, and a scrollable text viewer of the selected
/// recording on the right. Borderless panes split by a single vertical rule —
/// the home area's box is the only outer frame.
fn recordings_body(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    if app.recordings.is_empty() {
        let body = Paragraph::new(vec![
            Line::from(""),
            Line::styled("No recordings found.", theme.dim),
            Line::from(""),
            Line::from("Open a feed tab in the Browser, then 'r' to record — files land"),
            Line::from("in the recordings directory."),
        ])
        .alignment(Alignment::Center);
        frame.render_widget(body, area);
        return;
    }
    let [list_area, viewer_area] =
        Layout::horizontal([Constraint::Percentage(45), Constraint::Percentage(55)]).areas(area);
    recordings_list(frame, app, theme, list_area);
    recording_viewer(frame, app, theme, viewer_area);
}

/// The left pane: one row per recording. The name column flexes to the pane
/// width; the size and modified-time columns are fixed-width tails. Borderless.
fn recordings_list(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    // Inner width minus the highlight symbol (2); no border now.
    let inner_w = area.width.saturating_sub(2) as usize;
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
        .highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    frame.render_stateful_widget(list, area, &mut app.recordings_state);
}

/// The right pane: a metadata header plus every record of the selected
/// recording. A single left border rules it off from the list. Not a bounded
/// preview — the whole file is loaded, so the record count is exact (it shows
/// what fits the pane; selecting another recording re-targets the viewer).
fn recording_viewer(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(theme.border)
        .padding(Padding::horizontal(1));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some((name, view)) = &app.recording_view else {
        frame.render_widget(
            Paragraph::new(Line::styled("Select a recording to view it.", theme.dim))
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
    if let Some(source_type) = &view.source_type {
        meta.push_str(source_type);
    }
    if let Some(connection) = &view.connection {
        if !meta.is_empty() {
            meta.push_str(" · ");
        }
        meta.push_str(connection);
    }
    if !meta.is_empty() {
        lines.push(Line::styled(meta, theme.dim));
    }
    // Exact record count · size · modified time (never a "1000+" estimate).
    if let Some(file) = app.recordings.iter().find(|f| &f.name == name) {
        let n = view.records.len();
        let count = if n == 1 {
            "1 record".to_string()
        } else {
            format!("{n} records")
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

    if let Some(err) = &view.error {
        lines.push(Line::styled(format!("error: {err}"), theme.error));
    } else if view.records.is_empty() {
        lines.push(Line::styled("(empty recording)", theme.dim));
    }

    for rec in &view.records {
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

    // Clamp the scroll offset against the content height (a viewport-derived
    // render write, mirroring the Browser value pane); inner height is the
    // full pane since the divider is a side border, not a top/bottom one.
    let inner_h = inner.height as usize;
    let max_scroll = lines.len().saturating_sub(inner_h) as u16;
    app.recordings_scroll = app.recordings_scroll.min(max_scroll);
    frame.render_widget(
        Paragraph::new(lines).scroll((app.recordings_scroll, 0)),
        inner,
    );
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

/// The Server Details tab's content: the server statistics moved down from the
/// Server band (version, uptime, clients, memory, hit) on one line, the
/// deployment facts on the next, then — after a margin — two time-series graphs
/// (ops/sec and total keys) on the left and the connected-client list on the
/// right. The leftmost bottom-panel tab; only ever drawn for a broker with a
/// dashboard (Redis).
fn server_details_content(frame: &mut Frame, conn: &Connection, theme: &Theme, area: Rect) {
    let Some(stats) = conn.dashboard.stats.as_ref() else {
        frame.render_widget(
            Paragraph::new(Line::styled("Collecting server details…", theme.dim)),
            area,
        );
        return;
    };

    // A one-column inset on each side keeps the content off the panel border, so
    // the tab reads with a little margin rather than text flush to the frame.
    let area = Rect {
        x: area.x.saturating_add(1),
        width: area.width.saturating_sub(2),
        ..area
    };

    // Three fact lines (server metrics, deployment facts, then health), a blank
    // spacer for margin, then the body: graphs left, clients right.
    let [metrics_area, facts_area, health_area, _gap, body_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .areas(area);
    frame.render_widget(
        Paragraph::new(server_metrics_line(stats, theme)),
        metrics_area,
    );
    frame.render_widget(Paragraph::new(server_facts_line(stats, theme)), facts_area);
    frame.render_widget(
        Paragraph::new(server_health_line(stats, theme)),
        health_area,
    );

    // Carve a two-column gutter out of the graphs column so it doesn't butt
    // against the client list.
    let [graphs_full, clients_area] =
        Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)])
            .areas(body_area);
    let graphs_area = Rect {
        width: graphs_full.width.saturating_sub(2),
        ..graphs_full
    };
    server_graphs(frame, conn, theme, graphs_area);
    server_clients(
        frame,
        &stats.clients,
        conn.dashboard.details_scroll,
        theme,
        clients_area,
    );
}

/// The server statistics moved down from the Server band: version, uptime,
/// client count, memory use, and cache hit ratio. Values lead in the foreground;
/// their units and labels stay dim. Metrics absent from `INFO` are omitted.
fn server_metrics_line(stats: &ServerStats, theme: &Theme) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let sep = |spans: &mut Vec<Span<'static>>| {
        if !spans.is_empty() {
            spans.push(Span::styled(" · ", theme.dim));
        }
    };
    if let Some(v) = &stats.redis_version {
        spans.push(Span::raw(format!("v{v}")));
    }
    if let Some(up) = stats.uptime_seconds {
        sep(&mut spans);
        spans.push(Span::styled("up ", theme.dim));
        spans.push(Span::raw(human_duration(up)));
    }
    if let Some(c) = stats.connected_clients {
        sep(&mut spans);
        // Show the cap (`7/10000`) when `maxclients` is known, so a connection
        // count nearing the limit reads as such.
        let max = stats
            .raw
            .get("maxclients")
            .and_then(|v| v.parse::<u64>().ok());
        let value = match max {
            Some(m) if m > 0 => format!("{c}/{m}"),
            _ => c.to_string(),
        };
        spans.push(Span::raw(value));
        spans.push(Span::styled(" clients", theme.dim));
    }
    // Memory: used / maxmemory when a cap is set, else used · peak, else just used.
    if let Some(used) = stats.used_memory {
        sep(&mut spans);
        let value = if let Some(max) = stats.maxmemory.filter(|m| *m > 0) {
            format!("{} / {}", human_bytes(used), human_bytes(max))
        } else if let Some(peak) = stats.used_memory_peak.filter(|p| *p > 0) {
            format!("{} · peak {}", human_bytes(used), human_bytes(peak))
        } else {
            human_bytes(used)
        };
        spans.push(Span::styled("mem ", theme.dim));
        spans.push(Span::raw(value));
    }
    if let Some(hit) = stats.hit_ratio() {
        sep(&mut spans);
        spans.push(Span::styled("hit ", theme.dim));
        spans.push(Span::raw(format!("{:.1}%", hit * 100.0)));
    }
    if spans.is_empty() {
        spans.push(Span::styled("server metrics", theme.dim));
    }
    Line::from(spans)
}

/// A dim one-liner of server facts the metrics line above doesn't carry: the
/// deployment mode and role, a replication summary, the total key count, then the
/// lifetime command / expiry / eviction counters. Values lead in the foreground;
/// their units stay dim.
fn server_facts_line(stats: &ServerStats, theme: &Theme) -> Line<'static> {
    let raw = |k: &str| stats.raw.get(k).cloned();
    let num = |k: &str| stats.raw.get(k).and_then(|v| v.parse::<u64>().ok());
    let mut spans: Vec<Span<'static>> = Vec::new();
    let sep = |spans: &mut Vec<Span<'static>>| {
        if !spans.is_empty() {
            spans.push(Span::styled(" · ", theme.dim));
        }
    };
    if let Some(mode) = raw("redis_mode") {
        spans.push(Span::raw(mode));
    }
    let role = raw("role");
    if let Some(role) = &role {
        sep(&mut spans);
        spans.push(Span::raw(role.clone()));
    }
    // Replication summary: a master shows how many replicas are attached; a
    // replica shows whether its link to the master is up (the field is only
    // present in a replica's INFO).
    if role.as_deref() == Some("master") {
        if let Some(n) = num("connected_slaves").filter(|n| *n > 0) {
            sep(&mut spans);
            spans.push(Span::raw(n.to_string()));
            spans.push(Span::styled(" replicas", theme.dim));
        }
    } else if let Some(link) = raw("master_link_status") {
        sep(&mut spans);
        spans.push(Span::styled("link ", theme.dim));
        let style = if link == "up" {
            theme.success
        } else {
            theme.error
        };
        spans.push(Span::styled(link, style));
    }
    // Total keys across all DBs — carried here as a figure now that the Server
    // Details graphs no longer chart it.
    if !stats.db_keys.is_empty() {
        let keys: u64 = stats.db_keys.iter().map(|(_, n)| n).sum();
        sep(&mut spans);
        spans.push(Span::raw(human_count(keys)));
        spans.push(Span::styled(" keys", theme.dim));
    }
    let counter = |spans: &mut Vec<Span<'static>>, key: &str, unit: &'static str| {
        if let Some(n) = num(key) {
            sep(spans);
            spans.push(Span::raw(human_count(n)));
            spans.push(Span::styled(unit, theme.dim));
        }
    };
    counter(&mut spans, "total_commands_processed", " cmds");
    counter(&mut spans, "expired_keys", " expired");
    counter(&mut spans, "evicted_keys", " evicted");
    if spans.is_empty() {
        spans.push(Span::styled("server details", theme.dim));
    }
    Line::from(spans)
}

/// A health one-liner surfacing the signals a debug tab is really for: memory
/// fragmentation and eviction policy, the last persistence (RDB/AOF) save
/// status, and — only when non-zero — the unsaved-change, blocked-client, and
/// rejected-connection counters that flag trouble. A failed save status reads in
/// the error colour; a high fragmentation ratio in the warning colour.
fn server_health_line(stats: &ServerStats, theme: &Theme) -> Line<'static> {
    let raw = |k: &str| stats.raw.get(k).cloned();
    let num = |k: &str| stats.raw.get(k).and_then(|v| v.parse::<u64>().ok());
    let float = |k: &str| stats.raw.get(k).and_then(|v| v.parse::<f64>().ok());
    let mut spans: Vec<Span<'static>> = Vec::new();
    let sep = |spans: &mut Vec<Span<'static>>| {
        if !spans.is_empty() {
            spans.push(Span::styled(" · ", theme.dim));
        }
    };
    // Memory fragmentation: RSS vs allocated. Above ~1.5 it warns.
    if let Some(frag) = float("mem_fragmentation_ratio") {
        spans.push(Span::styled("frag ", theme.dim));
        let style = if frag >= 1.5 {
            theme.warning
        } else {
            Style::default()
        };
        spans.push(Span::styled(format!("{frag:.2}"), style));
    }
    // Eviction policy (e.g. noeviction / allkeys-lru) — explains evictions.
    if let Some(policy) = raw("maxmemory_policy") {
        sep(&mut spans);
        spans.push(Span::raw(policy));
    }
    // Persistence: the last RDB save and, when AOF is on, the last AOF write —
    // an `err` here means data isn't being persisted.
    let status = |spans: &mut Vec<Span<'static>>, label: &'static str, value: String| {
        sep(spans);
        spans.push(Span::styled(label, theme.dim));
        let style = if value == "ok" {
            Style::default()
        } else {
            theme.error
        };
        spans.push(Span::styled(value, style));
    };
    if let Some(s) = raw("rdb_last_bgsave_status") {
        status(&mut spans, "rdb ", s);
    }
    if num("aof_enabled") == Some(1) {
        if let Some(s) = raw("aof_last_write_status").or_else(|| raw("aof_last_bgrewrite_status")) {
            status(&mut spans, "aof ", s);
        }
    }
    // Alarm counters: only shown when non-zero, so a healthy server's line stays
    // quiet and a number here always means something to look at.
    if let Some(n) = num("rdb_changes_since_last_save").filter(|n| *n > 0) {
        sep(&mut spans);
        spans.push(Span::raw(human_count(n)));
        spans.push(Span::styled(" unsaved", theme.dim));
    }
    if let Some(n) = num("blocked_clients").filter(|n| *n > 0) {
        sep(&mut spans);
        spans.push(Span::raw(n.to_string()));
        spans.push(Span::styled(" blocked", theme.dim));
    }
    if let Some(n) = num("rejected_connections").filter(|n| *n > 0) {
        sep(&mut spans);
        spans.push(Span::styled(human_count(n), theme.warning));
        spans.push(Span::styled(" rejected", theme.dim));
    }
    if spans.is_empty() {
        spans.push(Span::styled("server health", theme.dim));
    }
    Line::from(spans)
}

/// The left column of the Server Details tab: an ops/sec graph stacked over a
/// network-throughput graph, each captioned with its current value.
fn server_graphs(frame: &mut Frame, conn: &Connection, theme: &Theme, area: Rect) {
    // The two graphs split the column evenly. Each carries its own caption, so
    // they read apart without a spacer row between them — a row the third
    // (health) fact line above now claims on a short terminal.
    let [ops_area, net_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Min(0)]).areas(area);

    let ops_now = conn
        .dashboard
        .stats
        .as_ref()
        .and_then(|s| s.instantaneous_ops_per_sec)
        .unwrap_or(0);
    let ops_peak = conn
        .dashboard
        .ops_history
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    server_graph(
        frame,
        theme,
        ops_area,
        "Ops/sec",
        &conn.dashboard.ops_history,
        format!("{ops_now} · peak {ops_peak}"),
    );

    // Network throughput: the read and write rates broken out in the caption
    // (INFO reports them in KB/s), over a sparkline of their bytes/sec sum.
    let stats = conn.dashboard.stats.as_ref();
    let in_bps = (stats
        .and_then(|s| s.instantaneous_input_kbps)
        .unwrap_or(0.0)
        * 1024.0) as u64;
    let out_bps = (stats
        .and_then(|s| s.instantaneous_output_kbps)
        .unwrap_or(0.0)
        * 1024.0) as u64;
    server_graph(
        frame,
        theme,
        net_area,
        "Net",
        &conn.dashboard.net_history,
        format!(
            "{}/s in · {}/s out",
            human_bytes(in_bps),
            human_bytes(out_bps)
        ),
    );
}

/// One captioned time-series graph: a `label` + current `value` line over a
/// sparkline of `history` (oldest left, newest right; only the rightmost samples
/// that fit the width are drawn). Falls back to a dim note before any data lands.
fn server_graph(
    frame: &mut Frame,
    theme: &Theme,
    area: Rect,
    label: &str,
    history: &VecDeque<u64>,
    value: String,
) {
    let [caption_area, bars_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);
    let caption = Line::from(vec![
        Span::styled(format!("{label} "), theme.dim),
        Span::styled(value, theme.accent),
    ]);
    frame.render_widget(Paragraph::new(caption), caption_area);

    if history.is_empty() || bars_area.height == 0 {
        frame.render_widget(
            Paragraph::new(Line::styled("collecting…", theme.dim)),
            bars_area,
        );
        return;
    }
    // Keep only the newest samples that fit the bar width, in chronological order.
    let width = (bars_area.width as usize).max(1);
    let data: Vec<u64> = history.iter().rev().take(width).rev().copied().collect();
    let spark = Sparkline::default()
        .direction(RenderDirection::LeftToRight)
        .style(theme.gauge)
        .data(&data);
    frame.render_widget(spark, bars_area);
}

/// Fixed widths for the client table's name, idle, and command columns; the
/// address column flexes to fill whatever the pane has left, since addresses
/// vary most in length and clients often set no name at all. The four columns
/// are separated by single spaces (three gaps).
const CLIENT_NAME_COL: usize = 6;
const CLIENT_IDLE_COL: usize = 4;
const CLIENT_CMD_COL: usize = 13;

/// The flexing address column's width for a client table `width` columns wide:
/// the pane minus the fixed columns and the three inter-column gaps, never below
/// room for the `Addr` header itself.
fn client_addr_col(width: usize) -> usize {
    width
        .saturating_sub(CLIENT_NAME_COL + CLIENT_IDLE_COL + CLIENT_CMD_COL + 3)
        .max(4)
}

/// The right column of the Server Details tab: the connected clients from
/// `CLIENT LIST` as a table (name, address, idle, last command) under a title and
/// a column header. The rows are vertically scrollable (the offset is `scroll`,
/// rows from the top, clamped here against the list height).
fn server_clients(
    frame: &mut Frame,
    clients: &[ClientInfo],
    scroll: u16,
    theme: &Theme,
    area: Rect,
) {
    let [title_area, columns_area, list_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .areas(area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Clients ", theme.heading),
            Span::styled(format!("({})", clients.len()), theme.dim),
        ])),
        title_area,
    );

    if clients.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::styled("no client list", theme.dim)),
            list_area,
        );
        return;
    }

    // The column header, aligned to the data rows below it via the same widths.
    let addr_w = client_addr_col(columns_area.width as usize);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(pad_end("Name", CLIENT_NAME_COL), theme.header),
            Span::raw(" "),
            Span::styled(pad_end("Addr", addr_w), theme.header),
            Span::raw(" "),
            Span::styled(pad_end("Idle", CLIENT_IDLE_COL), theme.header),
            Span::raw(" "),
            Span::styled("Cmd", theme.header),
        ])),
        columns_area,
    );

    // Window the list to the visible rows; an over-scroll rests at the bottom.
    let width = list_area.width as usize;
    let height = list_area.height as usize;
    let off = (scroll as usize).min(clients.len().saturating_sub(height));
    let lines: Vec<Line> = clients
        .iter()
        .skip(off)
        .take(height.max(1))
        .map(|c| client_line(c, width, theme))
        .collect();
    frame.render_widget(Paragraph::new(lines), list_area);
}

/// One client row aligned to [`server_clients`]'s columns: the client's name (an
/// em-dash when it set none), its address and dim idle time, then its last
/// command in the accent colour. A client that has run nothing yet reports
/// `cmd=NULL`, which — like an empty command — renders as an em-dash rather than
/// the literal word.
fn client_line(client: &ClientInfo, width: usize, theme: &Theme) -> Line<'static> {
    let addr_w = client_addr_col(width);
    let name = if client.name.is_empty() {
        "—"
    } else {
        client.name.as_str()
    };
    let cmd = if client.last_cmd.is_empty() || client.last_cmd.eq_ignore_ascii_case("null") {
        "—"
    } else {
        client.last_cmd.as_str()
    };
    let idle = short_duration(client.idle);
    Line::from(vec![
        Span::raw(pad_end(&truncate(name, CLIENT_NAME_COL), CLIENT_NAME_COL)),
        Span::raw(" "),
        Span::styled(pad_end(&truncate(&client.addr, addr_w), addr_w), theme.dim),
        Span::raw(" "),
        Span::styled(
            pad_end(&truncate(&idle, CLIENT_IDLE_COL), CLIENT_IDLE_COL),
            theme.dim,
        ),
        Span::raw(" "),
        Span::styled(truncate(cmd, CLIENT_CMD_COL), theme.accent),
    ])
}

/// A compact, single-unit duration for the client list: `45s`, `12m`, `3h`,
/// `2d` — the coarsest unit only, so it fits a narrow column.
fn short_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

/// A compact decimal count: `1234` → `1.2k`, `2_500_000` → `2.5M`. For the
/// Server Details lifetime counters, where the scale matters but exact digits
/// don't.
fn human_count(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else if n < 1_000_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    }
}

/// The add/edit-connection modal overlay.
pub fn conn_form(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(form) = app.form.as_ref() else {
        return;
    };
    let editing = form.editing.is_some();
    // Edit mode carries one extra hint row (delete), so it gets a touch more room.
    let rect = centered(area, 60, if editing { 17 } else { 16 });
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(if editing {
            " Edit connection "
        } else {
            " Add connection "
        })
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    // One text-field row, right-aligned label + the field's current value and a
    // cursor bar when focused.
    let field_row = |i: usize, label: &str| -> Line {
        let focused = form.focus == i;
        let cursor = if focused { "▏" } else { "" };
        let label_style = if focused { theme.accent } else { theme.dim };
        Line::from(vec![
            Span::styled(format!("{label:>9}: "), label_style),
            Span::raw(format!("{}{cursor}", form.fields[i])),
        ])
    };

    let kind = match form.kind {
        BrokerKind::Redis => "redis",
        BrokerKind::Amqp => "amqp",
        BrokerKind::Rabbitmq => "rabbitmq",
    };
    let kind_focused = form.focus == ConnForm::KIND_FOCUS;
    let kind_row = Line::from(vec![
        Span::styled(
            format!("{:>9}: ", "Kind"),
            if kind_focused {
                theme.accent
            } else {
                theme.dim
            },
        ),
        Span::raw(format!("[{kind}]")),
        Span::styled("  (←/→ cycles)", theme.dim),
    ]);
    let tls_focused = form.focus == ConnForm::TLS_FOCUS;
    let tls_row = Line::from(vec![
        Span::styled(
            format!("{:>9}: ", "TLS"),
            if tls_focused { theme.accent } else { theme.dim },
        ),
        Span::raw(if form.tls { "[x]" } else { "[ ]" }),
        Span::styled("  (←/→ toggles)", theme.dim),
    ]);

    // Kind sits directly under Name (it drives the other fields' defaults), then
    // the connection details, with TLS last — matching the ↑/↓ focus order in
    // `ConnForm::FOCUS_ORDER`. Slot 3 is shared: a Redis DB index or a RabbitMQ
    // vhost, relabelled to suit; AMQP is not database-scoped, so the row is
    // omitted entirely.
    let mut lines: Vec<Line> = vec![
        field_row(0, ConnForm::LABELS[0]), // Name
        kind_row,
        field_row(1, ConnForm::LABELS[1]), // Host
        field_row(2, ConnForm::LABELS[2]), // Port
    ];
    if ConnForm::slot3_shown(form.kind) {
        lines.push(field_row(
            ConnForm::SLOT3_FIELD,
            ConnForm::slot3_label(form.kind),
        ));
    }
    lines.push(field_row(4, ConnForm::LABELS[4])); // Username
    lines.push(field_row(5, ConnForm::LABELS[5])); // Password
    lines.push(tls_row);

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
    if form.confirm_delete {
        lines.push(Line::styled(
            "⚠ Press Ctrl-D again to delete this connection",
            theme.error,
        ));
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        if editing {
            "↑/↓ move · Enter save · Ctrl-D delete · Esc cancel"
        } else {
            "↑/↓ move · Enter save & connect · Esc cancel"
        },
        theme.dim,
    ));
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

/// The help overlay.
pub fn help(frame: &mut Frame, theme: &Theme, area: Rect) {
    let lines = vec![
        Line::styled("Navigation", theme.heading),
        Line::from("  ↑ ↓ move   Home/End top/bottom   mouse wheel moves"),
        Line::from("  Enter connect (Connections)   Esc step back / quit"),
        Line::from(""),
        Line::styled("Home (Connections / Recordings tabs)", theme.heading),
        Line::from("  Tab / Shift-Tab switch tabs   b jump to last-viewed browser"),
        Line::from("  Connections: Enter connect · a add · e edit/delete · x disconnect"),
        Line::from("  Recordings: ↑↓ select · r rename · dd delete"),
        Line::from(""),
        Line::styled("Browser — focus follows the pane", theme.heading),
        Line::from("  the focused pane has a highlighted border; the footer lists its keys"),
        Line::from("  Tab / Shift-Tab focus & cycle the bottom subpanels"),
        Line::from("  Ctrl-↑ focus keys · Ctrl-↓ focus bottom"),
        Line::from(""),
        Line::styled("Keys pane", theme.heading),
        Line::from("  / filter   o sort column   O direction"),
        Line::from("  keys nest into collapsible groups by each ':' (start folded)"),
        Line::from("  →/l collapse/expand group   z fold/unfold all"),
        Line::from("  keys auto-refresh"),
        Line::from(""),
        Line::styled("Bottom subpanel (Redis)", theme.heading),
        Line::from(
            "  tabs: Details (graphs+clients) · Console · Monitor · Keyspace · Pub/Sub · Tail",
        ),
        Line::from("  feed tab: follows live · p play/pause · r rec · x close (tails)"),
        Line::from("  Pub/Sub & Tail: type a spec, Enter subscribes/tails"),
        Line::from("  (empty Tail = selected key · a glob makes a pattern)"),
        Line::from("  Console: type a command, Enter runs · ↑↓ or Ctrl-P/N history"),
        Line::from("  Ctrl-L clear · writes/admin refused"),
        Line::from(""),
        Line::styled("AMQP browser", theme.heading),
        Line::from("  destinations are curated (AMQP 1.0 can't enumerate them)"),
        Line::from("  a add · x/d remove · Enter open (queue → peek, topic → tail) · t tail"),
        Line::from("  ↑↓ scroll an open message · Tail tab: type topic:/queue: then Enter"),
        Line::from("  peek mode (browse / skip / destructive) is set in : → Settings"),
        Line::from(""),
        Line::styled("General", theme.heading),
        Line::from("  : command palette   a add connection   ? toggle help"),
        Line::from("  Esc back   Ctrl-c quit   m toggle mouse (off = select/copy)"),
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

/// The command-palette overlay (opened with `:`): a small centred, bordered list
/// of commands with the highlighted one marked. Today it lists a single command.
pub fn command_palette(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(palette) = app.palette.as_ref() else {
        return;
    };
    let commands = PaletteCommand::all();
    let rect = centered(area, 40, commands.len() as u16 + 2);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Command palette ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let items: Vec<ListItem> = commands
        .iter()
        .map(|c| ListItem::new(Line::raw(c.label())))
        .collect();
    let list = List::new(items)
        .block(block)
        .highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    let mut state = ListState::default();
    state.select(Some(palette.selected.min(commands.len().saturating_sub(1))));
    frame.render_stateful_widget(list, rect, &mut state);
}

/// The settings-page overlay (reached from the command palette): a small centred
/// page of options, one per [`SettingsRow`]. Each shows its current value
/// bracketed by `‹ ›`; ↑/↓ move the highlight (marked with `▶`) between rows and
/// ←/→ cycle the highlighted row's value. Changes apply live, so the overlay
/// itself is repainted in the just-picked theme.
pub fn settings(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(settings) = app.settings.as_ref() else {
        return;
    };
    let rows = SettingsRow::all();
    let rect = centered(area, 50, rows.len() as u16 + 6);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Settings ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let theme_name = crate::theme::THEME_BASES[crate::theme::theme_base_index(app.theme_base())];
    let mut lines = vec![Line::from("")];
    for (i, row) in rows.iter().enumerate() {
        let value = match row {
            SettingsRow::Theme => theme_name.to_string(),
            SettingsRow::Animations => app.animation_speed().label().to_string(),
            SettingsRow::PeekMode => app.peek_mode().label().to_string(),
        };
        let selected = i == settings.selected.min(rows.len().saturating_sub(1));
        // The highlighted row gets the `▶` marker and the accent tint; the rest
        // sit dim, so the cursor row stands out without moving the columns.
        let (marker, label_style) = if selected {
            ("▶ ", theme.accent)
        } else {
            ("  ", theme.dim)
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{marker}{:<14}", row.label()), label_style),
            Span::styled("‹ ", theme.dim),
            Span::raw(value),
            Span::styled(" ›", theme.dim),
        ]));
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "  ↑/↓ row · ←/→ change · Enter/Esc close",
        theme.dim,
    ));
    frame.render_widget(Paragraph::new(lines), inner);
}

// -- helpers ----------------------------------------------------------------

/// The border style for a pane: the highlighted `border_focused` when it owns
/// the keyboard, the plain `border` otherwise. Keeps the focused-pane tint in
/// one place across the key list and the bottom subpanel.
fn border_style(theme: &Theme, focused: bool) -> Style {
    if focused {
        theme.border_focused
    } else {
        theme.border
    }
}

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

    #[test]
    fn human_count_scales_with_suffixes() {
        assert_eq!(human_count(0), "0");
        assert_eq!(human_count(999), "999");
        assert_eq!(human_count(1_500), "1.5k");
        assert_eq!(human_count(2_500_000), "2.5M");
        assert_eq!(human_count(3_200_000_000), "3.2B");
    }

    #[test]
    fn short_duration_picks_the_coarsest_single_unit() {
        assert_eq!(short_duration(0), "0s");
        assert_eq!(short_duration(59), "59s");
        assert_eq!(short_duration(60), "1m");
        assert_eq!(short_duration(3599), "59m");
        assert_eq!(short_duration(3600), "1h");
        assert_eq!(short_duration(86_400), "1d");
    }

    fn client(name: &str, addr: &str, idle: u64, cmd: &str) -> ClientInfo {
        ClientInfo {
            id: 1,
            name: name.into(),
            addr: addr.into(),
            age: 0,
            idle,
            last_cmd: cmd.into(),
        }
    }

    #[test]
    fn client_line_lays_out_name_address_idle_and_command() {
        let theme = Theme::dark();
        // A named, active client shows all four columns: name, address, compact
        // idle, and last command.
        let named = line_text(&client_line(
            &client("web-1", "10.0.0.2:5555", 75, "get"),
            44,
            &theme,
        ));
        assert!(named.contains("web-1"), "name shown: {named:?}");
        assert!(named.contains("10.0.0.2:5555"), "address shown: {named:?}");
        assert!(named.contains("1m"), "compact idle shown: {named:?}");
        assert!(named.contains("get"), "last command shown: {named:?}");
        // An unnamed client that has run nothing yet reports `cmd=NULL`: the name
        // and the command both collapse to an em-dash, but the address still
        // identifies it.
        let unnamed = line_text(&client_line(
            &client("", "10.0.0.9:6666", 0, "NULL"),
            44,
            &theme,
        ));
        assert!(
            unnamed.contains("10.0.0.9:6666"),
            "address identifies it: {unnamed:?}"
        );
        assert!(
            unnamed.matches('—').count() >= 2,
            "missing name and NULL command both em-dashed: {unnamed:?}"
        );
    }

    #[test]
    fn server_facts_line_lists_non_band_facts() {
        let theme = Theme::dark();
        let mut stats = ServerStats::default();
        for (k, v) in [
            ("redis_mode", "standalone"),
            ("role", "master"),
            ("total_commands_processed", "12345"),
            ("expired_keys", "7"),
            ("evicted_keys", "0"),
        ] {
            stats.raw.insert(k.into(), v.into());
        }
        let text = line_text(&server_facts_line(&stats, &theme));
        assert!(text.contains("standalone"), "mode: {text:?}");
        assert!(text.contains("master"), "role: {text:?}");
        assert!(text.contains("12.3k cmds"), "command counter: {text:?}");
        assert!(text.contains("7 expired"), "expiry counter: {text:?}");
        // With nothing known, it degrades to a placeholder rather than a blank.
        let empty = line_text(&server_facts_line(&ServerStats::default(), &theme));
        assert_eq!(empty, "server details");
    }

    #[test]
    fn server_facts_line_summarizes_replication_and_keys() {
        let theme = Theme::dark();
        // A master with replicas and keys across DBs: the replica count and the
        // summed key total both read.
        let mut master = ServerStats {
            db_keys: vec![(0, 42), (1, 7)],
            ..Default::default()
        };
        for (k, v) in [("role", "master"), ("connected_slaves", "2")] {
            master.raw.insert(k.into(), v.into());
        }
        let text = line_text(&server_facts_line(&master, &theme));
        assert!(text.contains("2 replicas"), "replica count: {text:?}");
        assert!(text.contains("49 keys"), "summed keys: {text:?}");

        // A replica reports its link status instead of a replica count.
        let mut replica = ServerStats::default();
        for (k, v) in [("role", "slave"), ("master_link_status", "down")] {
            replica.raw.insert(k.into(), v.into());
        }
        let text = line_text(&server_facts_line(&replica, &theme));
        assert!(text.contains("link down"), "link status: {text:?}");
        assert!(
            !text.contains("replicas"),
            "a replica shows no replica count: {text:?}"
        );
    }

    #[test]
    fn server_health_line_flags_problems_and_hides_quiet_counters() {
        let theme = Theme::dark();
        // A healthy server: fragmentation, policy and save status show; the alarm
        // counters are all zero, so they are omitted to keep the line quiet.
        let mut ok = ServerStats::default();
        for (k, v) in [
            ("mem_fragmentation_ratio", "1.05"),
            ("maxmemory_policy", "noeviction"),
            ("rdb_last_bgsave_status", "ok"),
            ("rdb_changes_since_last_save", "0"),
            ("blocked_clients", "0"),
            ("rejected_connections", "0"),
        ] {
            ok.raw.insert(k.into(), v.into());
        }
        let text = line_text(&server_health_line(&ok, &theme));
        assert!(text.contains("frag 1.05"), "fragmentation: {text:?}");
        assert!(text.contains("noeviction"), "policy: {text:?}");
        assert!(text.contains("rdb ok"), "save status: {text:?}");
        assert!(!text.contains("unsaved"), "zero counters hidden: {text:?}");
        assert!(!text.contains("blocked"), "zero counters hidden: {text:?}");
        assert!(!text.contains("rejected"), "zero counters hidden: {text:?}");

        // A struggling server surfaces AOF status and the non-zero counters.
        let mut bad = ServerStats::default();
        for (k, v) in [
            ("mem_fragmentation_ratio", "2.10"),
            ("rdb_last_bgsave_status", "err"),
            ("aof_enabled", "1"),
            ("aof_last_write_status", "err"),
            ("rdb_changes_since_last_save", "1280"),
            ("blocked_clients", "3"),
            ("rejected_connections", "5"),
        ] {
            bad.raw.insert(k.into(), v.into());
        }
        let text = line_text(&server_health_line(&bad, &theme));
        assert!(text.contains("rdb err"), "failed save: {text:?}");
        assert!(text.contains("aof err"), "failed aof: {text:?}");
        assert!(text.contains("1.3k unsaved"), "unsaved changes: {text:?}");
        assert!(text.contains("3 blocked"), "blocked clients: {text:?}");
        assert!(
            text.contains("5 rejected"),
            "rejected connections: {text:?}"
        );

        // With nothing known it degrades to a placeholder rather than a blank.
        let empty = line_text(&server_health_line(&ServerStats::default(), &theme));
        assert_eq!(empty, "server health");
    }
}
