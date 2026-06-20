//! Per-screen rendering. Each function draws one screen (or overlay) from the
//! current [`App`] state into the given area.

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Gauge, List, ListItem, Paragraph, Row, Table, Tabs, Wrap};
use ratatui::Frame;
use time::OffsetDateTime;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::app::{App, ConnForm, RecordState, SubState};
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

    let items: Vec<ListItem> = app
        .profiles
        .iter()
        .map(|p| {
            let dot = if app.is_connected(p.name()) {
                "●"
            } else {
                "○"
            };
            ListItem::new(Line::from(vec![
                Span::raw(format!("{dot} {}  ", p.name())),
                Span::styled(format!("[{}] ", p.kind_label()), theme.dim),
                Span::raw(p.endpoint()),
            ]))
        })
        .collect();

    let list = List::new(items)
        .block(block)
        .highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    frame.render_stateful_widget(list, area, &mut app.profile_state);
}

/// Browser screen: key table + value pane for the active connection.
pub fn browser(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    let Some(conn) = app.active_conn_mut() else {
        let body = Paragraph::new("No active connection. Press 'c', select a profile, and Enter.")
            .style(theme.dim)
            .block(Block::bordered().border_style(theme.border));
        frame.render_widget(body, area);
        return;
    };

    let [info_area, body_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);

    let scanning = if conn.complete { "" } else { " · scanning…" };
    let info = Line::from(vec![
        Span::styled(format!(" db{} ", conn.db), theme.accent),
        Span::styled(format!(" match={} ", conn.pattern), theme.dim),
        Span::styled(format!(" {} keys{scanning} ", conn.keys.len()), theme.dim),
    ]);
    frame.render_widget(Paragraph::new(info).style(theme.status_bar), info_area);

    let [table_area, value_area] =
        Layout::horizontal([Constraint::Percentage(48), Constraint::Percentage(52)])
            .areas(body_area);

    let rows: Vec<Row> = conn
        .keys
        .iter()
        .map(|e| {
            let ttl = match e.ttl {
                Ttl::NoExpire => "—".to_string(),
                Ttl::Seconds(s) => human_duration(s.max(0) as u64),
                Ttl::Unknown => "?".to_string(),
            };
            Row::new([e.key.clone(), e.vtype.label().to_string(), ttl])
        })
        .collect();
    let widths = [
        Constraint::Percentage(64),
        Constraint::Length(7),
        Constraint::Length(10),
    ];
    let table = Table::new(rows, widths)
        .header(Row::new(["Key", "Type", "TTL"]).style(theme.header))
        .block(
            Block::bordered()
                .title(" Keys ")
                .title_style(theme.heading)
                .border_style(theme.border),
        )
        .row_highlight_style(theme.selected)
        .highlight_symbol("▶ ");
    frame.render_stateful_widget(table, table_area, &mut conn.table);

    let title = match &conn.value_key {
        Some(k) => format!(" {k} "),
        None => " Value ".to_string(),
    };
    let value_lines = render_value(theme, conn.value.as_ref());
    // Clamp the scroll offset so paging can't run off the end of the value. The
    // bound uses logical line count (wrapping may split lines further, as the
    // console's scroll does too); inner height excludes the two border rows.
    let inner_h = value_area.height.saturating_sub(2) as usize;
    let max_scroll = value_lines.len().saturating_sub(inner_h) as u16;
    conn.value_scroll = conn.value_scroll.min(max_scroll);
    let value = Paragraph::new(value_lines)
        .block(
            Block::bordered()
                .title(title)
                .title_style(theme.heading)
                .border_style(theme.border),
        )
        .wrap(Wrap { trim: false })
        .scroll((conn.value_scroll, 0));
    frame.render_widget(value, value_area);
}

/// Dashboard screen: server stats for the active connection.
pub fn dashboard(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(conn) = app.active_conn() else {
        frame.render_widget(
            Paragraph::new("No active connection.").style(theme.dim),
            area,
        );
        return;
    };
    let block = Block::bordered()
        .title(format!(" Dashboard — {} ", conn.name))
        .title_style(theme.heading)
        .border_style(theme.border);

    let Some(stats) = conn.stats.as_ref() else {
        frame.render_widget(
            Paragraph::new("Loading server stats…")
                .style(theme.dim)
                .block(block),
            area,
        );
        return;
    };

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [gauges, metrics] =
        Layout::vertical([Constraint::Length(3), Constraint::Min(0)]).areas(inner);
    let [g1, g2] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(gauges);

    let used = stats.used_memory.unwrap_or(0);
    let (mem_ratio, mem_label) = if let Some(max) = stats.maxmemory.filter(|m| *m > 0) {
        (
            used as f64 / max as f64,
            format!("{} / {}", human_bytes(used), human_bytes(max)),
        )
    } else if let Some(peak) = stats.used_memory_peak.filter(|p| *p > 0) {
        (
            used as f64 / peak as f64,
            format!("{} (peak {})", human_bytes(used), human_bytes(peak)),
        )
    } else {
        (0.0, human_bytes(used))
    };
    frame.render_widget(
        Gauge::default()
            .block(Block::bordered().title("Memory").border_style(theme.border))
            .gauge_style(theme.gauge)
            .ratio(gauge_ratio(mem_ratio))
            .label(mem_label),
        g1,
    );

    let hit = stats.hit_ratio().unwrap_or(0.0);
    frame.render_widget(
        Gauge::default()
            .block(
                Block::bordered()
                    .title("Hit ratio")
                    .border_style(theme.border),
            )
            .gauge_style(theme.gauge)
            .ratio(gauge_ratio(hit))
            .label(format!("{:.1}%", hit * 100.0)),
        g2,
    );

    let mut lines: Vec<Line> = Vec::new();
    {
        let mut row = |k: &str, v: String| {
            lines.push(Line::from(vec![
                Span::styled(format!("{k:<16}"), theme.accent),
                Span::raw(v),
            ]));
        };
        row(
            "Version",
            stats.redis_version.clone().unwrap_or_else(|| "?".into()),
        );
        row(
            "Uptime",
            stats
                .uptime_seconds
                .map(human_duration)
                .unwrap_or_else(|| "?".into()),
        );
        row("Clients", opt_num(stats.connected_clients));
        row("Ops/sec", opt_num(stats.instantaneous_ops_per_sec));
        row("Memory used", human_bytes(used));
        row(
            "Memory peak",
            stats
                .used_memory_peak
                .map(human_bytes)
                .unwrap_or_else(|| "?".into()),
        );
        row(
            "Hits / Misses",
            format!(
                "{} / {}",
                opt_num(stats.keyspace_hits),
                opt_num(stats.keyspace_misses)
            ),
        );
        let dbs = stats
            .db_keys
            .iter()
            .map(|(db, n)| format!("db{db}={n}"))
            .collect::<Vec<_>>()
            .join("  ");
        row(
            "Keys per DB",
            if dbs.is_empty() {
                "(empty)".into()
            } else {
                dbs
            },
        );
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), metrics);
}

/// Realtime screen: live tail tabs + the focused tail's scrollback ring buffer.
pub fn realtime(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(conn) = app.active_conn() else {
        frame.render_widget(
            Paragraph::new("No active connection. Connect first, then press 's' to subscribe.")
                .style(theme.dim)
                .block(Block::bordered().border_style(theme.border)),
            area,
        );
        return;
    };
    if conn.subs.is_empty() {
        let body = Paragraph::new(vec![
            Line::from(""),
            Line::styled("No live tails.", theme.dim),
            Line::from(""),
            Line::from("Press 's' to subscribe (pubsub:ch · psub:ch.* · stream:key),"),
            Line::from("or 't' on a stream key in the Browser."),
        ])
        .alignment(Alignment::Center)
        .block(
            Block::bordered()
                .title(" Realtime ")
                .title_style(theme.heading)
                .border_style(theme.border),
        );
        frame.render_widget(body, area);
        return;
    }

    let [tabs_area, status_area, body_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .areas(area);

    let titles: Vec<Line> = conn
        .subs
        .iter()
        .map(|s| {
            let mut spans = vec![Span::raw(s.label.clone())];
            if s.recording.is_on() {
                spans.push(Span::styled(" ●", theme.error));
            }
            match &s.state {
                SubState::Connecting => spans.push(Span::styled(" …", theme.dim)),
                SubState::Ended(_) => spans.push(Span::styled(" ✗", theme.dim)),
                SubState::Active => {}
            }
            Line::from(spans)
        })
        .collect();
    let tabs = Tabs::new(titles)
        .select(conn.active_sub.unwrap_or(0))
        .style(theme.dim)
        .highlight_style(theme.selected)
        .divider("│");
    frame.render_widget(tabs, tabs_area);

    let Some(sub) = conn.active_subscription() else {
        return;
    };

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
    let mut status = vec![
        Span::raw(" "),
        state_span,
        Span::styled(format!("  {} events", sub.received), theme.dim),
    ];
    if sub.received as usize > sub.events.len() {
        status.push(Span::styled(
            format!(" (last {})", sub.events.len()),
            theme.dim,
        ));
    }
    if let RecordState::On { records, bytes, .. } = &sub.recording {
        status.push(Span::styled(
            format!("  ● REC {records} ({})", human_bytes(*bytes)),
            theme.error,
        ));
    }
    if !sub.follow {
        status.push(Span::styled("  ⏸ paused (G to follow)", theme.accent));
    }
    frame.render_widget(
        Paragraph::new(Line::from(status)).style(theme.status_bar),
        status_area,
    );

    let block = Block::bordered()
        .title(format!(" {} ", sub.label))
        .title_style(theme.heading)
        .border_style(theme.border);
    let inner = block.inner(body_area);
    frame.render_widget(block, body_area);

    // A keyspace/advisory notice takes a wrapped banner above the events.
    let events_area = match &sub.notice {
        Some(notice) => {
            let [banner, rest] =
                Layout::vertical([Constraint::Length(2), Constraint::Min(0)]).areas(inner);
            frame.render_widget(
                Paragraph::new(Line::styled(format!("⚠ {notice}"), theme.error))
                    .wrap(Wrap { trim: true }),
                banner,
            );
            rest
        }
        None => inner,
    };

    let height = events_area.height as usize;
    let len = sub.events.len();
    if len == 0 || height == 0 {
        frame.render_widget(
            Paragraph::new(Line::styled("  waiting for events…", theme.dim)),
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

/// Recordings screen: JSONL files in the recordings directory.
pub fn recordings(frame: &mut Frame, app: &mut App, theme: &Theme, area: Rect) {
    let block = Block::bordered()
        .title(" Recordings ")
        .title_style(theme.heading)
        .border_style(theme.border);
    if app.recordings.is_empty() {
        let body = Paragraph::new(vec![
            Line::from(""),
            Line::styled("No recordings found.", theme.dim),
            Line::from(""),
            Line::from("Start a tail with 's', then 'r' to record — files land in the"),
            Line::from("recordings directory. Press 'r' here to rescan."),
        ])
        .alignment(Alignment::Center)
        .block(block);
        frame.render_widget(body, area);
        return;
    }
    let items: Vec<ListItem> = app
        .recordings
        .iter()
        .map(|f| {
            let when = f
                .modified
                .map(fmt_datetime)
                .unwrap_or_else(|| "?".to_string());
            ListItem::new(Line::from(vec![
                Span::raw(pad_end(&truncate(&f.name, 46), 46)),
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

/// Console screen: read-only command output for the active connection, with an
/// input prompt pinned to the bottom.
pub fn console(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(conn) = app.active_conn() else {
        frame.render_widget(
            Paragraph::new("No active connection. Connect first, then press 'e' for the console.")
                .style(theme.dim)
                .block(Block::bordered().border_style(theme.border)),
            area,
        );
        return;
    };
    let console = &conn.console;

    let block = Block::bordered()
        .title(format!(" Console — {} (read-only) ", conn.name))
        .title_style(theme.heading)
        .border_style(theme.border);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [output_area, prompt_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(inner);

    // Flatten the executed commands + replies into display lines.
    let mut lines: Vec<Line> = Vec::new();
    if console.entries.is_empty() {
        lines.push(Line::styled("Read-only command console.", theme.dim));
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "Press 'i', type a command, Enter to run. Writes and admin commands are refused.",
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
        let cursor = if app.mode == crate::app::InputMode::Command {
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

/// The command-palette overlay: a filtered, selectable action list.
pub fn palette(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let Some(state) = app.palette.as_ref() else {
        return;
    };
    let labels = app.palette_labels();
    let rect = centered(area, 56, 16);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Command palette ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let [query_area, list_area] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(inner);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("› ", theme.accent),
            Span::raw(format!("{}▏", state.query)),
        ])),
        query_area,
    );

    if labels.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::styled("  no matching commands", theme.dim)),
            list_area,
        );
        return;
    }
    let selected = state.selected.min(labels.len() - 1);
    let items: Vec<ListItem> = labels
        .iter()
        .enumerate()
        .map(|(i, label)| {
            if i == selected {
                ListItem::new(Line::styled(format!("▶ {label}"), theme.selected))
            } else {
                ListItem::new(Line::from(format!("  {label}")))
            }
        })
        .collect();
    frame.render_widget(List::new(items), list_area);
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
    for (i, label) in ConnForm::LABELS.iter().enumerate() {
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
        Span::styled("  (space switches redis/amqp)", theme.dim),
    ]));
    lines.push(Line::from(""));
    if matches!(form.kind, BrokerKind::Amqp) {
        lines.push(Line::styled(
            "AMQP: DB is ignored; port is 5672 (amqp) or 5671 (amqps/TLS).",
            theme.dim,
        ));
    }
    lines.push(Line::styled(
        "Password: env:VAR · keyring · prompt · or a literal (session only)",
        theme.dim,
    ));
    if let Some(err) = &form.error {
        lines.push(Line::styled(format!("⚠ {err}"), theme.error));
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "Tab/↑↓ move · Enter save & connect · Esc cancel",
        theme.dim,
    ));
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

/// The help overlay.
pub fn help(frame: &mut Frame, theme: &Theme, area: Rect) {
    let rect = centered(area, 66, 30);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Help ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let lines = vec![
        Line::styled("Navigation", theme.heading),
        Line::from("  ↑/k ↓/j move   g/G top/bottom   Ctrl-u/d page   mouse wheel scrolls"),
        Line::from("  Enter connect (Connections)    : command palette"),
        Line::from(""),
        Line::styled("Screens", theme.heading),
        Line::from("  c connections  b browser  d dashboard  w realtime  R recordings  e console"),
        Line::from(""),
        Line::styled("Browser", theme.heading),
        Line::from("  / filter   [ ] change DB   n load more   r refresh"),
        Line::from("  PgUp/PgDn (or Ctrl-u/d) scroll the value pane"),
        Line::from("  t tail selected stream    s subscribe (pub/sub or stream)"),
        Line::from(""),
        Line::styled("Realtime tails", theme.heading),
        Line::from("  s subscribe   m MONITOR   K keyspace   Tab/[ ] switch tab   x stop"),
        Line::from("  ↑↓ scroll   G follow newest   r toggle recording"),
        Line::from("  spec: pubsub:ch · psub:ch.* · stream:key · keyspace[:N] · monitor"),
        Line::from(""),
        Line::styled("Console (read-only)", theme.heading),
        Line::from("  i type a command   Enter run   ↑↓ history / scroll   r clear"),
        Line::from("  writes and admin commands are refused"),
        Line::from(""),
        Line::styled("General", theme.heading),
        Line::from("  a add connection    ? toggle help    q / Ctrl-c quit"),
    ];
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

fn opt_num(v: Option<u64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "?".into())
}

/// Clamp a gauge ratio into `[0, 1]`, mapping non-finite values (NaN/∞) to 0.
/// `f64::clamp` passes NaN through unchanged, which would trip `Gauge::ratio`'s
/// internal `0.0..=1.0` assertion and panic the dashboard.
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
    fn opt_num_renders_placeholder_for_none() {
        assert_eq!(opt_num(Some(42)), "42");
        assert_eq!(opt_num(None), "?");
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
