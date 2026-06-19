//! Per-screen rendering. Each function draws one screen (or overlay) from the
//! current [`App`] state into the given area.

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Gauge, List, ListItem, Paragraph, Row, Table, Wrap};
use ratatui::Frame;

use super::theme::Theme;
use crate::app::{App, ConnForm};
use crate::broker::{Ttl, ValueView};

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
            let dot = if app.is_connected(&p.name) {
                "●"
            } else {
                "○"
            };
            let db = if p.db > 0 {
                format!("/{}", p.db)
            } else {
                String::new()
            };
            let tls = if p.tls { " tls" } else { "" };
            ListItem::new(format!("{dot} {}   {}:{}{db}{tls}", p.name, p.host, p.port))
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
    let value = Paragraph::new(render_value(theme, conn.value.as_ref()))
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
            .ratio(mem_ratio.clamp(0.0, 1.0))
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
            .ratio(hit.clamp(0.0, 1.0))
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
    lines.push(Line::from(""));
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
    let rect = centered(area, 62, 20);
    frame.render_widget(Clear, rect);
    let block = Block::bordered()
        .title(" Help ")
        .title_style(theme.heading)
        .border_style(theme.border_focused);
    let lines = vec![
        Line::styled("Navigation", theme.heading),
        Line::from("  ↑/k ↓/j move   g/G top/bottom   Ctrl-u/d page"),
        Line::from("  Enter      connect (on Connections)"),
        Line::from(""),
        Line::styled("Screens", theme.heading),
        Line::from("  c connections    b browser    d dashboard"),
        Line::from(""),
        Line::styled("Browser", theme.heading),
        Line::from("  / filter    [ ] change DB    n load more    r refresh"),
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

fn opt_num(v: Option<u64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "?".into())
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
