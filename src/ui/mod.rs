//! Rendering. [`render`] is a pure function of [`App`] state, called once per
//! frame by the main loop. Phase 0 draws a status bar, a welcome body, and a
//! hints line; real views are layered in starting Phase 1.

mod theme;
mod views;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::Theme;

/// Draw one frame from the current application state.
pub fn render(frame: &mut Frame, app: &mut App) {
    let theme = Theme::default();
    let [status, body, hints] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    render_status_bar(frame, app, &theme, status);
    render_body(frame, &theme, body);
    render_hints(frame, &theme, hints);
}

fn render_status_bar(frame: &mut Frame, app: &App, theme: &Theme, area: Rect) {
    let [left, right] =
        Layout::horizontal([Constraint::Min(0), Constraint::Length(13)]).areas(area);

    let title = Line::from(vec![
        Span::styled(" BrokerTUI ", theme.title),
        Span::styled("  no connection", theme.dim),
    ]);
    frame.render_widget(Paragraph::new(title).style(theme.status_bar), left);
    frame.render_widget(
        Paragraph::new(Line::from(format!("{} ", app.state.clock())))
            .alignment(Alignment::Right)
            .style(theme.status_bar),
        right,
    );
}

fn render_body(frame: &mut Frame, theme: &Theme, area: Rect) {
    let lines = vec![
        Line::from(""),
        Line::styled("BrokerTUI", theme.heading),
        Line::from("Connect to Redis & AMQP · browse · watch · record"),
        Line::from(""),
        Line::styled(
            "Phase 0 scaffold — event loop, logging, terminal lifecycle.",
            theme.dim,
        ),
        Line::from(""),
        Line::from("Press q or Ctrl-C to quit."),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .block(Block::bordered().title(" Welcome ")),
        area,
    );
}

fn render_hints(frame: &mut Frame, theme: &Theme, area: Rect) {
    frame.render_widget(
        Paragraph::new(Line::from("  q quit   ·   ? help (coming soon)")).style(theme.hints),
        area,
    );
}
