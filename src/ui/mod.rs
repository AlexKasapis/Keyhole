//! Rendering. [`render`] is a pure function of [`App`] state, called once per
//! frame by the main loop: a header, the active screen, a footer (hints or the
//! active text-entry prompt), plus modal overlays (connection form, help).

mod theme;
mod views;

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{App, InputMode, Screen};
use theme::Theme;

/// Draw one frame from the current application state.
pub fn render(frame: &mut Frame, app: &mut App) {
    let theme = Theme::default();
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
        Screen::Connections => "  ↑↓ move · Enter connect · a add · ? help · q quit",
        Screen::Browser => "  ↑↓ keys · / filter · [ ] db · d dash · c conns · ? help · q quit",
        Screen::Dashboard => "  b browser · c conns · r refresh · ? help · q quit",
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
