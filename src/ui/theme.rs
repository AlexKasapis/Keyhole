//! Named styles for the UI. A single source of colour so widgets never hardcode
//! styling. Made user-themeable from config in Phase 3.

use ratatui::style::{Color, Modifier, Style};

/// The set of styles the UI draws with.
pub struct Theme {
    pub status_bar: Style,
    pub title: Style,
    pub heading: Style,
    pub dim: Style,
    pub selected: Style,
    pub header: Style,
    pub border: Style,
    pub border_focused: Style,
    pub accent: Style,
    pub error: Style,
    pub success: Style,
    pub gauge: Style,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            status_bar: Style::new().bg(Color::Indexed(236)).fg(Color::Gray),
            title: Style::new()
                .bg(Color::Cyan)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
            heading: Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(Color::DarkGray),
            selected: Style::new().add_modifier(Modifier::REVERSED.union(Modifier::BOLD)),
            header: Style::new()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD.union(Modifier::UNDERLINED)),
            border: Style::new().fg(Color::Indexed(240)),
            border_focused: Style::new().fg(Color::Cyan),
            accent: Style::new().fg(Color::Cyan),
            error: Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
            success: Style::new().fg(Color::Green),
            gauge: Style::new().fg(Color::Cyan).bg(Color::Indexed(236)),
        }
    }
}
