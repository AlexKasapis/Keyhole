//! Named styles for the UI. A single source of color so widgets never hardcode
//! styling. Loaded from config and made user-themeable in Phase 3.

use ratatui::style::{Color, Modifier, Style};

/// The set of styles the UI draws with.
pub struct Theme {
    pub status_bar: Style,
    pub title: Style,
    pub heading: Style,
    pub dim: Style,
    pub hints: Style,
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
            hints: Style::new().fg(Color::DarkGray),
        }
    }
}
