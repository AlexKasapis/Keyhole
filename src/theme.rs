//! Named styles for the UI: one source of colour so widgets never hardcode
//! styling. A [`Theme`] is built once at startup from the `[theme]` config
//! section (a `dark`/`light` base plus optional per-style colour overrides) and
//! honours `NO_COLOR` by falling back to a colourless, modifier-only palette.

use std::str::FromStr;

use ratatui::style::{Color, Modifier, Style};

use crate::config::ThemeConfig;

/// The set of styles the UI draws with. `Copy` so the render loop can hand a
/// value to each view without lifetime entanglement with `&mut App`.
#[derive(Debug, Clone, Copy)]
pub struct Theme {
    pub status_bar: Style,
    pub heading: Style,
    pub dim: Style,
    /// Foreground for an inactive (unselected) tab in a tab strip. Brighter than
    /// [`Self::dim`] so unselected tabs stay legible even while their panel is
    /// unfocused, while the selected tab's highlight (see [`Self::selected`])
    /// stays the standout.
    pub tab_inactive: Style,
    pub selected: Style,
    pub header: Style,
    pub border: Style,
    pub border_focused: Style,
    pub accent: Style,
    pub error: Style,
    pub success: Style,
    /// A cautionary amber, used for transitional/at-risk states such as the
    /// Server band's "connecting" connection indicator.
    pub warning: Style,
    pub gauge: Style,
}

impl Default for Theme {
    fn default() -> Self {
        Self::dark()
    }
}

impl Theme {
    /// The default dark palette (cyan accents on a dark status bar).
    pub fn dark() -> Self {
        Self {
            status_bar: Style::new().bg(Color::Indexed(236)).fg(Color::Gray),
            heading: Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(Color::DarkGray),
            // A step brighter than `dim` (DarkGray) so unselected tabs read.
            tab_inactive: Style::new().fg(Color::Gray),
            // A background highlight (not reverse video) so per-span foreground
            // colours — e.g. a connected row's green status dot — survive on the
            // selected row and read the same whether selected or not.
            selected: Style::new()
                .bg(Color::Indexed(238))
                .add_modifier(Modifier::BOLD),
            header: Style::new()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD.union(Modifier::UNDERLINED)),
            border: Style::new().fg(Color::Indexed(240)),
            border_focused: Style::new().fg(Color::Cyan),
            accent: Style::new().fg(Color::Cyan),
            error: Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
            success: Style::new().fg(Color::Green),
            warning: Style::new().fg(Color::Yellow),
            gauge: Style::new().fg(Color::Cyan).bg(Color::Indexed(236)),
        }
    }

    /// A light palette for light terminal backgrounds (blue accents).
    pub fn light() -> Self {
        Self {
            status_bar: Style::new().bg(Color::Indexed(254)).fg(Color::Indexed(238)),
            heading: Style::new().fg(Color::Blue).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(Color::Indexed(245)),
            // Darker (higher contrast on a light bg) than `dim` so unselected
            // tabs read — the light-theme analogue of the dark palette's Gray.
            tab_inactive: Style::new().fg(Color::Indexed(238)),
            // Background highlight (see the dark palette) so foreground colours
            // survive on the selected row.
            selected: Style::new()
                .bg(Color::Indexed(252))
                .add_modifier(Modifier::BOLD),
            header: Style::new()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD.union(Modifier::UNDERLINED)),
            border: Style::new().fg(Color::Indexed(250)),
            border_focused: Style::new().fg(Color::Blue),
            accent: Style::new().fg(Color::Blue),
            error: Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
            success: Style::new().fg(Color::Green),
            warning: Style::new().fg(Color::Yellow),
            gauge: Style::new().fg(Color::Blue).bg(Color::Indexed(254)),
        }
    }

    /// A colourless palette for `NO_COLOR`: affordances survive via modifiers
    /// (reverse video, bold, underline) but nothing sets a foreground/background
    /// colour, so it reads correctly on any terminal theme.
    pub fn plain() -> Self {
        let none = Style::new();
        Self {
            status_bar: none.add_modifier(Modifier::REVERSED),
            heading: none.add_modifier(Modifier::BOLD),
            dim: none.add_modifier(Modifier::DIM),
            // No colour under NO_COLOR: an unselected tab is plain text, while the
            // selected tab's reverse video (see `selected`) does the disambiguating.
            tab_inactive: none,
            selected: none.add_modifier(Modifier::REVERSED.union(Modifier::BOLD)),
            header: none.add_modifier(Modifier::BOLD.union(Modifier::UNDERLINED)),
            border: none,
            border_focused: none.add_modifier(Modifier::BOLD),
            accent: none.add_modifier(Modifier::BOLD),
            error: none.add_modifier(Modifier::BOLD.union(Modifier::REVERSED)),
            success: none.add_modifier(Modifier::BOLD),
            warning: none.add_modifier(Modifier::BOLD),
            gauge: none.add_modifier(Modifier::REVERSED),
        }
    }

    /// Build the theme from config. `NO_COLOR` wins outright (colourless); else
    /// the `base` palette is loaded and any colour overrides applied on top.
    /// Unparseable colour strings are logged and ignored (base value kept).
    pub fn from_config(cfg: &ThemeConfig, no_color: bool) -> Self {
        if no_color {
            return Self::plain();
        }
        let mut theme = match cfg.base.as_deref() {
            Some(b) if b.eq_ignore_ascii_case("light") => Self::light(),
            _ => Self::dark(),
        };
        override_fg(&mut theme.accent, &cfg.accent);
        override_fg(&mut theme.heading, &cfg.heading);
        override_fg(&mut theme.error, &cfg.error);
        override_fg(&mut theme.success, &cfg.success);
        override_fg(&mut theme.border, &cfg.border);
        override_fg(&mut theme.border_focused, &cfg.border_focused);
        override_fg(&mut theme.gauge, &cfg.gauge);
        theme
    }

    /// Resolve the theme from config, consulting the process environment for
    /// `NO_COLOR` (present and non-empty disables colour).
    pub fn resolve(cfg: &ThemeConfig) -> Self {
        Self::from_config(cfg, no_color_env())
    }
}

/// `NO_COLOR` is honoured when set to a non-empty value (the widely-adopted
/// convention) — see <https://no-color.org/>.
fn no_color_env() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
}

/// Apply a colour override to a style's foreground, if the spec parses.
fn override_fg(style: &mut Style, spec: &Option<String>) {
    let Some(spec) = spec else { return };
    match Color::from_str(spec) {
        Ok(color) => *style = style.fg(color),
        Err(_) => tracing::warn!(color = %spec, "ignoring invalid theme colour"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_selects_light_or_dark() {
        let dark = Theme::from_config(&ThemeConfig::default(), false);
        assert_eq!(dark.accent.fg, Some(Color::Cyan));
        let light = Theme::from_config(
            &ThemeConfig {
                base: Some("light".into()),
                ..Default::default()
            },
            false,
        );
        assert_eq!(light.accent.fg, Some(Color::Blue));
        // Base is case-insensitive.
        let light2 = Theme::from_config(
            &ThemeConfig {
                base: Some("LIGHT".into()),
                ..Default::default()
            },
            false,
        );
        assert_eq!(light2.accent.fg, Some(Color::Blue));
    }

    #[test]
    fn warning_is_amber_with_colour_and_modifier_only_when_plain() {
        // The connection indicator's "connecting" state leans on this style, so
        // it must carry an actual colour in the coloured palettes …
        for t in [Theme::dark(), Theme::light()] {
            assert_eq!(t.warning.fg, Some(Color::Yellow));
        }
        // … and survive NO_COLOR as a modifier (no foreground colour) so the
        // accompanying label still does the disambiguating.
        let plain = Theme::plain();
        assert_eq!(plain.warning.fg, None);
        assert!(plain.warning.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn inactive_tab_foreground_is_distinct_from_dim() {
        // Unselected tabs use a readable foreground that is brighter/higher-
        // contrast than the muted `dim`, so they don't wash out — in both
        // coloured palettes the two differ.
        let dark = Theme::dark();
        assert_eq!(dark.tab_inactive.fg, Some(Color::Gray));
        assert_ne!(dark.tab_inactive.fg, dark.dim.fg);
        let light = Theme::light();
        assert_ne!(light.tab_inactive.fg, light.dim.fg);
        // NO_COLOR: no foreground at all; the selected tab's reverse video carries
        // the distinction instead.
        assert_eq!(Theme::plain().tab_inactive.fg, None);
    }

    #[test]
    fn no_color_forces_plain_palette() {
        let cfg = ThemeConfig {
            accent: Some("red".into()),
            ..Default::default()
        };
        let t = Theme::from_config(&cfg, true);
        assert_eq!(t.accent.fg, None, "NO_COLOR drops all foreground colours");
        assert!(t.selected.add_modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn coloured_selection_uses_background_not_reverse() {
        // With colour available the selection is a background highlight, so a
        // row's per-span foreground colours (e.g. the green status dot) are not
        // inverted when the row is selected — it looks the same either way.
        for t in [Theme::dark(), Theme::light()] {
            assert!(t.selected.bg.is_some(), "selection sets a background");
            assert!(
                !t.selected.add_modifier.contains(Modifier::REVERSED),
                "selection must not reverse foreground/background"
            );
            assert!(t.selected.add_modifier.contains(Modifier::BOLD));
        }
        // The colourless palette has no background to use, so it keeps reverse
        // video as the only available affordance.
        assert!(Theme::plain()
            .selected
            .add_modifier
            .contains(Modifier::REVERSED));
    }

    #[test]
    fn overrides_named_hex_and_indexed_colours() {
        let cfg = ThemeConfig {
            accent: Some("magenta".into()),
            heading: Some("#ff8800".into()),
            border: Some("240".into()),
            ..Default::default()
        };
        let t = Theme::from_config(&cfg, false);
        assert_eq!(t.accent.fg, Some(Color::Magenta));
        assert_eq!(t.heading.fg, Some(Color::Rgb(0xff, 0x88, 0x00)));
        assert_eq!(t.border.fg, Some(Color::Indexed(240)));
    }

    #[test]
    fn invalid_colour_is_ignored_keeping_base() {
        let cfg = ThemeConfig {
            accent: Some("not-a-colour".into()),
            ..Default::default()
        };
        let t = Theme::from_config(&cfg, false);
        assert_eq!(t.accent.fg, Some(Color::Cyan), "base colour is kept");
    }
}
