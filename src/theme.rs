//! Named styles for the UI: one source of colour so widgets never hardcode
//! styling. A [`Theme`] is built once at startup from the `[theme]` config
//! section (a base palette — `gruvbox` (the default), `dark`, or `light` — plus
//! optional per-style colour overrides) and honours `NO_COLOR` by falling back
//! to a colourless, modifier-only palette.

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
    /// unfocused, while the selected tab (see [`Self::tab_selected`]) stays the
    /// standout.
    pub tab_inactive: Style,
    /// The selected tab in a tab strip: the selection highlight plus an *explicit*
    /// bright foreground, so the label stays the most prominent one beside the
    /// (now brighter) inactive tabs and regardless of panel focus. Distinct from
    /// [`Self::selected`], which omits a foreground so table rows keep their
    /// per-span colours — a tab label has no such colours to preserve.
    pub tab_selected: Style,
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
        Self::gruvbox()
    }
}

impl Theme {
    /// The original dark palette (cyan accents on a dark status bar). Selectable
    /// as `base = "dark"`; the default is [`Self::gruvbox`].
    pub fn dark() -> Self {
        Self {
            status_bar: Style::new().bg(Color::Indexed(236)).fg(Color::Gray),
            heading: Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(Color::DarkGray),
            // A step brighter than `dim` (DarkGray) so unselected tabs read.
            tab_inactive: Style::new().fg(Color::Gray),
            // The selection highlight plus a bright white foreground (brighter
            // than the inactive Gray) so the selected tab is unmistakably the
            // standout — the bare highlight let the inactive tabs out-shine it.
            tab_selected: Style::new()
                .bg(Color::Indexed(238))
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
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
            // Selection highlight plus a near-black foreground so the selected tab
            // is the most prominent label on a light background (the dark-theme
            // bright-white analogue).
            tab_selected: Style::new()
                .bg(Color::Indexed(252))
                .fg(Color::Indexed(232))
                .add_modifier(Modifier::BOLD),
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

    /// A warm "Gruvbox"-flavoured dark palette: orange accents and a yellow
    /// heading over a soft brown-black base, with the retro green/red signal
    /// colours. The default palette — applied when no `base` is configured — and
    /// a coloured dark theme beside [`Self::dark`], selectable from the settings
    /// page.
    pub fn gruvbox() -> Self {
        // Gruvbox tones, by their canonical hex (see https://github.com/morhetz/gruvbox).
        let bg_status = Color::Rgb(0x3c, 0x38, 0x36); // bg1
        let bg_sel = Color::Rgb(0x50, 0x49, 0x45); // bg2
        let fg_status = Color::Rgb(0xa8, 0x99, 0x84); // fg4 (gray-4)
        let fg_bright = Color::Rgb(0xfb, 0xf1, 0xc7); // fg0 (brightest)
        let gray = Color::Rgb(0x92, 0x83, 0x74); // gray
        let orange = Color::Rgb(0xfe, 0x80, 0x19); // bright orange
        let yellow = Color::Rgb(0xfa, 0xbd, 0x2f); // bright yellow
        let green = Color::Rgb(0xb8, 0xbb, 0x26); // bright green
        let red = Color::Rgb(0xfb, 0x49, 0x34); // bright red
        let border = Color::Rgb(0x66, 0x5c, 0x54); // bg3
        Self {
            status_bar: Style::new().bg(bg_status).fg(fg_status),
            heading: Style::new().fg(yellow).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(gray),
            // A step brighter than `dim` (gray) so unselected tabs read.
            tab_inactive: Style::new().fg(fg_status),
            // The selection highlight plus the brightest foreground so the
            // selected tab stays the standout beside the brighter inactive tabs.
            tab_selected: Style::new()
                .bg(bg_sel)
                .fg(fg_bright)
                .add_modifier(Modifier::BOLD),
            // A background highlight (not reverse video) so per-span foreground
            // colours survive on the selected row — see the dark palette.
            selected: Style::new().bg(bg_sel).add_modifier(Modifier::BOLD),
            header: Style::new()
                .fg(yellow)
                .add_modifier(Modifier::BOLD.union(Modifier::UNDERLINED)),
            border: Style::new().fg(border),
            border_focused: Style::new().fg(orange),
            accent: Style::new().fg(orange),
            error: Style::new().fg(red).add_modifier(Modifier::BOLD),
            success: Style::new().fg(green),
            warning: Style::new().fg(yellow),
            gauge: Style::new().fg(orange).bg(bg_status),
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
            // selected tab's reverse video (see `tab_selected`) does the disambiguating.
            tab_inactive: none,
            tab_selected: none.add_modifier(Modifier::REVERSED.union(Modifier::BOLD)),
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
            Some(b) if b.eq_ignore_ascii_case("dark") => Self::dark(),
            Some(b) if b.eq_ignore_ascii_case("light") => Self::light(),
            Some(b) if b.eq_ignore_ascii_case("gruvbox") => Self::gruvbox(),
            // Absent or unrecognised base falls back to the default, gruvbox.
            _ => Self::gruvbox(),
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

/// The built-in theme bases the settings page cycles through, in order. Each
/// name is matched (case-insensitively) by [`Theme::from_config`]; the first is
/// the default applied for an absent or unrecognised `base`.
pub const THEME_BASES: [&str; 3] = ["gruvbox", "dark", "light"];

/// The index of `base` within [`THEME_BASES`], defaulting to `0` (the first,
/// `gruvbox`) for an absent or unrecognised name — mirroring the fallback in
/// [`Theme::from_config`]. Lets the settings page show and step the selection.
pub fn theme_base_index(base: Option<&str>) -> usize {
    base.and_then(|b| {
        THEME_BASES
            .iter()
            .position(|name| name.eq_ignore_ascii_case(b))
    })
    .unwrap_or(0)
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
    fn base_selects_palette_and_defaults_to_gruvbox() {
        // An absent base resolves to the default palette — gruvbox (orange accent).
        let default = Theme::from_config(&ThemeConfig::default(), false);
        assert_eq!(default.accent.fg, Some(Color::Rgb(0xfe, 0x80, 0x19)));
        // An explicit `dark` still selects the dark palette (cyan): it has its own
        // match arm and must not fall through to the gruvbox default.
        let dark = Theme::from_config(
            &ThemeConfig {
                base: Some("dark".into()),
                ..Default::default()
            },
            false,
        );
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
    fn selected_tab_sets_an_explicit_foreground_unlike_row_selection() {
        // The tab selection sets its own bright foreground (brighter than the
        // inactive-tab colour) so the selected tab stays the standout, whereas
        // the row-selection style omits a foreground so a selected row keeps its
        // per-span colours.
        let dark = Theme::dark();
        assert_eq!(dark.tab_selected.fg, Some(Color::White));
        assert_ne!(dark.tab_selected.fg, dark.tab_inactive.fg);
        assert_eq!(dark.selected.fg, None, "row selection keeps no foreground");
        // Both tab styles still carry the selection background highlight.
        assert_eq!(dark.tab_selected.bg, dark.selected.bg);
        let light = Theme::light();
        assert!(light.tab_selected.fg.is_some());
        assert_ne!(light.tab_selected.fg, light.tab_inactive.fg);
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
            // Pin a non-default base so the kept colour is the dark palette's cyan,
            // independent of whichever palette is the default.
            base: Some("dark".into()),
            accent: Some("not-a-colour".into()),
            ..Default::default()
        };
        let t = Theme::from_config(&cfg, false);
        assert_eq!(t.accent.fg, Some(Color::Cyan), "base colour is kept");
    }

    #[test]
    fn gruvbox_base_selects_the_warm_palette() {
        // "gruvbox" is its own coloured base, distinct from dark/light: an orange
        // accent (neither cyan nor blue) and the warm yellow heading.
        let t = Theme::from_config(
            &ThemeConfig {
                base: Some("gruvbox".into()),
                ..Default::default()
            },
            false,
        );
        assert_eq!(t.accent.fg, Some(Color::Rgb(0xfe, 0x80, 0x19)));
        assert_eq!(t.heading.fg, Some(Color::Rgb(0xfa, 0xbd, 0x2f)));
        assert_ne!(t.accent.fg, Theme::dark().accent.fg);
        assert_ne!(t.accent.fg, Theme::light().accent.fg);
        // The base is case-insensitive, like dark/light.
        let upper = Theme::from_config(
            &ThemeConfig {
                base: Some("GRUVBOX".into()),
                ..Default::default()
            },
            false,
        );
        assert_eq!(upper.accent.fg, t.accent.fg);
        // Per-style colour overrides still apply on top of the gruvbox base.
        let overridden = Theme::from_config(
            &ThemeConfig {
                base: Some("gruvbox".into()),
                accent: Some("magenta".into()),
                ..Default::default()
            },
            false,
        );
        assert_eq!(overridden.accent.fg, Some(Color::Magenta));
    }

    #[test]
    fn theme_base_index_finds_the_cycle_position_or_defaults_to_gruvbox() {
        // Each known base resolves to its slot in the cycle order …
        assert_eq!(THEME_BASES, ["gruvbox", "dark", "light"]);
        assert_eq!(theme_base_index(Some("gruvbox")), 0);
        assert_eq!(theme_base_index(Some("dark")), 1);
        assert_eq!(theme_base_index(Some("light")), 2);
        // … case-insensitively …
        assert_eq!(theme_base_index(Some("GruvBox")), 0);
        // … while an absent or unknown base falls back to gruvbox (index 0),
        // matching `from_config`'s own fallback.
        assert_eq!(theme_base_index(None), 0);
        assert_eq!(theme_base_index(Some("solarized")), 0);
    }
}
