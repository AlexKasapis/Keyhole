//! Small, self-contained UI animations driven by the tick clock (`App::now`,
//! advanced every ~250 ms). Two effects live here:
//!
//! * [`pulse`] — a gentle "breathing" brightness applied to a *connected*
//!   status dot, so a live connection reads as alive. It runs off the wall
//!   clock, so every connected dot across the UI breathes in unison.
//! * [`fade`] — a fade-out applied to a transient footer notification over the
//!   final stretch of its life, dissolving its colour toward the status bar's
//!   background. Notifications that don't self-dismiss never fade (their opacity
//!   stays `1.0`): a confirmation prompt, or a message replaced by a newer one,
//!   simply appears or disappears outright.

use ratatui::style::{Color, Modifier, Style};
use time::OffsetDateTime;

use crate::theme::Theme;

/// One full breath of the connected-dot pulse, in milliseconds. At the ~250 ms
/// tick this is ~8 frames per cycle — slow enough to read as breathing rather
/// than blinking.
const PULSE_PERIOD_MS: u64 = 2000;

/// Apply the connection "breathing" pulse to a status dot's base style. The
/// brightness eases up and down over [`PULSE_PERIOD_MS`] via the DIM/BOLD
/// modifiers, so the effect stays faithful to the terminal palette — it never
/// rewrites the dot's colour — and remains visible under `NO_COLOR`.
pub(crate) fn pulse(base: Style, now: OffsetDateTime) -> Style {
    match pulse_level(now) {
        Brightness::Dim => base.add_modifier(Modifier::DIM),
        Brightness::Normal => base,
        Brightness::Bold => base.add_modifier(Modifier::BOLD),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Brightness {
    Dim,
    Normal,
    Bold,
}

/// The pulse brightness for an instant: a triangle wave over the period, so the
/// dot steps dim → normal → bold → normal → dim across each breath rather than
/// snapping between the extremes.
fn pulse_level(now: OffsetDateTime) -> Brightness {
    let phase = cycle_phase(now, PULSE_PERIOD_MS);
    // Triangle: 0 at the period's ends, 1 at its midpoint.
    let tri = 1.0 - (2.0 * phase - 1.0).abs();
    if tri < 1.0 / 3.0 {
        Brightness::Dim
    } else if tri < 2.0 / 3.0 {
        Brightness::Normal
    } else {
        Brightness::Bold
    }
}

/// Position within a repeating cycle of `period_ms`, as a fraction in `[0, 1)`.
fn cycle_phase(now: OffsetDateTime, period_ms: u64) -> f32 {
    let total_ms = now.unix_timestamp_nanos() / 1_000_000;
    let pos = total_ms.rem_euclid(period_ms as i128) as f32;
    pos / period_ms as f32
}

/// Apply a fade-out to a notification's style. `alpha` is its opacity — `1.0`
/// fully visible, `0.0` gone; see [`crate::app::App::status_fade`]. With a
/// resolvable colour the foreground is interpolated toward the status bar's
/// background, so the message dissolves into the bar; with no colour to fade
/// (e.g. `NO_COLOR`, where the bar is reverse-video) it dims for the tail of
/// the fade instead, so something still happens as it leaves.
pub(crate) fn fade(base: Style, theme: &Theme, alpha: f32) -> Style {
    if alpha >= 1.0 {
        return base;
    }
    let alpha = alpha.clamp(0.0, 1.0);
    match (
        base.fg.and_then(resolve_rgb),
        theme.status_bar.bg.and_then(resolve_rgb),
    ) {
        // Dissolve the foreground toward the bar: alpha 1 keeps the colour,
        // alpha 0 reaches the background.
        (Some(fg), Some(bg)) => base.fg(lerp_rgb(bg, fg, alpha)),
        // No colour to interpolate: dim once the fade is past its first third.
        _ if alpha < 2.0 / 3.0 => base.add_modifier(Modifier::DIM),
        _ => base,
    }
}

/// Linear interpolation between two RGB colours; `t = 0` yields `a`, `t = 1`
/// yields `b`.
fn lerp_rgb(a: (u8, u8, u8), b: (u8, u8, u8), t: f32) -> Color {
    let mix = |x: u8, y: u8| (x as f32 + (y as f32 - x as f32) * t).round() as u8;
    Color::Rgb(mix(a.0, b.0), mix(a.1, b.1), mix(a.2, b.2))
}

/// Best-effort RGB for a colour so it can be interpolated. Handles truecolour,
/// the xterm 256-colour cube/grayscale, and the 16 ANSI names (mapped to the
/// common xterm defaults). `Reset` — a deliberate fall-back to the terminal's
/// default — yields `None`, signalling the caller to use a modifier-only effect.
fn resolve_rgb(color: Color) -> Option<(u8, u8, u8)> {
    Some(match color {
        Color::Rgb(r, g, b) => (r, g, b),
        Color::Indexed(i) => indexed_rgb(i),
        Color::Black => ANSI16[0],
        Color::Red => ANSI16[1],
        Color::Green => ANSI16[2],
        Color::Yellow => ANSI16[3],
        Color::Blue => ANSI16[4],
        Color::Magenta => ANSI16[5],
        Color::Cyan => ANSI16[6],
        Color::Gray => ANSI16[7],
        Color::DarkGray => ANSI16[8],
        Color::LightRed => ANSI16[9],
        Color::LightGreen => ANSI16[10],
        Color::LightYellow => ANSI16[11],
        Color::LightBlue => ANSI16[12],
        Color::LightMagenta => ANSI16[13],
        Color::LightCyan => ANSI16[14],
        Color::White => ANSI16[15],
        Color::Reset => return None,
    })
}

/// The 16 ANSI base colours in the common xterm palette. Indexed entries 0..=15
/// alias these, so both the named and indexed paths resolve through one table.
const ANSI16: [(u8, u8, u8); 16] = [
    (0, 0, 0),       // black
    (205, 0, 0),     // red
    (0, 205, 0),     // green
    (205, 205, 0),   // yellow
    (0, 0, 238),     // blue
    (205, 0, 205),   // magenta
    (0, 205, 205),   // cyan
    (229, 229, 229), // white (ANSI 7)
    (127, 127, 127), // bright black / dark gray
    (255, 0, 0),     // bright red
    (0, 255, 0),     // bright green
    (255, 255, 0),   // bright yellow
    (92, 92, 255),   // bright blue
    (255, 0, 255),   // bright magenta
    (0, 255, 255),   // bright cyan
    (255, 255, 255), // bright white
];

/// RGB for an xterm 256-colour index: the 16 base colours, the 6×6×6 colour
/// cube, then the 24-step grayscale ramp.
fn indexed_rgb(i: u8) -> (u8, u8, u8) {
    match i {
        0..=15 => ANSI16[i as usize],
        16..=231 => {
            let i = i - 16;
            let comp = |c: u8| if c == 0 { 0 } else { 55 + 40 * c };
            (comp(i / 36), comp((i / 6) % 6), comp(i % 6))
        }
        232..=255 => {
            let v = 8 + 10 * (i - 232);
            (v, v, v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An instant `ms` milliseconds past the Unix epoch.
    fn at(ms: i128) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(ms * 1_000_000).unwrap()
    }

    #[test]
    fn pulse_breathes_dim_normal_bold_over_the_cycle() {
        // Start of the cycle: darkest. Midpoint: brightest. Quarter points:
        // the normal mid-step. This is the triangle the eye reads as breathing.
        assert_eq!(pulse_level(at(0)), Brightness::Dim);
        assert_eq!(pulse_level(at(1000)), Brightness::Bold);
        assert_eq!(pulse_level(at(500)), Brightness::Normal);
        assert_eq!(pulse_level(at(1500)), Brightness::Normal);
        // It repeats every period, regardless of which cycle we're in.
        assert_eq!(pulse_level(at(PULSE_PERIOD_MS as i128)), Brightness::Dim);
        assert_eq!(
            pulse_level(at(PULSE_PERIOD_MS as i128 + 1000)),
            Brightness::Bold
        );
    }

    #[test]
    fn pulse_layers_modifiers_without_touching_colour() {
        // The pulse only ever adds DIM/BOLD; the dot keeps its palette colour so
        // a themed terminal's green is preserved.
        let base = Style::new().fg(Color::Green);
        assert_eq!(pulse(base, at(0)).fg, Some(Color::Green));
        assert!(pulse(base, at(0)).add_modifier.contains(Modifier::DIM));
        assert!(pulse(base, at(1000)).add_modifier.contains(Modifier::BOLD));
        assert_eq!(pulse(base, at(500)).add_modifier, Modifier::empty());
    }

    #[test]
    fn fade_is_a_no_op_at_full_opacity() {
        let base = Style::new().fg(Color::Green);
        let theme = Theme::dark();
        assert_eq!(fade(base, &theme, 1.0), base);
        // Above 1.0 is treated as fully opaque too.
        assert_eq!(fade(base, &theme, 1.5), base);
    }

    #[test]
    fn fade_dissolves_foreground_toward_the_status_bar_background() {
        let theme = Theme::dark();
        let bg = resolve_rgb(theme.status_bar.bg.unwrap()).unwrap();
        let base = Style::new().fg(Color::Green);
        // Fully transparent lands on the background colour …
        assert_eq!(
            fade(base, &theme, 0.0).fg,
            Some(Color::Rgb(bg.0, bg.1, bg.2))
        );
        // … and a mid value sits strictly between green and the background.
        let Some(Color::Rgb(_, g, _)) = fade(base, &theme, 0.5).fg else {
            panic!("mid-fade should interpolate to an RGB colour");
        };
        let green = resolve_rgb(Color::Green).unwrap().1;
        assert!(
            g > bg.1 && g < green,
            "green channel eases toward the background"
        );
    }

    #[test]
    fn fade_falls_back_to_dim_without_a_resolvable_colour() {
        // The colourless (NO_COLOR) palette has no foreground to interpolate, so
        // the tail of the fade dims instead of dissolving.
        let theme = Theme::plain();
        let base = theme.success;
        assert!(fade(base, &theme, 0.4).add_modifier.contains(Modifier::DIM));
        // Early in the fade (still mostly visible) it holds steady rather than
        // dimming on the very first frame.
        assert!(!fade(base, &theme, 0.9).add_modifier.contains(Modifier::DIM));
    }

    #[test]
    fn indexed_rgb_covers_cube_and_grayscale_anchors() {
        // Base colours alias the ANSI table.
        assert_eq!(indexed_rgb(2), (0, 205, 0));
        // Cube corners: index 16 is black, 231 is white.
        assert_eq!(indexed_rgb(16), (0, 0, 0));
        assert_eq!(indexed_rgb(231), (255, 255, 255));
        // Grayscale ramp endpoints.
        assert_eq!(indexed_rgb(232), (8, 8, 8));
        assert_eq!(indexed_rgb(255), (238, 238, 238));
    }
}
