//! Terminal lifecycle: enter/leave raw mode + the alternate screen + mouse
//! capture, and install a panic hook so a panic can never leave the user's
//! terminal in a broken state.

use std::io::stdout;

use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use ratatui::DefaultTerminal;

/// The concrete terminal type used throughout the app.
pub type Tui = DefaultTerminal;

/// Enter raw mode + the alternate screen + mouse capture, returning a ready
/// terminal. Installs a panic hook first so even a panic during setup restores
/// the screen. Mouse capture lets the UI react to the scroll wheel (note that,
/// while captured, the terminal's own text selection is suppressed).
pub fn init() -> Tui {
    install_panic_hook();
    let terminal = ratatui::init();
    // Best-effort: a terminal without mouse support just yields no mouse events.
    // Capture can be toggled off at runtime (see `set_mouse_capture`) to hand the
    // terminal's own text selection back to the user.
    set_mouse_capture(true);
    terminal
}

/// Turn mouse capture on or off mid-session. Capture lets the UI react to the
/// scroll wheel but suppresses the terminal's own click-drag text selection;
/// turning it off hands selection (and copy) back to the user. Best-effort: a
/// terminal without mouse support simply ignores the request.
pub fn set_mouse_capture(enabled: bool) {
    if enabled {
        let _ = execute!(stdout(), EnableMouseCapture);
    } else {
        let _ = execute!(stdout(), DisableMouseCapture);
    }
}

/// Disable mouse capture, leave the alternate screen, and disable raw mode.
/// Idempotent and best-effort.
pub fn restore() {
    let _ = execute!(stdout(), DisableMouseCapture);
    ratatui::restore();
}

/// Chain a terminal-restoring step in front of the existing panic hook so the
/// panic message prints on a clean screen.
fn install_panic_hook() {
    let original = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore();
        original(info);
    }));
}
