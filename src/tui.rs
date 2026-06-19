//! Terminal lifecycle: enter/leave raw mode + the alternate screen, and install
//! a panic hook so a panic can never leave the user's terminal in a broken
//! state. Mouse capture is added in Phase 3.

use ratatui::DefaultTerminal;

/// The concrete terminal type used throughout the app.
pub type Tui = DefaultTerminal;

/// Enter raw mode + the alternate screen and return a ready terminal.
///
/// Installs a panic hook first so even a panic during setup restores the screen.
pub fn init() -> Tui {
    install_panic_hook();
    ratatui::init()
}

/// Leave the alternate screen and disable raw mode. Idempotent and best-effort.
pub fn restore() {
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
