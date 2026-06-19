//! UI-facing application state — the single source of truth that `ui::render`
//! reads each frame.

use time::OffsetDateTime;

pub struct AppState {
    /// Last observed time, refreshed on each tick and shown as the status clock.
    now: OffsetDateTime,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            now: OffsetDateTime::now_utc(),
        }
    }

    /// Advance time-based state on each tick.
    pub fn on_tick(&mut self) {
        self.now = OffsetDateTime::now_utc();
    }

    /// `HH:MM:SS UTC` for the status bar.
    pub fn clock(&self) -> String {
        format!(
            "{:02}:{:02}:{:02} UTC",
            self.now.hour(),
            self.now.minute(),
            self.now.second()
        )
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
