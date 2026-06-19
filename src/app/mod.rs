//! Application state machine: owns [`AppState`] and turns [`AppEvent`]s into
//! state changes. The render loop is the sole owner of this type, so no locking
//! is needed for UI state.

mod action;
mod command;
mod state;

pub use state::AppState;

use crossterm::event::{Event, KeyEvent, KeyEventKind};

use crate::app::action::Action;
use crate::event::AppEvent;

/// The whole application, as seen by the render loop.
pub struct App {
    /// Cleared to stop the main loop.
    pub running: bool,
    /// UI-facing state (single source of truth for rendering).
    pub state: AppState,
}

impl App {
    pub fn new() -> Self {
        Self {
            running: true,
            state: AppState::new(),
        }
    }

    /// Apply a single event to the application state.
    pub fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::Input(Event::Key(key)) => self.handle_key(key),
            AppEvent::Input(_) => {}
            AppEvent::Tick => self.state.on_tick(),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        // Ignore key-release/repeat events (Windows emits these).
        if key.kind != KeyEventKind::Press {
            return;
        }
        if let Some(action) = action::map_key(&key) {
            self.apply(action);
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Quit => self.running = false,
        }
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}
