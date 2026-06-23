//! `App` read-only command console: input, scrollback, and execution.
//! Part of the `app` module (overview in `app.rs`).

use super::*;

impl App {
    // -- command console -----------------------------------------------------

    pub(super) fn active_console_mut(&mut self) -> Option<&mut Console> {
        self.active_conn_mut().map(|c| &mut c.console)
    }

    pub(super) fn clear_console(&mut self) {
        if let Some(console) = self.active_console_mut() {
            console.entries.clear();
            console.scroll = 0;
        }
        self.set_status("console cleared".to_string(), false);
    }

    pub(super) fn scroll_console(&mut self, delta: i32) {
        if let Some(console) = self.active_console_mut() {
            // Up (delta < 0) scrolls back through output (larger offset from the
            // bottom); the upper bound is clamped against total lines at render.
            let next = console.scroll as i32 - delta;
            console.scroll = next.clamp(0, u16::MAX as i32) as u16;
        }
    }

    /// The console prompt while it is focused (bottom subpanel, Console tab).
    /// Focus moves (Tab / Ctrl-↑↓ / Esc) are handled in `handle_browser_key`
    /// before this runs; ↑/↓ (and Ctrl-P/N) recall history since the key list is
    /// no longer driven from here.
    pub(super) fn handle_command_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Up => {
                if let Some(console) = self.active_console_mut() {
                    console.recall_prev();
                }
            }
            KeyCode::Down => {
                if let Some(console) = self.active_console_mut() {
                    console.recall_next();
                }
            }
            KeyCode::Char('p') if ctrl => {
                if let Some(console) = self.active_console_mut() {
                    console.recall_prev();
                }
            }
            KeyCode::Char('n') if ctrl => {
                if let Some(console) = self.active_console_mut() {
                    console.recall_next();
                }
            }
            KeyCode::Enter => self.submit_command(),
            // The output band scrolls with PageUp/PageDown.
            KeyCode::PageUp => self.scroll_console(-CONSOLE_SCROLL_STEP),
            KeyCode::PageDown => self.scroll_console(CONSOLE_SCROLL_STEP),
            // Ctrl-L clears the console (the standalone screen's `r`, now gone).
            KeyCode::Char('l') if ctrl => self.clear_console(),
            KeyCode::Char(c) => {
                if let Some(console) = self.active_console_mut() {
                    console.input.push(c);
                    console.history_pos = None;
                }
            }
            KeyCode::Backspace => {
                if let Some(console) = self.active_console_mut() {
                    console.input.pop();
                }
            }
            _ => {}
        }
    }

    /// Submit the typed command for read-only execution. Stays in command mode
    /// (console-style) so commands can be issued back to back; `Esc` leaves.
    pub(super) fn submit_command(&mut self) {
        let Some(id) = self.active_id() else {
            self.mode = InputMode::Normal;
            return;
        };
        let command = self
            .active_console_mut()
            .map(|c| c.input.trim().to_string())
            .unwrap_or_default();
        if command.is_empty() {
            return;
        }
        if let Some(conn) = self.conn_by_id_mut(id) {
            conn.console.remember(&command);
            conn.console.input.clear();
            conn.console.pending = Some(command.clone());
            conn.console.scroll = 0;
            conn.handle.send(ConnCommand::Exec { command });
        }
    }
}
