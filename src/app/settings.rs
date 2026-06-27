//! The command palette and settings overlays: opening and dismissing them,
//! palette navigation + command execution, and the settings page's live theme
//! switch (applied immediately and persisted best-effort to the config file).
//! Part of the `app` module (overview in `app.rs`).

use super::*;
use crate::theme::{self, Theme};

impl App {
    // -- command palette -----------------------------------------------------

    /// Open the command palette (a small list of commands, opened with `:`).
    /// Dismisses the help overlay if it was up, so the two never stack.
    pub(super) fn open_palette(&mut self) {
        self.show_help = false;
        self.palette = Some(PaletteState::default());
    }

    /// Keys while the command palette is open. It owns the keyboard wholesale:
    /// navigate the command list, Enter runs the highlighted command, Esc closes
    /// it, and Ctrl-C still quits.
    pub(super) fn handle_palette_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (true, KeyCode::Char('c')) => self.running = false,
            (false, KeyCode::Esc) => self.palette = None,
            (false, KeyCode::Down) => self.move_palette(1),
            (false, KeyCode::Up) => self.move_palette(-1),
            (false, KeyCode::Enter) => self.run_palette_selection(),
            _ => {}
        }
    }

    /// Move the palette's highlight by `delta`, clamped to the command list.
    fn move_palette(&mut self, delta: i32) {
        if let Some(palette) = &mut self.palette {
            let len = PaletteCommand::all().len();
            palette.selected = move_selection(Some(palette.selected), len, delta).unwrap_or(0);
        }
    }

    /// Run the highlighted palette command and close the palette.
    fn run_palette_selection(&mut self) {
        let Some(selected) = self.palette.as_ref().map(|p| p.selected) else {
            return;
        };
        let commands = PaletteCommand::all();
        // `selected` is kept in range by `move_palette`, but clamp defensively so
        // a future longer list can never index out of bounds here.
        match commands[selected.min(commands.len() - 1)] {
            PaletteCommand::OpenSettings => {
                self.palette = None;
                self.settings = Some(SettingsState::default());
            }
        }
    }

    // -- settings page -------------------------------------------------------

    /// Keys while the settings page is open. It owns the keyboard wholesale:
    /// ↑/↓ move between rows, ←/→ cycle the highlighted row's value (applied
    /// live), Enter/Esc close, and Ctrl-C quits.
    pub(super) fn handle_settings_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match (ctrl, key.code) {
            (true, KeyCode::Char('c')) => self.running = false,
            (false, KeyCode::Esc | KeyCode::Enter) => self.settings = None,
            (false, KeyCode::Up) => self.move_settings(-1),
            (false, KeyCode::Down) => self.move_settings(1),
            (false, KeyCode::Left | KeyCode::Char('h')) => self.cycle_setting(-1),
            (false, KeyCode::Right | KeyCode::Char('l')) => self.cycle_setting(1),
            _ => {}
        }
    }

    /// Move the settings highlight by `delta`, clamped to the row list.
    fn move_settings(&mut self, delta: i32) {
        if let Some(settings) = &mut self.settings {
            let len = SettingsRow::all().len();
            settings.selected = move_selection(Some(settings.selected), len, delta).unwrap_or(0);
        }
    }

    /// Cycle the highlighted settings row's value by `delta` (←/→). Dispatches to
    /// the per-row cycler; each applies live and persists best-effort.
    fn cycle_setting(&mut self, delta: i32) {
        let Some(selected) = self.settings.as_ref().map(|s| s.selected) else {
            return;
        };
        let rows = SettingsRow::all();
        // `selected` is kept in range by `move_settings`, but clamp defensively
        // so a future longer list can never index out of bounds here.
        match rows[selected.min(rows.len() - 1)] {
            SettingsRow::Theme => self.cycle_theme(delta),
            SettingsRow::Animations => self.cycle_animation(delta),
            SettingsRow::PeekMode => self.cycle_peek_mode(delta),
        }
    }

    /// Step the active theme base through [`theme::THEME_BASES`] by `delta`
    /// (wrapping), apply the new palette to the live UI at once, and persist the
    /// choice to the config file. Persisting is best-effort: a write failure is
    /// surfaced as a footer status but the in-memory theme still changes, so the
    /// switch is never silently lost on screen.
    pub(super) fn cycle_theme(&mut self, delta: i32) {
        let names = theme::THEME_BASES;
        let cur = theme::theme_base_index(self.theme_base()) as i32;
        let next = (cur + delta).rem_euclid(names.len() as i32) as usize;
        let name = names[next];
        self.config.theme.base = Some(name.to_string());
        // Re-resolve from config so per-style overrides and `NO_COLOR` are honoured
        // exactly as they are at startup.
        self.theme = Theme::resolve(&self.config.theme);
        match config::save(&self.config_path, &self.config) {
            Ok(()) => self.set_status(format!("Theme: {name}"), false),
            Err(e) => self.set_status(
                format!("theme set to {name}, but could not save: {e}"),
                true,
            ),
        }
    }

    /// Step the UI animation setting through [`config::AnimationSpeed::ALL`] by
    /// `delta` (wrapping, so it toggles on/off). The new setting takes effect on
    /// the next tick — the dot's breath and the notification fade both read it
    /// live — and is persisted to the config file. Like [`Self::cycle_theme`],
    /// persisting is best-effort: a write failure is surfaced as a footer status
    /// but the in-memory setting still changes, so the switch is never silently
    /// lost.
    pub(super) fn cycle_animation(&mut self, delta: i32) {
        let all = config::AnimationSpeed::ALL;
        let cur = all
            .iter()
            .position(|&s| s == self.animation_speed())
            .unwrap_or_default() as i32;
        let next = (cur + delta).rem_euclid(all.len() as i32) as usize;
        let speed = all[next];
        self.config.settings.animation = speed;
        let label = speed.label();
        match config::save(&self.config_path, &self.config) {
            Ok(()) => self.set_status(format!("Animations: {label}"), false),
            Err(e) => self.set_status(
                format!("animations set to {label}, but could not save: {e}"),
                true,
            ),
        }
    }

    /// Step the AMQP queue-peek mode through [`config::PeekMode::ALL`] by `delta`
    /// (wrapping). Takes effect on the next peek and is persisted to the config
    /// file. Like the other cyclers, persisting is best-effort: a write failure is
    /// surfaced as a footer status but the in-memory setting still changes.
    pub(super) fn cycle_peek_mode(&mut self, delta: i32) {
        let all = config::PeekMode::ALL;
        let cur = all
            .iter()
            .position(|&m| m == self.peek_mode())
            .unwrap_or_default() as i32;
        let next = (cur + delta).rem_euclid(all.len() as i32) as usize;
        let mode = all[next];
        self.config.settings.peek_mode = mode;
        let label = mode.label();
        match config::save(&self.config_path, &self.config) {
            Ok(()) => self.set_status(format!("AMQP peek mode: {label}"), false),
            Err(e) => self.set_status(
                format!("AMQP peek mode set to {label}, but could not save: {e}"),
                true,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::event::AppEvent;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    /// A fresh app over a unique (initially absent) config file, so theme
    /// persistence can be asserted by reading the file back.
    fn app_with_config_path() -> (App, PathBuf) {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("keyhole-settings-{}-{n}.toml", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let (tx, _rx) = mpsc::channel::<AppEvent>(16);
        let app = App::new(
            Config::default(),
            path.clone(),
            std::env::temp_dir(),
            tx,
            TaskTracker::new(),
            CancellationToken::new(),
            None,
        );
        (app, path)
    }

    #[test]
    fn colon_opens_palette_and_it_captures_input() {
        let (mut app, _) = app_with_config_path();
        // `:` from the home screen opens the palette.
        app.handle_key(key(KeyCode::Char(':')));
        assert!(app.palette.is_some(), "':' opens the command palette");
        // While the palette is up it captures input: a Down moves its highlight
        // (clamped to the single command) rather than navigating the home list.
        let before = app.profile_state.selected();
        app.handle_key(key(KeyCode::Down));
        assert_eq!(
            app.profile_state.selected(),
            before,
            "input is captured by the palette, not the underlying screen"
        );
        // Esc closes it.
        app.handle_key(key(KeyCode::Esc));
        assert!(app.palette.is_none(), "Esc closes the palette");
    }

    #[test]
    fn palette_enter_opens_settings_and_closes_palette() {
        let (mut app, _) = app_with_config_path();
        app.open_palette();
        app.handle_key(key(KeyCode::Enter));
        assert!(
            app.palette.is_none(),
            "running a command closes the palette"
        );
        assert!(
            app.settings.is_some(),
            "Settings page command opens settings"
        );
    }

    #[test]
    fn settings_cycles_theme_live_and_persists() {
        let (mut app, path) = app_with_config_path();
        // Default base is unset (gruvbox). Right steps gruvbox -> dark -> light -> gruvbox.
        app.settings = Some(SettingsState::default());
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.theme_base(), Some("dark"));
        assert_eq!(app.theme.accent.fg, Theme::dark().accent.fg, "applied live");

        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.theme_base(), Some("light"));
        assert_eq!(app.theme.accent.fg, Theme::light().accent.fg);

        // The choice is persisted to the config file as it changes.
        let saved = crate::config::load(&path).expect("config reloads");
        assert_eq!(saved.theme.base.as_deref(), Some("light"));

        // Wrapping forward returns to the default gruvbox; Left wraps the other way.
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.theme_base(), Some("gruvbox"));
        assert_eq!(app.theme.accent.fg, Theme::gruvbox().accent.fg);
        app.handle_key(key(KeyCode::Left));
        assert_eq!(app.theme_base(), Some("light"), "Left wraps backwards");

        // Esc closes the settings overlay.
        app.handle_key(key(KeyCode::Esc));
        assert!(app.settings.is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn settings_navigates_rows_and_cycles_animation_live_and_persists() {
        let (mut app, path) = app_with_config_path();
        app.settings = Some(SettingsState::default());
        // Default config: animation on, with the highlight on the first row.
        assert_eq!(app.animation_speed(), crate::config::AnimationSpeed::On);
        assert_eq!(app.settings.unwrap().selected, 0);

        // Right on the Theme row cycles the theme; the animation is untouched.
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.theme_base(), Some("dark"));
        assert_eq!(
            app.animation_speed(),
            crate::config::AnimationSpeed::On,
            "the animation row is not cycled while Theme is highlighted"
        );

        // Down moves the highlight to the Animations row …
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.settings.unwrap().selected, 1);
        // … where ←/→ now toggle it, applied live: on -> off.
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.animation_speed(), crate::config::AnimationSpeed::Off);
        assert_eq!(
            app.theme_base(),
            Some("dark"),
            "the theme is left as it was"
        );

        // The choice is persisted to the config file as it changes.
        let saved = crate::config::load(&path).expect("config reloads");
        assert_eq!(saved.settings.animation, crate::config::AnimationSpeed::Off);

        // Left wraps the toggle backwards: off -> on.
        app.handle_key(key(KeyCode::Left));
        assert_eq!(app.animation_speed(), crate::config::AnimationSpeed::On);

        // ↓ moves to the third row (AMQP peek mode); a further ↓ clamps there
        // rather than wrapping.
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.settings.unwrap().selected, 2);
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.settings.unwrap().selected, 2, "clamped at the last row");
        // ↑ returns to the Animations row.
        app.handle_key(key(KeyCode::Up));
        assert_eq!(app.settings.unwrap().selected, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn settings_cycles_peek_mode_live_and_persists() {
        let (mut app, path) = app_with_config_path();
        app.settings = Some(SettingsState::default());
        // Default peek mode is the non-destructive browse.
        assert_eq!(app.peek_mode(), crate::config::PeekMode::Browse);

        // Navigate to the third row (AMQP peek mode).
        app.handle_key(key(KeyCode::Down));
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.settings.unwrap().selected, 2);

        // ←/→ cycle it, applied live: browse -> skip -> destructive.
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.peek_mode(), crate::config::PeekMode::Skip);
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.peek_mode(), crate::config::PeekMode::Destructive);

        // The choice is persisted to the config file as it changes.
        let saved = crate::config::load(&path).expect("config reloads");
        assert_eq!(
            saved.settings.peek_mode,
            crate::config::PeekMode::Destructive
        );

        // Wrapping forward returns to browse; Left wraps the other way.
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.peek_mode(), crate::config::PeekMode::Browse);
        app.handle_key(key(KeyCode::Left));
        assert_eq!(app.peek_mode(), crate::config::PeekMode::Destructive);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn settings_overlay_takes_priority_over_palette_in_dispatch() {
        // Both flags set (defensive): the settings handler wins, so its keys
        // (←/→) are interpreted as theme cycling, not palette navigation.
        let (mut app, _) = app_with_config_path();
        app.palette = Some(PaletteState::default());
        app.settings = Some(SettingsState::default());
        app.handle_key(key(KeyCode::Right));
        assert_eq!(app.theme_base(), Some("dark"));
    }

    #[test]
    fn open_overlay_swallows_mouse_scroll() {
        // An open overlay owns input, so the scroll wheel must not move the
        // selection on the screen beneath it.
        use crossterm::event::MouseEventKind;
        let (mut app, _) = app_with_config_path();
        let before = app.profile_state.selected();
        app.palette = Some(PaletteState::default());
        app.handle_mouse(MouseEventKind::ScrollDown);
        assert_eq!(
            app.profile_state.selected(),
            before,
            "palette swallows scroll"
        );
        app.palette = None;
        app.settings = Some(SettingsState::default());
        app.handle_mouse(MouseEventKind::ScrollDown);
        assert_eq!(
            app.profile_state.selected(),
            before,
            "settings swallows scroll"
        );
    }
}
