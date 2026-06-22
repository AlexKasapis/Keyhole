//! `App` recordings tab: scan the recordings dir, load the selected recording
//! into the viewer, and rename / delete recordings. Part of the `app` module
//! (overview in `app.rs`).

use super::*;

impl App {
    // -- recordings ----------------------------------------------------------

    pub(super) fn scan_recordings(&mut self) {
        let dir = self.recordings_dir.clone();
        let mut files = Vec::new();
        if let Ok(read) = std::fs::read_dir(&dir) {
            for entry in read.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                let meta = entry.metadata().ok();
                let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
                let modified = meta
                    .as_ref()
                    .and_then(|m| m.modified().ok())
                    .map(OffsetDateTime::from);
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?")
                    .to_string();
                files.push(RecordingFile {
                    name,
                    size,
                    modified,
                });
            }
        }
        // Newest first.
        files.sort_by_key(|f| std::cmp::Reverse(f.modified));
        self.recordings = files;
        let sel = match self.recordings.len() {
            0 => None,
            len => Some(self.recordings_state.selected().unwrap_or(0).min(len - 1)),
        };
        self.recordings_state.select(sel);
        self.load_recording_view();
    }

    /// (Re)load the whole selected recording into [`Self::recording_view`]. A
    /// no-op when the cached view is already for the selected file, so it is
    /// cheap to call after every selection change; an actual reload reads the
    /// entire file (so counts are exact and the viewer scrolls every record)
    /// and resets the viewer scroll to the top.
    pub(super) fn load_recording_view(&mut self) {
        let name = self
            .recordings_state
            .selected()
            .and_then(|i| self.recordings.get(i))
            .map(|f| f.name.clone());
        let Some(name) = name else {
            self.recording_view = None;
            return;
        };
        if self
            .recording_view
            .as_ref()
            .is_some_and(|(cached, _)| cached == &name)
        {
            return;
        }
        let path = self.recordings_dir.join(&name);
        let view = match std::fs::File::open(&path) {
            Ok(file) => recording::view(std::io::BufReader::new(file)),
            Err(e) => RecordingView {
                error: Some(e.to_string()),
                ..Default::default()
            },
        };
        self.recording_view = Some((name, view));
        // A different recording starts at the top of the viewer.
        self.recordings_scroll = 0;
    }

    /// The file name of the currently-selected recording, if any.
    fn selected_recording_name(&self) -> Option<String> {
        self.recordings_state
            .selected()
            .and_then(|i| self.recordings.get(i))
            .map(|f| f.name.clone())
    }

    /// Begin renaming the selected recording: prime the rename buffer with its
    /// current name and enter [`InputMode::Rename`]. A no-op when nothing is
    /// selected.
    pub(super) fn start_rename(&mut self) {
        let Some(name) = self.selected_recording_name() else {
            return;
        };
        self.rename_buf = name;
        self.mode = InputMode::Rename;
    }

    /// Apply the in-progress rename: move the selected recording to the typed
    /// name (auto-appending `.jsonl` so it stays discoverable), then rescan and
    /// keep the highlight on the renamed file. Rejects empty names, path
    /// separators, and collisions with an explanatory status.
    pub(super) fn submit_rename(&mut self) {
        self.mode = InputMode::Normal;
        let new_raw = std::mem::take(&mut self.rename_buf);
        let Some(old) = self.selected_recording_name() else {
            return;
        };
        let mut new = new_raw.trim().to_string();
        if new.is_empty() || new == old {
            return; // nothing to do
        }
        if new.contains('/') || new.contains('\\') {
            self.set_status(
                "recording name cannot contain a path separator".to_string(),
                true,
            );
            return;
        }
        if !new.ends_with(".jsonl") {
            new.push_str(".jsonl");
        }
        if new == old {
            return;
        }
        let from = self.recordings_dir.join(&old);
        let to = self.recordings_dir.join(&new);
        if to.exists() {
            self.set_status(format!("{new} already exists"), true);
            return;
        }
        match std::fs::rename(&from, &to) {
            Ok(()) => {
                self.set_status(format!("renamed to {new}"), false);
                self.scan_recordings();
                self.select_recording(&new);
            }
            Err(e) => self.set_status(format!("rename failed: {e}"), true),
        }
    }

    /// Delete the selected recording, confirming on the second consecutive `d`.
    /// The first press arms (and reports) the confirmation; the arm is cleared
    /// by any other input (see [`Self::apply`]).
    pub(super) fn confirm_delete_recording(&mut self) {
        let Some(name) = self.selected_recording_name() else {
            return;
        };
        if self.recordings_delete_armed {
            self.recordings_delete_armed = false;
            let path = self.recordings_dir.join(&name);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    self.set_status(format!("deleted {name}"), false);
                    self.scan_recordings();
                }
                Err(e) => self.set_status(format!("delete failed: {e}"), true),
            }
        } else {
            self.recordings_delete_armed = true;
            self.set_status(format!("Press d again to delete {name}"), false);
        }
    }

    /// Select the recording named `name` (after a rescan) and load its view.
    fn select_recording(&mut self, name: &str) {
        if let Some(i) = self.recordings.iter().position(|f| f.name == name) {
            self.recordings_state.select(Some(i));
            self.load_recording_view();
        }
    }
}
