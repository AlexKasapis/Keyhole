//! `App` recordings screen: scan the recordings dir and load previews.
//! Part of the `app` module (overview in `app.rs`).

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
        self.load_recording_preview();
    }

    /// (Re)load the preview of the currently-selected recording into
    /// [`Self::recording_preview`]. A no-op when the cached preview is already
    /// for the selected file, so it is cheap to call after every selection
    /// change; an actual reload reads at most [`recording::PREVIEW_CAP`]
    /// records from the head of the file.
    pub(super) fn load_recording_preview(&mut self) {
        let name = self
            .recordings_state
            .selected()
            .and_then(|i| self.recordings.get(i))
            .map(|f| f.name.clone());
        let Some(name) = name else {
            self.recording_preview = None;
            return;
        };
        if self
            .recording_preview
            .as_ref()
            .is_some_and(|(cached, _)| cached == &name)
        {
            return;
        }
        let path = self.recordings_dir.join(&name);
        let preview = match std::fs::File::open(&path) {
            Ok(file) => recording::preview(std::io::BufReader::new(file)),
            Err(e) => RecordingPreview {
                error: Some(e.to_string()),
                ..Default::default()
            },
        };
        self.recording_preview = Some((name, preview));
    }
}
