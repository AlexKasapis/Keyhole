//! Recording subsystem.
//!
//! A [`Recorder`] consumes a broker's [`BrokerEvent`] stream — the same stream
//! the live view shows — and writes a binary-safe JSONL envelope ([`Record`]) to
//! disk. The recorder path is lossless (the tail task awaits each write), unlike
//! the lossy `try_send` UI path. [`RecordSink`] is the on-disk, buffered file
//! target; [`Recorder`] is generic over any [`Write`] so it can be unit tested
//! against an in-memory buffer.
//!
//! [`export_csv`] converts a finished JSONL recording into CSV for spreadsheets.

use std::fs::{self, File};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::broker::{BrokerEvent, SubSpec};

/// Flush the recorder at least this often regardless of volume, bounding the
/// data-loss window on a crash for low-rate sources.
pub const FLUSH_EVERY: u64 = 50;

/// One recorded line. Binary payloads are base64 (`encoding = "base64"`); text
/// and JSON are stored verbatim. `meta` carries source-specific extras (stream
/// entry id, matched pattern).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub seq: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub ts: OffsetDateTime,
    pub connection: String,
    pub source: String,
    pub source_type: String,
    pub encoding: String,
    pub payload: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub meta: Vec<(String, String)>,
}

/// Live recording status, reported back to the UI.
#[derive(Debug, Clone)]
pub enum RecordingStatus {
    /// Recording opened to `path`.
    Started { path: PathBuf },
    /// Progress counters while recording.
    Progress { records: u64, bytes: u64 },
    /// Recording closed (flushed) with final counters.
    Stopped {
        records: u64,
        bytes: u64,
        path: PathBuf,
    },
    /// Recording failed; the tail keeps running, recording off.
    Failed { error: String },
}

/// Serializes [`BrokerEvent`]s into JSONL [`Record`]s, tracking counters.
pub struct Recorder<W: Write> {
    writer: W,
    connection: String,
    source_type: String,
    seq: u64,
    records: u64,
    bytes: u64,
}

impl<W: Write> Recorder<W> {
    /// Wrap a writer. `connection` and the source type tag the envelope; the
    /// per-event `source` and timestamp come from each [`BrokerEvent`].
    pub fn new(writer: W, connection: impl Into<String>, spec: &SubSpec) -> Self {
        Self {
            writer,
            connection: connection.into(),
            source_type: spec.source_type().to_string(),
            seq: 0,
            records: 0,
            bytes: 0,
        }
    }

    /// Append one event as a JSONL line. Lossless: the caller awaits this.
    pub fn record(&mut self, ev: &BrokerEvent) -> io::Result<()> {
        let rec = Record {
            seq: self.seq,
            ts: ev.ts,
            connection: self.connection.clone(),
            source: ev.source.clone(),
            source_type: self.source_type.clone(),
            encoding: ev.payload.encoding().tag().to_string(),
            payload: ev.payload.as_text(),
            meta: ev.meta.clone(),
        };
        let mut line = serde_json::to_vec(&rec).map_err(io::Error::other)?;
        line.push(b'\n');
        self.writer.write_all(&line)?;
        self.seq += 1;
        self.records += 1;
        self.bytes += line.len() as u64;
        Ok(())
    }

    /// Flush buffered bytes to the underlying writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Number of records written so far.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Total bytes written so far.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// A buffered, on-disk JSONL file. Implements [`Write`] so a `Recorder` can wrap
/// it; remembers its path for status reporting and the recordings list.
pub struct RecordSink {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl RecordSink {
    /// Create `dir/<connection>-<type>-<target>-<timestamp>.jsonl`, making `dir`
    /// if needed. `now` is injected so callers can stamp deterministically.
    pub fn create(
        dir: &Path,
        connection: &str,
        spec: &SubSpec,
        now: OffsetDateTime,
    ) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(recording_filename(connection, spec, now));
        let file = File::create(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    /// The file this sink writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Write for RecordSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Build the recording filename for a source at a point in time.
fn recording_filename(connection: &str, spec: &SubSpec, now: OffsetDateTime) -> String {
    let stamp = format!(
        "{:04}{:02}{:02}-{:02}{:02}{:02}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    format!(
        "{}-{}-{}-{}.jsonl",
        sanitize(connection),
        spec.source_type(),
        sanitize(&spec.target()),
        stamp
    )
}

/// Make a string safe for a filename component.
fn sanitize(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "_".to_string()
    } else {
        cleaned
    }
}

/// Convert a JSONL recording into CSV, writing a header then one row per record.
/// Returns the number of records exported. Malformed lines abort with an error.
pub fn export_csv(reader: impl BufRead, mut out: impl Write) -> anyhow::Result<u64> {
    writeln!(
        out,
        "seq,ts,connection,source,source_type,encoding,payload,meta"
    )?;
    let mut count = 0u64;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: Record = serde_json::from_str(&line)
            .map_err(|e| anyhow::anyhow!("invalid record on line {}: {e}", count + 1))?;
        let ts = rec.ts.format(&Rfc3339).unwrap_or_default();
        let meta = rec
            .meta
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(";");
        writeln!(
            out,
            "{},{},{},{},{},{},{},{}",
            rec.seq,
            csv_field(&ts),
            csv_field(&rec.connection),
            csv_field(&rec.source),
            csv_field(&rec.source_type),
            csv_field(&rec.encoding),
            csv_field(&rec.payload),
            csv_field(&meta),
        )?;
        count += 1;
    }
    Ok(count)
}

/// RFC 4180 CSV escaping: quote fields containing a comma, quote, or newline,
/// doubling any embedded quotes.
fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::Payload;
    use std::io::Cursor;
    use time::macros::datetime;

    fn event(source: &str, payload: Payload, meta: Vec<(String, String)>) -> BrokerEvent {
        BrokerEvent {
            ts: datetime!(2026-06-19 12:00:00 UTC),
            source: source.to_string(),
            payload,
            meta,
        }
    }

    #[test]
    fn records_jsonl_with_counters_and_encodings() {
        let spec = SubSpec::Channel("news".into());
        let mut rec = Recorder::new(Vec::<u8>::new(), "local", &spec);
        rec.record(&event("news", Payload::Utf8("hello".into()), vec![]))
            .unwrap();
        rec.record(&event(
            "news",
            Payload::Binary(vec![0x00, 0xff]),
            vec![("k".into(), "v".into())],
        ))
        .unwrap();
        rec.flush().unwrap();
        assert_eq!(rec.records(), 2);

        let buf = rec.writer;
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);

        let r0: Record = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(r0.seq, 0);
        assert_eq!(r0.connection, "local");
        assert_eq!(r0.source, "news");
        assert_eq!(r0.source_type, "pubsub");
        assert_eq!(r0.encoding, "utf8");
        assert_eq!(r0.payload, "hello");
        assert!(r0.meta.is_empty());

        let r1: Record = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(r1.seq, 1);
        assert_eq!(r1.encoding, "base64");
        assert_eq!(r1.payload, "AP8="); // base64 of {0x00, 0xff}
        assert_eq!(r1.meta, vec![("k".to_string(), "v".to_string())]);
    }

    #[test]
    fn record_roundtrips_through_serde() {
        let spec = SubSpec::Stream {
            key: "orders".into(),
            db: 0,
        };
        let mut rec = Recorder::new(Vec::<u8>::new(), "c", &spec);
        let ev = event(
            "orders",
            Payload::Json(r#"{"a":1}"#.into()),
            vec![("id".into(), "1-0".into())],
        );
        rec.record(&ev).unwrap();
        let text = String::from_utf8(rec.writer).unwrap();
        let back: Record = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(back.ts, ev.ts);
        assert_eq!(back.source_type, "stream");
        assert_eq!(back.encoding, "json");
        assert_eq!(back.payload, r#"{"a":1}"#);
    }

    #[test]
    fn export_csv_writes_header_and_escapes() {
        // payload contains a comma and quotes -> must be quoted/escaped.
        let jsonl = concat!(
            r#"{"seq":0,"ts":"2026-06-19T12:00:00Z","connection":"local","source":"news","source_type":"pubsub","encoding":"utf8","payload":"a,\"b\"","meta":[["id","1-0"]]}"#,
            "\n",
            "\n", // blank line is skipped
            r#"{"seq":1,"ts":"2026-06-19T12:00:01Z","connection":"local","source":"news","source_type":"pubsub","encoding":"utf8","payload":"plain","meta":[]}"#,
            "\n",
        );
        let mut out = Vec::new();
        let n = export_csv(Cursor::new(jsonl), &mut out).unwrap();
        assert_eq!(n, 2);
        let csv = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(
            lines[0],
            "seq,ts,connection,source,source_type,encoding,payload,meta"
        );
        assert_eq!(
            lines[1],
            r#"0,2026-06-19T12:00:00Z,local,news,pubsub,utf8,"a,""b""",id=1-0"#
        );
        assert_eq!(
            lines[2],
            "1,2026-06-19T12:00:01Z,local,news,pubsub,utf8,plain,"
        );
    }

    #[test]
    fn export_csv_rejects_malformed() {
        let mut out = Vec::new();
        assert!(export_csv(Cursor::new("not json\n"), &mut out).is_err());
    }

    #[test]
    fn filename_is_sanitized_and_stamped() {
        let spec = SubSpec::Pattern("a.b*".into());
        let name = recording_filename("prod/eu", &spec, datetime!(2026-06-19 09:08:07 UTC));
        assert_eq!(name, "prod_eu-psubscribe-a_b_-20260619-090807.jsonl");
    }

    #[test]
    fn record_sink_creates_file_and_path() {
        let dir = std::env::temp_dir().join(format!("brokertui-rec-test-{}", std::process::id()));
        let spec = SubSpec::Channel("c".into());
        let sink = RecordSink::create(&dir, "conn", &spec, datetime!(2026-01-02 03:04:05 UTC))
            .expect("create sink");
        assert!(sink.path().exists());
        assert_eq!(
            sink.path().file_name().unwrap().to_str().unwrap(),
            "conn-pubsub-c-20260102-030405.jsonl"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
