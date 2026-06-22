//! File-only logging. The TUI owns the terminal, so logs must never touch
//! stdout/stderr — a stray write would corrupt the screen. We append to a daily
//! rolling file under the data directory.

use std::path::PathBuf;

use anyhow::Context;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

/// Initialize logging to a rolling file in `log_dir`.
///
/// Returns a [`WorkerGuard`] that **must be kept alive** for the lifetime of the
/// program; dropping it flushes and stops the background writer.
pub fn init(filter: &str, log_dir: PathBuf) -> anyhow::Result<WorkerGuard> {
    std::fs::create_dir_all(&log_dir)
        .with_context(|| format!("creating log directory {}", log_dir.display()))?;

    let appender = tracing_appender::rolling::daily(&log_dir, "keyhole.log");
    let (writer, guard) = tracing_appender::non_blocking(appender);

    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(writer)
        .with_ansi(false)
        .with_target(true)
        .init();

    Ok(guard)
}
