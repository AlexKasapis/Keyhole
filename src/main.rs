//! BrokerTUI — a terminal UI for connecting to message/data brokers, browsing
//! their data, watching realtime activity, and recording live streams to disk.
//!
//! With no subcommand this launches the TUI render loop; the `record` and
//! `export` subcommands run headlessly, reusing the broker + recording stack.

mod app;
mod broker;
mod cli;
mod config;
mod event;
mod logging;
mod recording;
mod theme;
mod tui;
mod ui;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context};
use clap::Parser;
use futures_util::StreamExt;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::App;
use crate::broker::redis::RedisConnection;
use crate::broker::{BrokerConnection, SubSpec};
use crate::cli::{Cli, Command};
use crate::config::{Config, ConnectionConfig, RedisProfile};
use crate::event::AppEvent;
use crate::recording::{RecordSink, Recorder};
use crate::tui::Tui;

/// How often the UI ticks (clock, stat refresh, animations).
const TICK_PERIOD: Duration = Duration::from_millis(250);
/// Capacity of the app-event channel. Bounded so a firehose applies backpressure.
const EVENT_CHANNEL_CAPACITY: usize = 1024;
/// Flush + report progress every N records during a headless recording.
const RECORD_PROGRESS_EVERY: u64 = 50;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let paths = config::paths().context("resolving application directories")?;
    // The guard must stay alive for the whole program so buffered logs flush.
    let _log_guard =
        logging::init(&cli.log_level, paths.log_dir()).context("initializing logging")?;

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting brokertui");

    let config_path = cli.config.clone().unwrap_or_else(|| paths.config_file());
    let config = config::load(&config_path).context("loading config")?;
    tracing::info!(
        connections = config.connections.len(),
        "configuration loaded"
    );

    match cli.command {
        // Headless: convert a recording to CSV. No broker, no terminal.
        Some(Command::Export { file, csv, out }) => run_export(&file, csv, out.as_deref()),
        // Headless: record a source until Ctrl-C. No terminal.
        Some(Command::Record {
            connect,
            source,
            out,
        }) => {
            let dir = out.unwrap_or_else(|| paths.recordings_dir());
            run_record(config, &connect, &source, dir).await
        }
        // Default: the interactive TUI.
        None => {
            let recordings_dir = paths.recordings_dir();
            tracing::debug!(
                connect = ?cli.connect,
                config_file = %config_path.display(),
                recordings_dir = %recordings_dir.display(),
                "startup configuration"
            );
            let mut terminal = tui::init();
            let result = run(
                &mut terminal,
                config,
                config_path,
                recordings_dir,
                cli.connect,
            )
            .await;
            tui::restore();

            if let Err(err) = &result {
                tracing::error!(error = %err, "exiting with error");
                // Safe to print now: the terminal has been restored.
                eprintln!("error: {err:?}");
            }
            result
        }
    }
}

/// The render loop. Owns `App`; only draws and reacts to channel events.
async fn run(
    terminal: &mut Tui,
    config: Config,
    config_path: PathBuf,
    recordings_dir: PathBuf,
    connect: Option<String>,
) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(EVENT_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();

    event::spawn_input(tx.clone(), cancel.child_token(), &tracker);
    event::spawn_tick(tx.clone(), cancel.child_token(), &tracker, TICK_PERIOD);

    let mut app = App::new(
        config,
        config_path,
        recordings_dir,
        tx.clone(),
        tracker.clone(),
        cancel.clone(),
        connect,
    );
    // Our local handle is no longer needed; App and the spawned tasks hold clones.
    drop(tx);
    app.on_start();

    while app.running {
        terminal
            .draw(|frame| ui::render(frame, &mut app))
            .context("drawing frame")?;
        match rx.recv().await {
            Some(event) => app.handle_event(event),
            None => break,
        }
    }

    // Graceful shutdown: cancel tasks (input, tick, connection actors) and wait.
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    Ok(())
}

/// Export a JSONL recording to CSV.
fn run_export(file: &Path, _csv: bool, out: Option<&Path>) -> anyhow::Result<()> {
    let input = std::fs::File::open(file).with_context(|| format!("opening {}", file.display()))?;
    let reader = std::io::BufReader::new(input);
    let count = match out {
        Some(path) => {
            let w = std::fs::File::create(path)
                .with_context(|| format!("creating {}", path.display()))?;
            recording::export_csv(reader, std::io::BufWriter::new(w))?
        }
        None => {
            let stdout = std::io::stdout();
            recording::export_csv(reader, stdout.lock())?
        }
    };
    // Summary to stderr — stdout may be carrying the CSV.
    eprintln!("exported {count} records");
    Ok(())
}

/// Headlessly record a source to JSONL until Ctrl-C or the source closes.
async fn run_record(
    config: Config,
    profile_name: &str,
    source: &str,
    dir: PathBuf,
) -> anyhow::Result<()> {
    let profile = find_profile(&config, profile_name)
        .ok_or_else(|| anyhow!("no connection profile named '{profile_name}'"))?;
    let spec = SubSpec::parse(source, profile.db)?;

    let password = resolve_password(&profile).await?;
    let preview = config.settings.value_preview_bytes;
    let mut conn = RedisConnection::new(profile.clone(), password, preview);
    conn.connect().await.context("connecting")?;
    let mut stream = conn.subscribe(spec.clone()).await.context("subscribing")?;

    let sink = RecordSink::create(&dir, &profile.name, &spec, OffsetDateTime::now_utc())
        .context("opening recording file")?;
    let path = sink.path().to_path_buf();
    let mut recorder = Recorder::new(sink, profile.name.clone(), &spec);
    eprintln!("recording {} → {}", spec.label(), path.display());
    eprintln!("(Ctrl-C to stop)");

    let mut count = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            item = stream.next() => match item {
                Some(ev) => {
                    recorder.record(&ev).context("writing record")?;
                    count += 1;
                    if count.is_multiple_of(RECORD_PROGRESS_EVERY) {
                        recorder.flush().ok();
                        eprint!("\r{count} events…");
                    }
                }
                None => {
                    eprintln!("\nsource closed");
                    break;
                }
            },
        }
    }
    recorder.flush().context("flushing recording")?;
    eprintln!(
        "\nrecorded {count} events ({} bytes) → {}",
        recorder.bytes(),
        path.display()
    );
    Ok(())
}

/// Look up a Redis profile by name.
fn find_profile(config: &Config, name: &str) -> Option<RedisProfile> {
    config.connections.iter().find_map(|c| match c {
        ConnectionConfig::Redis(p) if p.name == name => Some(p.clone()),
        _ => None,
    })
}

/// Resolve a profile's secret off the async runtime (keyring access can block).
async fn resolve_password(profile: &RedisProfile) -> anyhow::Result<Option<String>> {
    let spec = profile.password_spec();
    let account = profile.name.clone();
    tokio::task::spawn_blocking(move || config::resolve_secret(&spec, &account)).await?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redis(name: &str) -> ConnectionConfig {
        ConnectionConfig::Redis(RedisProfile {
            name: name.into(),
            host: "127.0.0.1".into(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            tls: false,
        })
    }

    #[test]
    fn find_profile_matches_by_name() {
        let config = Config {
            connections: vec![redis("a"), redis("b")],
            ..Default::default()
        };
        assert_eq!(
            find_profile(&config, "b").map(|p| p.name).as_deref(),
            Some("b")
        );
        assert!(find_profile(&config, "missing").is_none());
        assert!(
            find_profile(&Config::default(), "a").is_none(),
            "empty config finds nothing"
        );
    }
}
