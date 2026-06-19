//! BrokerTUI — a terminal UI for connecting to message/data brokers, browsing
//! their data, watching realtime activity, and recording live streams to disk.
//!
//! Phase 0 wires up the skeleton: CLI parsing, file-only logging, terminal
//! lifecycle, the async↔UI event loop, and a minimal render pass.

mod app;
mod broker;
mod cli;
mod config;
mod event;
mod logging;
mod recording;
mod tui;
mod ui;

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::App;
use crate::cli::Cli;
use crate::event::AppEvent;
use crate::tui::Tui;

/// How often the UI ticks (clock, stat refresh, animations).
const TICK_PERIOD: Duration = Duration::from_millis(250);
/// Capacity of the app-event channel. Bounded so a firehose applies backpressure.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let paths = config::paths().context("resolving application directories")?;
    // The guard must stay alive for the whole program so buffered logs flush.
    let _log_guard =
        logging::init(&cli.log_level, paths.log_dir()).context("initializing logging")?;

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting brokertui");

    let config_path = cli.config.clone().unwrap_or_else(|| paths.config_file());
    tracing::debug!(
        connect = ?cli.connect,
        config_file = %config_path.display(),
        recordings_dir = %paths.recordings_dir().display(),
        "startup configuration"
    );
    let config = config::load(&config_path).context("loading config")?;
    tracing::info!(
        connections = config.connections.len(),
        "configuration loaded"
    );

    let mut terminal = tui::init();
    let result = run(&mut terminal).await;
    tui::restore();

    if let Err(err) = &result {
        tracing::error!(error = %err, "exiting with error");
        // Safe to print now: the terminal has been restored.
        eprintln!("error: {err:?}");
    }
    result
}

/// The render loop. Owns `App`; only draws and reacts to channel events.
async fn run(terminal: &mut Tui) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(EVENT_CHANNEL_CAPACITY);
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();

    event::spawn_input(tx.clone(), cancel.child_token(), &tracker);
    event::spawn_tick(tx.clone(), cancel.child_token(), &tracker, TICK_PERIOD);
    // Drop our own handle; the only remaining senders live in the spawned tasks,
    // so the channel closes once they stop.
    drop(tx);

    let mut app = App::new();
    while app.running {
        terminal
            .draw(|frame| ui::render(frame, &mut app))
            .context("drawing frame")?;
        match rx.recv().await {
            Some(event) => app.handle_event(event),
            None => break,
        }
    }

    // Graceful shutdown: cancel tasks and wait for them to finish.
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    Ok(())
}
