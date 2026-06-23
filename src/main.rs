//! Keyhole — a terminal UI for connecting to message/data brokers, browsing
//! their data, watching realtime activity, and recording live streams to disk.
//!
//! With no subcommand this launches the TUI render loop. The only subcommand is
//! the hidden `gen` packaging helper (man page + shell completions).

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

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::App;
use crate::cli::{Cli, Command};
use crate::config::Config;
use crate::event::AppEvent;
use crate::tui::Tui;

/// How often the UI ticks (clock, stat refresh, animations).
const TICK_PERIOD: Duration = Duration::from_millis(250);
/// Capacity of the app-event channel. Bounded so a firehose applies backpressure.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // `gen` is a packaging helper: emit the man page / completions and exit
    // before any logging, config, or terminal setup, so it runs in a minimal
    // build environment with no writable home directory.
    if let Some(Command::Gen { asset }) = &cli.command {
        return cli::run_gen(asset);
    }

    let paths = config::paths().context("resolving application directories")?;
    // The guard must stay alive for the whole program so buffered logs flush.
    let _log_guard =
        logging::init(&cli.log_level, paths.log_dir()).context("initializing logging")?;

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting keyhole");

    let config_path = cli.config.clone().unwrap_or_else(|| paths.config_file());
    let config = config::load(&config_path).context("loading config")?;
    tracing::info!(
        connections = config.connections.len(),
        "configuration loaded"
    );

    match cli.command {
        // Handled above, before logging/config setup.
        Some(Command::Gen { .. }) => unreachable!("`gen` is dispatched before this match"),
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

    // Tracks the mouse-capture state actually applied to the terminal, so we
    // only issue an escape sequence when the app's desired state changes.
    // `tui::init` enabled capture, so they start in agreement.
    let mut mouse_capture = true;
    while app.running {
        terminal
            .draw(|frame| ui::render(frame, &mut app))
            .context("drawing frame")?;
        match rx.recv().await {
            Some(event) => app.handle_event(event),
            None => break,
        }
        // Reconcile the terminal with the app's desired capture state. The app
        // never touches the terminal itself, which keeps it fully testable.
        if app.mouse_capture() != mouse_capture {
            mouse_capture = app.mouse_capture();
            tui::set_mouse_capture(mouse_capture);
        }
    }

    // Graceful shutdown: cancel tasks (input, tick, connection actors) and wait.
    cancel.cancel();
    tracker.close();
    tracker.wait().await;
    Ok(())
}
