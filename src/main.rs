//! Keyhole — a terminal UI for connecting to message/data brokers, browsing
//! their data, watching realtime activity, and recording live streams to disk.
//!
//! With no subcommand this launches the TUI render loop. The only subcommand is
//! the hidden `gen` packaging helper (man page + shell completions).

mod app;
mod broker;
mod cli;
mod config;
mod dev;
mod event;
mod logging;
mod recording;
mod theme;
mod tui;
mod ui;

use std::path::PathBuf;
use std::time::{Duration, Instant};

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

/// How often the UI ticks. This also paces redraws, so it sets the animation
/// frame rate: ~33 ms ≈ 30 fps, smooth enough for the breathing connection dot.
/// Data refreshes (stats, key-browser auto-scan) ride this tick but are gated to
/// their own slower cadences by tick counts, so the faster tick doesn't hammer
/// the broker. Mirror any change in [`crate::app`]'s `TICK_PERIOD_MS`.
const TICK_PERIOD: Duration = Duration::from_millis(33);
/// Capacity of the app-event channel. Bounded so a firehose applies backpressure.
const EVENT_CHANNEL_CAPACITY: usize = 1024;
/// Smallest gap between full repaints (~60 fps). Under a high-rate feed (e.g.
/// redis `MONITOR`) the render loop coalesces every queued event into one frame
/// and holds repaints to this budget, so a firehose can't drive the renderer at
/// the event rate and starve input. It sits far below the cost of per-event
/// redraws yet far above the 250 ms `TICK_PERIOD` that paces stats/animation, so
/// nothing visible is lost. Sparse, interactive events never reach the cap (the
/// previous frame is already older than the budget), so typing/navigation latency
/// is unchanged.
const FRAME_BUDGET: Duration = Duration::from_millis(16);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // `gen` is a packaging helper: emit the man page / completions and exit
    // before any logging, config, or terminal setup, so it runs in a minimal
    // build environment with no writable home directory.
    if let Some(Command::Gen { asset }) = &cli.command {
        return cli::run_gen(asset);
    }

    // `dev` is a headless fake-data helper (seed/publish to local brokers). Like
    // `gen` it runs before logging/terminal setup and prints progress straight
    // to stdout, so it never touches the file logger or the alternate screen.
    if let Some(Command::Dev { action }) = &cli.command {
        return dev::run(action).await;
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
        Some(Command::Dev { .. }) => unreachable!("`dev` is dispatched before this match"),
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

    // Paint once up front so the UI is on screen before the first event.
    terminal
        .draw(|frame| ui::render(frame, &mut app))
        .context("drawing frame")?;
    let mut last_draw = Instant::now();

    while app.running {
        // Refresh per-frame budgets (the Monitor feed's reveal cap) before this
        // frame's events are drained, so a firehose is paced into a steady scroll.
        app.begin_frame();
        // Block for the next event so the loop sleeps when idle — no busy-poll.
        match rx.recv().await {
            Some(event) => app.handle_event(event),
            None => break,
        }
        // Fold every event already queued into this same batch: a burst of
        // realtime events becomes one redraw, not one redraw per event.
        app.drain_events(&mut rx);

        // Cap the repaint rate. When the last frame was painted less than one
        // budget ago (a burst), wait out the remainder — folding in further
        // arrivals — instead of spinning the renderer at the event rate. Sparse
        // events leave the previous frame already older than the budget, so this
        // never delays an interactive redraw.
        if app.running {
            if let Some(wait) = frame_wait(last_draw.elapsed(), FRAME_BUDGET) {
                tokio::time::sleep(wait).await;
                app.drain_events(&mut rx);
            }
            terminal
                .draw(|frame| ui::render(frame, &mut app))
                .context("drawing frame")?;
            last_draw = Instant::now();
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

/// How long to wait before the next repaint is allowed, given how long ago the
/// last frame was painted and the per-frame `budget`. `None` means "draw now":
/// the budget is already spent, which is the common case for sparse interactive
/// events, so they incur no added latency. `Some(d)` throttles a burst to the
/// frame budget.
fn frame_wait(since_last: Duration, budget: Duration) -> Option<Duration> {
    budget.checked_sub(since_last).filter(|d| !d.is_zero())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_wait_returns_none_once_budget_is_spent() {
        let budget = Duration::from_millis(16);
        // Exactly at and past the budget: draw now, no wait.
        assert_eq!(frame_wait(budget, budget), None);
        assert_eq!(frame_wait(Duration::from_millis(50), budget), None);
    }

    #[test]
    fn frame_wait_returns_remaining_budget_within_a_burst() {
        let budget = Duration::from_millis(16);
        // Painted 6 ms ago: wait the remaining 10 ms before the next frame.
        assert_eq!(
            frame_wait(Duration::from_millis(6), budget),
            Some(Duration::from_millis(10))
        );
        // Painted "just now": wait nearly the whole budget.
        assert_eq!(frame_wait(Duration::ZERO, budget), Some(budget));
    }
}
