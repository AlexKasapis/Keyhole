//! Command-line interface. With no subcommand the TUI launches; the `record`
//! and `export` subcommands run headlessly (no terminal), reusing the broker and
//! recording stack minus `ui/`.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// BrokerTUI — connect to brokers, browse data, and record live streams.
#[derive(Debug, Parser)]
#[command(name = "brokertui", version, about)]
pub struct Cli {
    /// Path to the config file (defaults to the platform config directory).
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Connect to a saved connection profile on startup (TUI mode).
    #[arg(long, value_name = "PROFILE")]
    pub connect: Option<String>,

    /// Log level / filter, e.g. `info` or `brokertui=debug`.
    #[arg(long, default_value = "info", value_name = "FILTER")]
    pub log_level: String,

    /// Headless subcommand; omit to launch the TUI.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Headless subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Record a live source to a JSONL file until interrupted (Ctrl-C).
    Record {
        /// Connection profile name (from the config file).
        #[arg(long, value_name = "PROFILE")]
        connect: String,
        /// Source spec: `pubsub:ch`, `psub:ch.*`, or `stream:key`.
        #[arg(long, value_name = "SPEC")]
        source: String,
        /// Output directory (defaults to the data recordings directory).
        #[arg(long, value_name = "DIR")]
        out: Option<PathBuf>,
    },
    /// Export a JSONL recording to CSV (stdout by default).
    Export {
        /// The `.jsonl` recording to read.
        #[arg(value_name = "FILE")]
        file: PathBuf,
        /// Emit CSV (the only supported format today).
        #[arg(long)]
        csv: bool,
        /// Write to this file instead of stdout.
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
    },
}
