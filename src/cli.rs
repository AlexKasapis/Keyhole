//! Command-line interface. Phase 0 exposes only global flags; the `record` and
//! `export` subcommands arrive in Phase 2 alongside the recording subsystem.

use std::path::PathBuf;

use clap::Parser;

/// BrokerTUI — connect to brokers, browse data, and record live streams.
#[derive(Debug, Parser)]
#[command(name = "brokertui", version, about)]
pub struct Cli {
    /// Path to the config file (defaults to the platform config directory).
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Connect to a saved connection profile on startup.
    #[arg(long, value_name = "PROFILE")]
    pub connect: Option<String>,

    /// Log level / filter, e.g. `info` or `brokertui=debug`.
    #[arg(long, default_value = "info", value_name = "FILTER")]
    pub log_level: String,
}
