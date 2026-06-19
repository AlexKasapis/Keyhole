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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use std::path::Path;

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn bare_invocation_defaults_to_the_tui() {
        let cli = Cli::try_parse_from(["brokertui"]).unwrap();
        assert!(cli.command.is_none());
        assert!(cli.config.is_none());
        assert!(cli.connect.is_none());
        assert_eq!(cli.log_level, "info");
    }

    #[test]
    fn top_level_flags_parse() {
        let cli = Cli::try_parse_from([
            "brokertui",
            "--config",
            "/etc/bt.toml",
            "--connect",
            "prod",
            "--log-level",
            "brokertui=debug",
        ])
        .unwrap();
        assert_eq!(cli.config.as_deref(), Some(Path::new("/etc/bt.toml")));
        assert_eq!(cli.connect.as_deref(), Some("prod"));
        assert_eq!(cli.log_level, "brokertui=debug");
        assert!(cli.command.is_none());
    }

    #[test]
    fn record_subcommand_parses_with_optional_out() {
        let cli = Cli::try_parse_from([
            "brokertui",
            "record",
            "--connect",
            "p",
            "--source",
            "pubsub:n",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Record {
                connect,
                source,
                out,
            }) => {
                assert_eq!(connect, "p");
                assert_eq!(source, "pubsub:n");
                assert!(out.is_none());
            }
            other => panic!("expected Record, got {other:?}"),
        }

        let cli = Cli::try_parse_from([
            "brokertui",
            "record",
            "--connect",
            "p",
            "--source",
            "stream:k",
            "--out",
            "/data",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Record { out, .. }) => {
                assert_eq!(out.as_deref(), Some(Path::new("/data")))
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn record_requires_connect_and_source() {
        assert!(Cli::try_parse_from(["brokertui", "record", "--source", "pubsub:c"]).is_err());
        assert!(Cli::try_parse_from(["brokertui", "record", "--connect", "p"]).is_err());
    }

    #[test]
    fn export_subcommand_parses() {
        let cli = Cli::try_parse_from(["brokertui", "export", "rec.jsonl", "--csv"]).unwrap();
        match cli.command {
            Some(Command::Export { file, csv, out }) => {
                assert_eq!(file, PathBuf::from("rec.jsonl"));
                assert!(csv);
                assert!(out.is_none());
            }
            other => panic!("expected Export, got {other:?}"),
        }
    }

    #[test]
    fn export_csv_defaults_off_and_accepts_out() {
        let cli =
            Cli::try_parse_from(["brokertui", "export", "rec.jsonl", "--out", "rec.csv"]).unwrap();
        match cli.command {
            Some(Command::Export { csv, out, .. }) => {
                assert!(!csv, "the --csv flag defaults to false");
                assert_eq!(out.as_deref(), Some(Path::new("rec.csv")));
            }
            other => panic!("expected Export, got {other:?}"),
        }
    }

    #[test]
    fn export_requires_a_file_argument() {
        assert!(Cli::try_parse_from(["brokertui", "export"]).is_err());
    }
}
