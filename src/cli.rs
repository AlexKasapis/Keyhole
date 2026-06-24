//! Command-line interface. With no subcommand the TUI launches. The only
//! subcommand is the hidden `gen` packaging helper (man page + completions).

use std::io::Write;
use std::path::PathBuf;

use anyhow::Context;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;

/// Keyhole — connect to brokers, browse data, and record live streams.
#[derive(Debug, Parser)]
#[command(name = "keyhole", version, about)]
pub struct Cli {
    /// Path to the config file (defaults to the platform config directory).
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Connect to a saved connection profile on startup (TUI mode).
    #[arg(long, value_name = "PROFILE")]
    pub connect: Option<String>,

    /// Log level / filter, e.g. `info` or `keyhole=debug`.
    #[arg(long, default_value = "info", value_name = "FILTER")]
    pub log_level: String,

    /// Subcommand; omit to launch the TUI.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// CLI subcommands. Both are hidden maintainer/dev helpers (`gen` for packaging,
/// `dev` for local fake data); with no subcommand the interactive TUI launches.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Generate packaging assets (man page, shell completions) and exit.
    ///
    /// Hidden from `--help`: this is a maintainer/packaging helper, not an
    /// end-user command. It introspects the live clap definition, so the
    /// artifacts never drift from the actual flags.
    #[command(hide = true)]
    Gen {
        #[command(subcommand)]
        asset: GenAsset,
    },

    /// Seed or publish fake data to the local dev brokers and exit.
    ///
    /// Hidden from `--help`: a developer/testing helper that writes to brokers,
    /// not an end-user command. Connection parameters come from the config file
    /// (default `config.dev.toml`, the dockerized broker stack).
    #[command(hide = true)]
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
}

/// What `keyhole dev` should do.
#[derive(Debug, Subcommand)]
pub enum DevAction {
    /// One-shot: seed the Redis keyspace with sample browse data.
    Seed {
        /// Config file to read the `redis` connection from.
        #[arg(long, value_name = "FILE", default_value = "config.dev.toml")]
        config: PathBuf,
        /// Key namespace to seed under (defaults to `keyhole:demo`).
        #[arg(long, value_name = "PREFIX")]
        prefix: Option<String>,
    },
    /// Continuous: publish fake traffic to the brokers until Ctrl-C.
    Publish {
        /// Which broker(s) to publish to.
        #[arg(long, value_name = "BROKER", default_value = "all")]
        broker: DevBroker,
        /// Messages per second, per broker.
        #[arg(long, default_value_t = 2.0, value_name = "HZ")]
        rate: f64,
        /// Config file to read the connection profiles from.
        #[arg(long, value_name = "FILE", default_value = "config.dev.toml")]
        config: PathBuf,
    },
}

/// Which broker(s) `keyhole dev publish` targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DevBroker {
    Redis,
    Amqp,
    Rabbitmq,
    All,
}

/// Which packaging asset `keyhole gen` should emit.
#[derive(Debug, Subcommand)]
pub enum GenAsset {
    /// Write the roff man page (to stdout, or `DIR/keyhole.1` with `--out`).
    Man {
        /// Write `keyhole.1` into this directory instead of stdout.
        #[arg(long, value_name = "DIR")]
        out: Option<PathBuf>,
    },
    /// Write a shell completion script (to stdout, or into `DIR` with `--out`).
    Completions {
        /// Target shell.
        #[arg(value_name = "SHELL")]
        shell: Shell,
        /// Write the conventionally-named script into this directory instead
        /// of stdout (e.g. `keyhole.bash`, `_keyhole`, `keyhole.fish`).
        #[arg(long, value_name = "DIR")]
        out: Option<PathBuf>,
    },
}

/// Render the roff man page for the whole CLI into bytes.
pub fn render_man() -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    clap_mangen::Man::new(Cli::command()).render(&mut buf)?;
    Ok(buf)
}

/// Render the completion script for `shell` into bytes.
pub fn render_completions(shell: Shell) -> Vec<u8> {
    let mut cmd = Cli::command();
    let bin = cmd.get_name().to_string();
    let mut buf = Vec::new();
    clap_complete::generate(shell, &mut cmd, bin, &mut buf);
    buf
}

/// Execute `keyhole gen`: write the requested asset to stdout or a directory.
///
/// Pure and side-effect-light by design — it touches neither logging, config,
/// nor the terminal, so it runs in a minimal packaging container.
pub fn run_gen(asset: &GenAsset) -> anyhow::Result<()> {
    match asset {
        GenAsset::Man { out } => {
            let bytes = render_man().context("rendering man page")?;
            match out {
                Some(dir) => {
                    std::fs::create_dir_all(dir)
                        .with_context(|| format!("creating {}", dir.display()))?;
                    let path = dir.join("keyhole.1");
                    std::fs::write(&path, bytes)
                        .with_context(|| format!("writing {}", path.display()))?;
                    println!("{}", path.display());
                }
                None => std::io::stdout()
                    .write_all(&bytes)
                    .context("writing man page to stdout")?,
            }
        }
        GenAsset::Completions { shell, out } => match out {
            // `generate_to` picks the shell's conventional filename for us.
            Some(dir) => {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                let mut cmd = Cli::command();
                let bin = cmd.get_name().to_string();
                let path = clap_complete::generate_to(*shell, &mut cmd, bin, dir)
                    .with_context(|| format!("writing completions into {}", dir.display()))?;
                println!("{}", path.display());
            }
            None => std::io::stdout()
                .write_all(&render_completions(*shell))
                .context("writing completions to stdout")?,
        },
    }
    Ok(())
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
        let cli = Cli::try_parse_from(["keyhole"]).unwrap();
        assert!(cli.command.is_none());
        assert!(cli.config.is_none());
        assert!(cli.connect.is_none());
        assert_eq!(cli.log_level, "info");
    }

    #[test]
    fn top_level_flags_parse() {
        let cli = Cli::try_parse_from([
            "keyhole",
            "--config",
            "/etc/bt.toml",
            "--connect",
            "prod",
            "--log-level",
            "keyhole=debug",
        ])
        .unwrap();
        assert_eq!(cli.config.as_deref(), Some(Path::new("/etc/bt.toml")));
        assert_eq!(cli.connect.as_deref(), Some("prod"));
        assert_eq!(cli.log_level, "keyhole=debug");
        assert!(cli.command.is_none());
    }

    #[test]
    fn dev_seed_parses_with_default_config_and_optional_prefix() {
        let cli = Cli::try_parse_from(["keyhole", "dev", "seed"]).unwrap();
        match cli.command {
            Some(Command::Dev {
                action: DevAction::Seed { config, prefix },
            }) => {
                assert_eq!(config, Path::new("config.dev.toml"));
                assert!(prefix.is_none());
            }
            other => panic!("expected dev seed, got {other:?}"),
        }

        let cli = Cli::try_parse_from(["keyhole", "dev", "seed", "--prefix", "demo:ns"]).unwrap();
        match cli.command {
            Some(Command::Dev {
                action: DevAction::Seed { prefix, .. },
            }) => assert_eq!(prefix.as_deref(), Some("demo:ns")),
            other => panic!("expected dev seed, got {other:?}"),
        }
    }

    #[test]
    fn dev_publish_parses_broker_and_rate() {
        let cli = Cli::try_parse_from([
            "keyhole", "dev", "publish", "--broker", "all", "--rate", "5",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Dev {
                action: DevAction::Publish { broker, rate, .. },
            }) => {
                assert_eq!(broker, DevBroker::All);
                assert_eq!(rate, 5.0);
            }
            other => panic!("expected dev publish, got {other:?}"),
        }

        // Defaults: broker = all, rate = 2.0.
        let cli = Cli::try_parse_from(["keyhole", "dev", "publish"]).unwrap();
        match cli.command {
            Some(Command::Dev {
                action: DevAction::Publish { broker, rate, .. },
            }) => {
                assert_eq!(broker, DevBroker::All);
                assert_eq!(rate, 2.0);
            }
            other => panic!("expected dev publish, got {other:?}"),
        }
    }

    #[test]
    fn dev_rejects_unknown_broker_and_missing_action() {
        assert!(Cli::try_parse_from(["keyhole", "dev", "publish", "--broker", "kafka"]).is_err());
        assert!(Cli::try_parse_from(["keyhole", "dev"]).is_err());
    }

    #[test]
    fn gen_man_parses_with_optional_out() {
        let cli = Cli::try_parse_from(["keyhole", "gen", "man"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Gen {
                asset: GenAsset::Man { out: None }
            })
        ));

        let cli = Cli::try_parse_from(["keyhole", "gen", "man", "--out", "/pkg"]).unwrap();
        match cli.command {
            Some(Command::Gen {
                asset: GenAsset::Man { out },
            }) => assert_eq!(out.as_deref(), Some(Path::new("/pkg"))),
            other => panic!("expected gen man, got {other:?}"),
        }
    }

    #[test]
    fn gen_completions_parses_a_shell_and_optional_out() {
        let cli = Cli::try_parse_from(["keyhole", "gen", "completions", "bash"]).unwrap();
        match cli.command {
            Some(Command::Gen {
                asset: GenAsset::Completions { shell, out },
            }) => {
                assert_eq!(shell, Shell::Bash);
                assert!(out.is_none());
            }
            other => panic!("expected gen completions, got {other:?}"),
        }

        let cli =
            Cli::try_parse_from(["keyhole", "gen", "completions", "zsh", "--out", "/pkg"]).unwrap();
        match cli.command {
            Some(Command::Gen {
                asset: GenAsset::Completions { shell, out },
            }) => {
                assert_eq!(shell, Shell::Zsh);
                assert_eq!(out.as_deref(), Some(Path::new("/pkg")));
            }
            other => panic!("expected gen completions, got {other:?}"),
        }
    }

    #[test]
    fn gen_rejects_unknown_shells_and_missing_subcommands() {
        assert!(Cli::try_parse_from(["keyhole", "gen", "completions", "tcsh"]).is_err());
        assert!(Cli::try_parse_from(["keyhole", "gen", "completions"]).is_err());
        assert!(Cli::try_parse_from(["keyhole", "gen"]).is_err());
    }

    #[test]
    fn man_page_renders_and_mentions_the_binary() {
        let man = String::from_utf8(render_man().expect("man page should render"))
            .expect("man page is valid UTF-8");
        // The roff `.TH` title header and the binary name must be present.
        assert!(man.contains(".TH"), "missing roff title header");
        assert!(man.contains("keyhole"), "man page omits the binary name");
        // The subcommands surface; the hidden `gen` command must not leak.
        assert!(
            man.contains("record"),
            "man page omits the record subcommand"
        );
        assert!(
            !man.contains("\\fIgen\\fR") && !man.contains("gen man"),
            "hidden gen subcommand leaked into the man page"
        );
    }

    #[test]
    fn completions_render_non_empty_for_every_shell() {
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::PowerShell,
            Shell::Elvish,
        ] {
            let script = String::from_utf8(render_completions(shell))
                .unwrap_or_else(|_| panic!("{shell} completions are valid UTF-8"));
            assert!(!script.is_empty(), "{shell} completion script is empty");
            assert!(
                script.contains("keyhole"),
                "{shell} completion script omits the binary name"
            );
        }
    }
}
