//! Dev-only fake-data tooling, reached via the hidden `keyhole dev` subcommand.
//!
//! `dev seed` fills the Redis keyspace once; `dev publish` streams fake traffic
//! to the brokers until Ctrl-C so every realtime tail has something to show.
//! Connection parameters (host / port / credentials / AMQP destinations) come
//! from the same config file the TUI reads — by default `config.dev.toml` — so
//! the publisher can never target a different endpoint than the reader.
//!
//! This is never used by the TUI itself; it writes to brokers, so it lives apart
//! from the observe-only app code.

mod amqp;
pub mod fixtures;
mod rabbitmq;
mod redis;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio_util::sync::CancellationToken;

use crate::cli::{DevAction, DevBroker};
use crate::config::{self, Config, ConnectionConfig};

/// Dispatch the `keyhole dev` subcommand. Headless: prints progress to stdout.
pub async fn run(action: &DevAction) -> anyhow::Result<()> {
    match action {
        DevAction::Seed { config, prefix } => seed(config, prefix.as_deref()).await,
        DevAction::Publish {
            config,
            broker,
            rate,
        } => publish(config, *broker, *rate).await,
    }
}

async fn seed(config_path: &Path, prefix: Option<&str>) -> anyhow::Result<()> {
    let cfg = load(config_path)?;
    let connection = require(&cfg, "redis", config_path)?;
    let ConnectionConfig::Redis(profile) = connection else {
        unreachable!("require() matched on type_label == redis")
    };
    let password = resolve(connection).await?;
    let prefix = prefix.unwrap_or(fixtures::DEFAULT_PREFIX);
    let written = redis::seed(profile, password.as_deref(), prefix).await?;
    println!(
        "seeded {written} Redis keys under `{prefix}:` on {}:{}",
        profile.host, profile.port
    );
    Ok(())
}

async fn publish(config_path: &Path, broker: DevBroker, rate: f64) -> anyhow::Result<()> {
    let cfg = load(config_path)?;
    let interval = Duration::from_secs_f64(1.0 / rate.max(0.1));

    // Ctrl-C cancels the token, which unwinds every publisher loop cleanly.
    let token = CancellationToken::new();
    let signal_token = token.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\nstopping publishers…");
            signal_token.cancel();
        }
    });

    let want = |k: DevBroker| broker == DevBroker::All || broker == k;
    type Job = Pin<Box<dyn Future<Output = anyhow::Result<()>>>>;
    let mut jobs: Vec<Job> = Vec::new();

    if want(DevBroker::Redis) {
        let connection = require(&cfg, "redis", config_path)?;
        let ConnectionConfig::Redis(profile) = connection else {
            unreachable!()
        };
        let (profile, password, token) =
            (profile.clone(), resolve(connection).await?, token.clone());
        jobs.push(Box::pin(redis::publish(
            profile,
            password,
            fixtures::DEFAULT_PREFIX.to_string(),
            interval,
            token,
        )));
    }
    if want(DevBroker::Amqp) {
        let connection = require(&cfg, "amqp", config_path)?;
        let (connection, password, token) = (
            connection.clone(),
            resolve(connection).await?,
            token.clone(),
        );
        jobs.push(Box::pin(amqp::publish(
            connection, password, interval, token,
        )));
    }
    if want(DevBroker::Rabbitmq) {
        let connection = require(&cfg, "rabbitmq", config_path)?;
        let ConnectionConfig::Rabbitmq(profile) = connection else {
            unreachable!()
        };
        let (profile, password, token) =
            (profile.clone(), resolve(connection).await?, token.clone());
        jobs.push(Box::pin(rabbitmq::publish(
            profile, password, interval, token,
        )));
    }

    if jobs.is_empty() {
        return Err(anyhow!("no brokers selected"));
    }
    println!("publishing fake traffic (Ctrl-C to stop)…");

    // Run the publishers concurrently; the first failure aborts the rest.
    futures_util::future::try_join_all(jobs).await?;
    Ok(())
}

fn load(path: &Path) -> anyhow::Result<Config> {
    config::load(path).with_context(|| format!("loading {}", path.display()))
}

/// The first connection of `kind` (`redis` / `amqp` / `rabbitmq`), or an error
/// naming the file so the dev knows which config is missing the profile.
fn require<'a>(cfg: &'a Config, kind: &str, path: &Path) -> anyhow::Result<&'a ConnectionConfig> {
    cfg.connections
        .iter()
        .find(|c| c.type_label() == kind)
        .ok_or_else(|| anyhow!("no `{kind}` connection found in {}", path.display()))
}

/// Resolve a connection's password spec the same way the TUI does.
async fn resolve(connection: &ConnectionConfig) -> anyhow::Result<Option<String>> {
    let (spec, account) = connection.secret_account();
    config::resolve_secret_async(spec, account).await
}
