//! Configuration & filesystem paths.
//!
//! Connection profiles live in a TOML file (`~/.config/brokertui/config.toml`).
//! Secrets are never stored in plaintext — a profile's `password` is a *spec*
//! string (`env:VAR`, `keyring`, `prompt`) resolved by [`secret`].

mod secret;

pub use secret::{resolve as resolve_secret, SecretSpec};

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/// Top-level config document.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Saved connections (`[[connection]]` array of tables).
    #[serde(default, rename = "connection")]
    pub connections: Vec<ConnectionConfig>,
    /// Global settings.
    #[serde(default)]
    pub settings: Settings,
}

/// A saved connection, tagged by broker type (`type = "redis"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectionConfig {
    Redis(RedisProfile),
}

/// A Redis connection profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisProfile {
    pub name: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_redis_port")]
    pub port: u16,
    #[serde(default)]
    pub db: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Secret spec: `env:VAR`, `keyring[:account]`, `prompt`, or omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default)]
    pub tls: bool,
}

impl RedisProfile {
    /// Parse the profile's password field into a [`SecretSpec`].
    pub fn password_spec(&self) -> SecretSpec {
        SecretSpec::parse(self.password.as_deref().unwrap_or(""))
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_redis_port() -> u16 {
    6379
}

/// Global behavioural settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    /// `COUNT` hint for SCAN paging.
    #[serde(default = "default_scan_count")]
    pub scan_count: usize,
    /// Max bytes of a value to fetch for the viewer before truncating.
    #[serde(default = "default_value_preview_bytes")]
    pub value_preview_bytes: usize,
    /// Max events retained per live tail (scrollback ring buffer size).
    #[serde(default = "default_tail_scrollback")]
    pub tail_scrollback: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            scan_count: default_scan_count(),
            value_preview_bytes: default_value_preview_bytes(),
            tail_scrollback: default_tail_scrollback(),
        }
    }
}

fn default_scan_count() -> usize {
    500
}

fn default_value_preview_bytes() -> usize {
    64 * 1024
}

fn default_tail_scrollback() -> usize {
    2000
}

// ---------------------------------------------------------------------------
// Load / save
// ---------------------------------------------------------------------------

/// Load config from `path`, returning defaults if the file does not exist.
pub fn load(path: &Path) -> anyhow::Result<Config> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
    }
}

/// Write `config` to `path`, creating parent directories as needed.
pub fn save(path: &Path, config: &Config) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config directory {}", parent.display()))?;
    }
    let text = toml::to_string(config).context("serializing config")?;
    std::fs::write(path, text).with_context(|| format!("writing config {}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// Resolved application directories (XDG layout on Linux).
pub struct Paths {
    dirs: ProjectDirs,
}

/// Resolve platform directories for config, data, and logs.
pub fn paths() -> anyhow::Result<Paths> {
    let dirs = ProjectDirs::from("dev", "", "brokertui")
        .ok_or_else(|| anyhow!("could not determine a home directory for config/data"))?;
    Ok(Paths { dirs })
}

impl Paths {
    /// `~/.local/share/brokertui`
    pub fn data_dir(&self) -> &Path {
        self.dirs.data_dir()
    }

    /// `~/.config/brokertui/config.toml`
    pub fn config_file(&self) -> PathBuf {
        self.dirs.config_dir().join("config.toml")
    }

    /// `~/.local/share/brokertui/logs`
    pub fn log_dir(&self) -> PathBuf {
        self.data_dir().join("logs")
    }

    /// `~/.local/share/brokertui/recordings`
    pub fn recordings_dir(&self) -> PathBuf {
        self.data_dir().join("recordings")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_redis_connection() {
        let text = r#"
            [[connection]]
            type = "redis"
            name = "local"
            host = "127.0.0.1"
            port = 6380
            db = 2
            password = "env:REDIS_PW"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        assert_eq!(cfg.connections.len(), 1);
        let ConnectionConfig::Redis(profile) = &cfg.connections[0];
        assert_eq!(profile.name, "local");
        assert_eq!(profile.port, 6380);
        assert_eq!(profile.db, 2);
        assert_eq!(profile.password_spec(), SecretSpec::Env("REDIS_PW".into()));
        assert!(!profile.tls);
        // Defaults applied.
        assert_eq!(cfg.settings.scan_count, 500);
    }

    #[test]
    fn applies_defaults_for_minimal_profile() {
        let text = r#"
            [[connection]]
            type = "redis"
            name = "min"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        let ConnectionConfig::Redis(profile) = &cfg.connections[0];
        assert_eq!(profile.host, "127.0.0.1");
        assert_eq!(profile.port, 6379);
        assert_eq!(profile.db, 0);
        assert_eq!(profile.password_spec(), SecretSpec::None);
    }

    #[test]
    fn roundtrips_through_toml() {
        let cfg = Config {
            connections: vec![ConnectionConfig::Redis(RedisProfile {
                name: "prod".into(),
                host: "redis.example.com".into(),
                port: 6379,
                db: 1,
                username: Some("default".into()),
                password: Some("keyring".into()),
                tls: true,
            })],
            settings: Settings::default(),
        };
        let text = toml::to_string(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(back.connections.len(), 1);
        let ConnectionConfig::Redis(profile) = &back.connections[0];
        assert_eq!(profile.name, "prod");
        assert!(profile.tls);
        assert_eq!(profile.username.as_deref(), Some("default"));
        assert_eq!(profile.password_spec(), SecretSpec::Keyring(None));
    }

    #[test]
    fn load_missing_file_returns_default() {
        let cfg = load(Path::new("/nonexistent/brokertui/does-not-exist.toml")).unwrap();
        assert!(cfg.connections.is_empty());
    }
}
