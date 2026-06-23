//! Configuration & filesystem paths.
//!
//! Connection profiles live in a TOML file (`~/.config/keyhole/config.toml`).
//! Secrets are never stored in plaintext — a profile's `password` is a *spec*
//! string (`env:VAR`, `keyring`, `prompt`) resolved by [`secret`].

mod secret;

pub use secret::{resolve_async as resolve_secret_async, SecretSpec};

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
    /// UI theme (`[theme]`).
    #[serde(default)]
    pub theme: ThemeConfig,
}

/// UI theme configuration (`[theme]`): a built-in base palette plus optional
/// per-style colour overrides. Each colour is a ratatui colour string — a name
/// (`cyan`), `#rrggbb` hex, or a `0`..`255` 256-colour index. Invalid values are
/// ignored. `NO_COLOR` (env) overrides everything with a colourless palette.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThemeConfig {
    /// Base palette: `dark` (default) or `light`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heading: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub border: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub border_focused: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gauge: Option<String>,
}

/// A saved connection, tagged by broker type (`type = "redis"` / `"amqp"` /
/// `"rabbitmq"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectionConfig {
    Redis(RedisProfile),
    Amqp(AmqpProfile),
    Rabbitmq(RabbitmqProfile),
}

impl ConnectionConfig {
    /// The connection's display name.
    pub fn name(&self) -> &str {
        match self {
            ConnectionConfig::Redis(p) => &p.name,
            ConnectionConfig::Amqp(p) => &p.name,
            ConnectionConfig::Rabbitmq(p) => &p.name,
        }
    }

    /// Lowercase broker-kind tag for the connections list.
    pub fn kind_label(&self) -> &'static str {
        match self {
            ConnectionConfig::Redis(_) => "redis",
            ConnectionConfig::Amqp(_) => "amqp",
            ConnectionConfig::Rabbitmq(_) => "rabbitmq",
        }
    }

    /// The configured login username, if any — shown as a `user@` prefix on the
    /// endpoint in the connections list.
    pub fn username(&self) -> Option<&str> {
        match self {
            ConnectionConfig::Redis(p) => p.username.as_deref(),
            ConnectionConfig::Amqp(p) => p.username.as_deref(),
            ConnectionConfig::Rabbitmq(p) => p.username.as_deref(),
        }
    }

    /// The bare `host:port` endpoint, without the db/vhost/tls trimmings — shown
    /// in the Browser's Server band (the db rides there as its own field).
    pub fn address(&self) -> String {
        match self {
            ConnectionConfig::Redis(p) => format!("{}:{}", p.host, p.port),
            ConnectionConfig::Amqp(p) => format!("{}:{}", p.host, p.port),
            ConnectionConfig::Rabbitmq(p) => format!("{}:{}", p.host, p.port),
        }
    }

    /// A `host:port[/db|/vhost][ tls]` summary for the connections list.
    pub fn endpoint(&self) -> String {
        match self {
            ConnectionConfig::Redis(p) => {
                let db = if p.db > 0 {
                    format!("/{}", p.db)
                } else {
                    String::new()
                };
                let tls = if p.tls { " tls" } else { "" };
                format!("{}:{}{db}{tls}", p.host, p.port)
            }
            ConnectionConfig::Amqp(p) => {
                let tls = if p.tls { " tls" } else { "" };
                format!("{}:{}{tls}", p.host, p.port)
            }
            ConnectionConfig::Rabbitmq(p) => {
                // Show the vhost only when it isn't the default "/".
                let vhost = if p.vhost == "/" {
                    String::new()
                } else {
                    format!("/{}", p.vhost)
                };
                let tls = if p.tls { " tls" } else { "" };
                format!("{}:{}{vhost}{tls}", p.host, p.port)
            }
        }
    }

    /// The broker kind this profile connects to.
    ///
    /// Exercised only by tests now that the headless `record` command (its sole
    /// caller) is gone; retained for the pending TUI realtime rework.
    #[allow(dead_code)]
    pub fn broker_kind(&self) -> crate::broker::BrokerKind {
        use crate::broker::BrokerKind;
        match self {
            ConnectionConfig::Redis(_) => BrokerKind::Redis,
            ConnectionConfig::Amqp(_) => BrokerKind::Amqp,
            ConnectionConfig::Rabbitmq(_) => BrokerKind::Rabbitmq,
        }
    }

    /// The `(secret spec, keyring account)` pair for resolving this connection's
    /// password. The account is the profile name. The per-variant match lives
    /// in exactly one place.
    pub fn secret_account(&self) -> (SecretSpec, String) {
        match self {
            ConnectionConfig::Redis(p) => (p.password_spec(), p.name.clone()),
            ConnectionConfig::Amqp(p) => (p.password_spec(), p.name.clone()),
            ConnectionConfig::Rabbitmq(p) => (p.password_spec(), p.name.clone()),
        }
    }
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

/// An AMQP 1.0 connection profile (ActiveMQ / Amazon MQ / RabbitMQ 4.x). The URL
/// is `amqp[s]://[user:pass@]host:port`; `tls` selects `amqps://` (Amazon MQ's
/// :5671 endpoint). Secrets follow the same spec rules as Redis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpProfile {
    pub name: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_amqp_port")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Secret spec: `env:VAR`, `keyring[:account]`, `prompt`, or omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default)]
    pub tls: bool,
}

impl AmqpProfile {
    /// Parse the profile's password field into a [`SecretSpec`].
    pub fn password_spec(&self) -> SecretSpec {
        SecretSpec::parse(self.password.as_deref().unwrap_or(""))
    }
}

/// A RabbitMQ (AMQP 0.9.1) connection profile. The URL is
/// `amqp[s]://[user:pass@]host:port/vhost`; `tls` selects `amqps://` (RabbitMQ's
/// :5671 TLS listener). `vhost` is the AMQP virtual host (default `/`). Secrets
/// follow the same spec rules as Redis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitmqProfile {
    pub name: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_amqp_port")]
    pub port: u16,
    /// AMQP virtual host (default `/`).
    #[serde(default = "default_vhost")]
    pub vhost: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Secret spec: `env:VAR`, `keyring[:account]`, `prompt`, or omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default)]
    pub tls: bool,
}

impl RabbitmqProfile {
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

fn default_amqp_port() -> u16 {
    5672
}

fn default_vhost() -> String {
    "/".to_string()
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
    /// How often the key browser re-scans the keyspace, in milliseconds, so
    /// keys added or removed in the server show up without a manual refresh.
    /// The refresh is independent of navigation. `0` disables auto-refresh.
    #[serde(default = "default_browse_refresh_ms")]
    pub browse_refresh_ms: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            scan_count: default_scan_count(),
            value_preview_bytes: default_value_preview_bytes(),
            tail_scrollback: default_tail_scrollback(),
            browse_refresh_ms: default_browse_refresh_ms(),
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

fn default_browse_refresh_ms() -> u64 {
    5000
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
    let dirs = ProjectDirs::from("dev", "", "keyhole")
        .ok_or_else(|| anyhow!("could not determine a home directory for config/data"))?;
    Ok(Paths { dirs })
}

impl Paths {
    /// `~/.local/share/keyhole`
    pub fn data_dir(&self) -> &Path {
        self.dirs.data_dir()
    }

    /// `~/.config/keyhole/config.toml`
    pub fn config_file(&self) -> PathBuf {
        self.dirs.config_dir().join("config.toml")
    }

    /// `~/.local/share/keyhole/logs`
    pub fn log_dir(&self) -> PathBuf {
        self.data_dir().join("logs")
    }

    /// `~/.local/share/keyhole/recordings`
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
        let ConnectionConfig::Redis(profile) = &cfg.connections[0] else {
            panic!("expected a redis profile");
        };
        assert_eq!(profile.name, "local");
        assert_eq!(profile.port, 6380);
        assert_eq!(profile.db, 2);
        assert_eq!(profile.password_spec(), SecretSpec::Env("REDIS_PW".into()));
        assert!(!profile.tls);
        // Defaults applied.
        assert_eq!(cfg.settings.scan_count, 500);
        assert_eq!(cfg.settings.browse_refresh_ms, 5000);
    }

    #[test]
    fn parses_browse_refresh_interval_override() {
        let text = r#"
            [settings]
            browse_refresh_ms = 1500
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        assert_eq!(cfg.settings.browse_refresh_ms, 1500);
        // Other settings still fall back to their defaults.
        assert_eq!(cfg.settings.scan_count, 500);
    }

    #[test]
    fn parses_amqp_connection() {
        let text = r#"
            [[connection]]
            type = "amqp"
            name = "aws-mq"
            host = "b-x.mq.eu-west-1.amazonaws.com"
            port = 5671
            username = "admin"
            password = "env:MQ_PW"
            tls = true
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        assert_eq!(cfg.connections.len(), 1);
        let ConnectionConfig::Amqp(p) = &cfg.connections[0] else {
            panic!("expected an amqp profile");
        };
        assert_eq!(p.name, "aws-mq");
        assert_eq!(p.port, 5671);
        assert!(p.tls);
        assert_eq!(p.password_spec(), SecretSpec::Env("MQ_PW".into()));
        assert_eq!(cfg.connections[0].kind_label(), "amqp");
        assert_eq!(
            cfg.connections[0].endpoint(),
            "b-x.mq.eu-west-1.amazonaws.com:5671 tls"
        );
    }

    #[test]
    fn parses_rabbitmq_connection() {
        let text = r#"
            [[connection]]
            type = "rabbitmq"
            name = "rabbit"
            host = "rabbit.example.com"
            port = 5671
            vhost = "prod"
            username = "app"
            password = "env:RABBIT_PW"
            tls = true
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        assert_eq!(cfg.connections.len(), 1);
        let ConnectionConfig::Rabbitmq(p) = &cfg.connections[0] else {
            panic!("expected a rabbitmq profile");
        };
        assert_eq!(p.name, "rabbit");
        assert_eq!(p.port, 5671);
        assert_eq!(p.vhost, "prod");
        assert!(p.tls);
        assert_eq!(p.password_spec(), SecretSpec::Env("RABBIT_PW".into()));
        assert_eq!(cfg.connections[0].kind_label(), "rabbitmq");
        // A non-default vhost is shown in the endpoint summary.
        assert_eq!(
            cfg.connections[0].endpoint(),
            "rabbit.example.com:5671/prod tls"
        );
    }

    #[test]
    fn applies_defaults_for_minimal_rabbitmq_profile() {
        let text = r#"
            [[connection]]
            type = "rabbitmq"
            name = "local"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        let ConnectionConfig::Rabbitmq(p) = &cfg.connections[0] else {
            panic!("expected a rabbitmq profile");
        };
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, 5672, "RabbitMQ defaults to 5672");
        assert_eq!(p.vhost, "/", "vhost defaults to /");
        assert!(!p.tls);
        assert_eq!(p.password_spec(), SecretSpec::None);
        // The default vhost is omitted from the endpoint summary.
        assert_eq!(cfg.connections[0].endpoint(), "127.0.0.1:5672");
    }

    #[test]
    fn applies_defaults_for_minimal_amqp_profile() {
        let text = r#"
            [[connection]]
            type = "amqp"
            name = "local"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        let ConnectionConfig::Amqp(p) = &cfg.connections[0] else {
            panic!("expected an amqp profile");
        };
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, 5672, "AMQP defaults to 5672");
        assert!(!p.tls);
        assert_eq!(p.password_spec(), SecretSpec::None);
    }

    #[test]
    fn applies_defaults_for_minimal_profile() {
        let text = r#"
            [[connection]]
            type = "redis"
            name = "min"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        let ConnectionConfig::Redis(profile) = &cfg.connections[0] else {
            panic!("expected a redis profile");
        };
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
            theme: ThemeConfig::default(),
        };
        let text = toml::to_string(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(back.connections.len(), 1);
        let ConnectionConfig::Redis(profile) = &back.connections[0] else {
            panic!("expected a redis profile");
        };
        assert_eq!(profile.name, "prod");
        assert!(profile.tls);
        assert_eq!(profile.username.as_deref(), Some("default"));
        assert_eq!(profile.password_spec(), SecretSpec::Keyring(None));
    }

    #[test]
    fn parses_theme_section_and_defaults_to_empty() {
        let text = r##"
            [[connection]]
            type = "redis"
            name = "x"

            [theme]
            base = "light"
            accent = "#ff8800"
        "##;
        let cfg: Config = toml::from_str(text).unwrap();
        assert_eq!(cfg.theme.base.as_deref(), Some("light"));
        assert_eq!(cfg.theme.accent.as_deref(), Some("#ff8800"));
        assert_eq!(cfg.theme.error, None);

        // Absent [theme] yields an all-None default.
        let minimal: Config = toml::from_str("").unwrap();
        assert!(minimal.theme.base.is_none());
    }

    #[test]
    fn username_accessor_reads_each_variant() {
        let text = r#"
            [[connection]]
            type = "redis"
            name = "r"
            username = "default"

            [[connection]]
            type = "amqp"
            name = "a"

            [[connection]]
            type = "rabbitmq"
            name = "rmq"
            username = "app"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        assert_eq!(cfg.connections[0].username(), Some("default"));
        assert_eq!(
            cfg.connections[1].username(),
            None,
            "no username configured"
        );
        assert_eq!(cfg.connections[2].username(), Some("app"));
    }

    #[test]
    fn load_missing_file_returns_default() {
        let cfg = load(Path::new("/nonexistent/keyhole/does-not-exist.toml")).unwrap();
        assert!(cfg.connections.is_empty());
    }
}
