//! Secret resolution: parse a spec string from a connection profile and resolve
//! it to an actual secret. Resolution order is **env var → OS keyring →
//! interactive prompt**. Plaintext secrets are intentionally not supported in
//! the config file. Keyring failures are surfaced as errors (handled by the UI),
//! never panics.

/// How a profile's secret should be obtained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretSpec {
    /// No secret.
    None,
    /// Read from the named environment variable.
    Env(String),
    /// Read from the OS keyring; optional account override (defaults to the
    /// profile name).
    Keyring(Option<String>),
    /// Ask the user interactively (handled by the UI at connect time).
    Prompt,
}

/// Service name used for all BrokerTUI keyring entries.
pub const KEYRING_SERVICE: &str = "brokertui";

impl SecretSpec {
    /// Parse a spec string: `env:VAR`, `keyring`, `keyring:account`, `prompt`,
    /// or empty for [`SecretSpec::None`]. Unknown forms are treated as
    /// [`SecretSpec::Prompt`] so a stray value is never used as a literal
    /// password.
    pub fn parse(spec: &str) -> Self {
        let spec = spec.trim();
        if spec.is_empty() {
            return SecretSpec::None;
        }
        if let Some(var) = spec.strip_prefix("env:") {
            return SecretSpec::Env(var.trim().to_string());
        }
        if spec == "keyring" {
            return SecretSpec::Keyring(None);
        }
        if let Some(account) = spec.strip_prefix("keyring:") {
            return SecretSpec::Keyring(Some(account.trim().to_string()));
        }
        if spec == "prompt" {
            return SecretSpec::Prompt;
        }
        SecretSpec::Prompt
    }
}

/// Resolve a secret to its value.
///
/// Returns `Ok(None)` when there is no secret, when it must be prompted for
/// (the caller handles the prompt), or when no keyring entry exists yet.
pub fn resolve(spec: &SecretSpec, account_hint: &str) -> anyhow::Result<Option<String>> {
    match spec {
        SecretSpec::None | SecretSpec::Prompt => Ok(None),
        SecretSpec::Env(var) => std::env::var(var)
            .map(Some)
            .map_err(|_| anyhow::anyhow!("environment variable `{var}` is not set")),
        SecretSpec::Keyring(account) => resolve_keyring(account.as_deref().unwrap_or(account_hint)),
    }
}

#[cfg(feature = "keyring")]
fn resolve_keyring(account: &str) -> anyhow::Result<Option<String>> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, account)
        .map_err(|e| anyhow::anyhow!("opening keyring entry for `{account}`: {e}"))?;
    match entry.get_password() {
        Ok(secret) => Ok(Some(secret)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(
            "reading keyring entry for `{account}`: {e}"
        )),
    }
}

#[cfg(not(feature = "keyring"))]
fn resolve_keyring(_account: &str) -> anyhow::Result<Option<String>> {
    anyhow::bail!("keyring support is not compiled in; use `env:VAR` or `prompt`")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_specs() {
        assert_eq!(SecretSpec::parse(""), SecretSpec::None);
        assert_eq!(SecretSpec::parse("  "), SecretSpec::None);
        assert_eq!(
            SecretSpec::parse("env:REDIS_PW"),
            SecretSpec::Env("REDIS_PW".into())
        );
        assert_eq!(SecretSpec::parse("keyring"), SecretSpec::Keyring(None));
        assert_eq!(
            SecretSpec::parse("keyring:prod"),
            SecretSpec::Keyring(Some("prod".into()))
        );
        assert_eq!(SecretSpec::parse("prompt"), SecretSpec::Prompt);
        // Unknown forms never become a literal password.
        assert_eq!(SecretSpec::parse("hunter2"), SecretSpec::Prompt);
    }

    #[test]
    fn resolves_env_var() {
        std::env::set_var("BROKERTUI_TEST_SECRET_ENV", "s3cr3t");
        let value = resolve(&SecretSpec::Env("BROKERTUI_TEST_SECRET_ENV".into()), "acct").unwrap();
        assert_eq!(value.as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn missing_env_var_errors() {
        let result = resolve(
            &SecretSpec::Env("BROKERTUI_TEST_DEFINITELY_UNSET".into()),
            "acct",
        );
        assert!(result.is_err());
    }

    #[test]
    fn none_and_prompt_resolve_to_no_value() {
        assert_eq!(resolve(&SecretSpec::None, "acct").unwrap(), None);
        assert_eq!(resolve(&SecretSpec::Prompt, "acct").unwrap(), None);
    }
}
