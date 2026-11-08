//! TLS connects from a fresh `keyhole` process. rustls panics building a TLS
//! config until a crypto provider is installed, and the unit and integration
//! tests share one process in which any earlier test may have installed it,
//! so only a new process shows whether a real entry point installs it.
//!
//! Runs `keyhole dev publish` with a `tls = true` profile against the compose
//! brokers' plain AMQP ports (`KEYHOLE_TEST_RABBITMQ_PORT`, default 5673;
//! `KEYHOLE_TEST_AMQP_PORT`, default 5674): the TLS handshake must fail with
//! an error and a non-zero exit, promptly, with no panic.
#![cfg(feature = "integration")]

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Long enough for a refused handshake; a hung connect takes forever.
const DEADLINE: Duration = Duration::from_secs(15);

fn port(var: &str, default: u16) -> u16 {
    std::env::var(var)
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(default)
}

/// Run `keyhole dev publish --broker <broker>` with `profile` as its only
/// connection, and return its exit code and output. Fails the test if it
/// runs past [`DEADLINE`].
fn dev_publish(broker: &str, profile: &str) -> (Option<i32>, String) {
    let config: PathBuf =
        std::env::temp_dir().join(format!("keyhole-tls-{broker}-{}.toml", std::process::id()));
    std::fs::write(&config, profile).expect("write the config");
    let mut child = Command::new(env!("CARGO_BIN_EXE_keyhole"))
        .args(["dev", "publish", "--broker", broker, "--config"])
        .arg(&config)
        .env("KEYHOLE_IT_TLS_PASSWORD", "keyhole")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run keyhole");
    let started = Instant::now();
    while child.try_wait().expect("poll keyhole").is_none() {
        if started.elapsed() > DEADLINE {
            child.kill().ok();
            let out = child.wait_with_output().expect("collect keyhole's output");
            std::fs::remove_file(&config).ok();
            panic!(
                "hung for {DEADLINE:?}:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let out = child.wait_with_output().expect("collect keyhole's output");
    std::fs::remove_file(&config).ok();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code(), text)
}

/// The run failed with an error of its own, not a panic.
fn assert_refused(code: Option<i32>, output: &str) {
    assert!(!output.contains("panicked"), "a panic:\n{output}");
    assert_eq!(code, Some(1), "an error exit:\n{output}");
    assert!(output.contains("Error: "), "an error message:\n{output}");
}

#[test]
fn a_tls_rabbitmq_profile_on_a_plain_port_fails_with_an_error() {
    let profile = format!(
        r#"[[connection]]
type = "rabbitmq"
name = "rabbitmq-tls"
host = "127.0.0.1"
port = {}
vhost = "/"
username = "keyhole"
password = "env:KEYHOLE_IT_TLS_PASSWORD"
tls = true
"#,
        port("KEYHOLE_TEST_RABBITMQ_PORT", 5673)
    );
    let (code, output) = dev_publish("rabbitmq", &profile);
    assert_refused(code, &output);
}

#[test]
fn a_tls_amqp_profile_on_a_plain_port_fails_with_an_error() {
    let profile = format!(
        r#"[[connection]]
type = "amqp"
name = "amqp-tls"
host = "127.0.0.1"
port = {}
tls = true
"#,
        port("KEYHOLE_TEST_AMQP_PORT", 5674)
    );
    let (code, output) = dev_publish("amqp", &profile);
    assert_refused(code, &output);
    assert!(output.contains("connecting to AMQP broker"), "{output}");
}
