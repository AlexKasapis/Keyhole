<div align="center">

# 🔑 Keyhole

**A terminal UI for message & data brokers — browse, watch live, and record streams to disk.**

Redis · AMQP 1.0 · RabbitMQ — one self-contained binary, fully read-only by design.

[![CI](https://github.com/AlexKasapis/Keyhole/actions/workflows/ci.yml/badge.svg)](https://github.com/AlexKasapis/Keyhole/actions/workflows/ci.yml)
[![Audit](https://github.com/AlexKasapis/Keyhole/actions/workflows/audit.yml/badge.svg)](https://github.com/AlexKasapis/Keyhole/actions/workflows/audit.yml)
[![Coverage ≥90%](https://img.shields.io/badge/coverage-%E2%89%A590%25-success.svg)](.github/workflows/ci.yml)
[![Rust 1.95+](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

<!--
  crates.io badge — uncomment once v0.1.0 is published:
  [![crates.io](https://img.shields.io/crates/v/keyhole.svg)](https://crates.io/crates/keyhole)

  Demo — drop an asciinema/terminalizer GIF or screenshot here once recorded, e.g.
  <p align="center"><img src="docs/demo.gif" alt="Keyhole in action" width="820"></p>
-->

</div>

Keyhole connects to **Redis**, **AMQP 1.0** (ActiveMQ / Amazon MQ / RabbitMQ 4.x),
and **RabbitMQ** (AMQP 0.9.1, all versions), lets you browse their data and watch
realtime activity, and **records live streams to disk** for later inspection.
Run it locally or over SSH — it's a single binary with no runtime to install.

**Every operation is read-only / non-destructive by design.** The command
console rejects writes, AMQP queues open in browse mode, topic subscriptions take
their own copy, and a RabbitMQ exchange is observed through a temporary bound
queue — observing never consumes or mutates your data.

> [!NOTE]
> The first tagged release (`v0.1.0`) is still in progress. `cargo install` and
> building from source work today; the prebuilt binaries, install script, and
> package-manager channels go live the moment the tag is published. Track the
> [Releases page] for status.

## Quick start

```sh
cargo install keyhole   # full build: Redis + AMQP + RabbitMQ + OS keyring
keyhole                 # launch the TUI
```

Press `?` for the help overlay, `Esc` to step back (Browser → Connections →
quit), or `Ctrl-C` to quit from anywhere. The footer always lists the keys for
the current screen.

## Supported brokers

| Capability                          | Redis        | AMQP 1.0          | RabbitMQ              |
|-------------------------------------|:------------:|:-----------------:|:---------------------:|
| Keyspace browser + value inspector  | ✅           | —                 | —                     |
| Read-only command console           | ✅           | —                 | —                     |
| Realtime tails                      | ✅           | ✅                | ✅                    |
| Record → JSONL · export → CSV       | ✅           | ✅                | ✅                    |
| Availability                        | always built in | `amqp` feature | `rabbitmq` feature |

<sub>AMQP 1.0 covers ActiveMQ / Amazon MQ / RabbitMQ 4.x. The RabbitMQ column is AMQP 0.9.1 (every RabbitMQ version). `amqps://` TLS is supported for both AMQP transports.</sub>

The broker is abstracted behind a trait, so each screen lights up only for the
brokers that support it.

## Features

### Brokers

- **Redis** — full support: keyspace browser with an inline server-statistics
  band, value inspector, read-only command console, and realtime tails.
  Multiplexed, auto-reconnecting connection with per-database `SELECT`.
- **AMQP 1.0** — read + record surface: non-destructively tail a **topic**
  (multicast — each subscriber gets a copy) or **queue** (browse mode — messages
  are read, not consumed). `amqps://` TLS for Amazon MQ. Optional `amqp` feature.
- **RabbitMQ** — read + record surface over AMQP 0.9.1: non-destructively tap an
  **exchange** (`exchange:name`, or `exchange:name/binding-key`) by binding a
  temporary, auto-deleting queue and consuming the copies routed to it — real
  queues and their consumers never lose a message. Virtual hosts and `amqps://`
  TLS supported. Reuses the same Realtime page as AMQP 1.0. Optional `rabbitmq`
  feature.

### Screens

- **Connections** — manage saved connection profiles, connect/disconnect.
- **Browser** — navigate the keyspace and inspect values, with a live
  server-statistics band (from `INFO`) atop the panes and an always-visible,
  read-only **command console** pinned along the bottom (Redis). Press `i` to
  type a command; output scrolls with PgUp/PgDn and clears with Ctrl-L.
- **Realtime** — live tails (see below).
- **Recordings** — browse on-disk `.jsonl` recordings.

### Realtime tails & recording

- Live tails of **pub/sub** (`pubsub:ch`), **pattern pub/sub** (`psub:ch.*`),
  and **streams** (`stream:key`); plus **keyspace event** notifications and a
  **MONITOR** command tail (Redis), **topic**/**queue** tails (AMQP 1.0), and
  **exchange** taps (`exchange:name`) (RabbitMQ).
- **Record** any live tail to a binary-safe **JSONL** file (lossless), toggleable
  on/off while it runs.
- **Export** a finished recording to **CSV** for spreadsheets.

### Connections & secrets

- Connection profiles stored in TOML (`~/.config/keyhole/config.toml`), edited
  in-app via a form modal with comment-preserving writes.
- Secrets are **never** stored in plaintext — a profile's password is a *spec*
  resolved at connect time, in order: **env var** (`env:VAR`) → **OS keyring**
  (`keyring`) → **interactive prompt** (`prompt`).
- `--connect PROFILE` to auto-connect on startup.

### Interface & polish

- **Single-key actions** — every action has a direct key binding, surfaced in
  the per-screen footer and the help overlay (`?`).
- **Help overlay**, sidebar connection/keyspace tree, and **mouse** support
  (scroll wheel).
- **Theming** — `dark`/`light` base plus per-style colour overrides in config;
  honours `NO_COLOR`.
- **File-only logging** (daily rolling, under the data dir) — the TUI owns the
  terminal, so logs never touch stdout/stderr.

### Headless mode

Run without a terminal, reusing the same broker + recording stack:

```sh
keyhole record --connect prod --source stream:events --out ./caps   # record until Ctrl-C
keyhole export caps/events-….jsonl --csv --out events.csv           # convert to CSV
```

## Installation

Keyhole is a single self-contained binary. Pick the channel that fits your setup.

### From crates.io

```sh
cargo install keyhole
```

This builds the full feature set (keyring + AMQP + RabbitMQ), which needs a C
toolchain and D-Bus / Secret Service headers at build time. For a minimal,
dependency-free build (Redis only, env-var secrets), add `--no-default-features`:

```sh
cargo install keyhole --no-default-features
```

### Prebuilt binaries

Download a tarball for your platform from the [Releases page], or use the
install script, which detects your OS/arch, verifies the SHA-256 checksum, and
installs `keyhole` (and its man page) into `~/.local/bin`:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/AlexKasapis/Keyhole/releases/latest/download/keyhole-installer.sh | sh
```

Two flavors are published per architecture (x86_64 and aarch64). The default is
the full **glibc** build (keyring + AMQP + RabbitMQ). For a dependency-free
**static** binary (Redis only, env-var secrets — ideal for headless/minimal
hosts), select the `musl` flavor:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/AlexKasapis/Keyhole/releases/latest/download/keyhole-installer.sh \
  | KEYHOLE_INSTALL_FLAVOR=musl sh
```

### Nix

The repository is a flake, so you can run or install straight from GitHub — no
tap or extra repo needed (builds the full feature set; requires flakes enabled):

```sh
nix run github:AlexKasapis/Keyhole               # try it without installing
nix profile install github:AlexKasapis/Keyhole   # install into your profile
```

### Distro packages (.deb / .rpm)

Each release attaches a Debian/Ubuntu `.deb` and an openSUSE/Fedora `.rpm` (both
the full **glibc** feature set, x86_64), so you can install from a file without
adding a repository. Download the package for your distro from the [Releases
page] and install it with your package manager so dependencies resolve:

```sh
# Debian / Ubuntu
sudo apt install ./keyhole_*_amd64.deb

# openSUSE
sudo zypper install ./keyhole-*.x86_64.rpm

# Fedora
sudo dnf install ./keyhole-*.x86_64.rpm
```

Both bundle the man page, shell completions, and licenses, and declare their
runtime dependencies (libc + a recommended `gnome-keyring` for the OS-keyring
secret backend). For other architectures, use the prebuilt tarball above or
`cargo install`.

### Other package managers _(coming soon)_

These reuse the same binaries/assets and go live as each channel is published;
the package definitions live in [`packaging/`](packaging/):

- **Arch (AUR):** `keyhole` (from source) / `keyhole-bin` (prebuilt)
- **Homebrew (incl. Linuxbrew):** `brew install AlexKasapis/tap/keyhole`

<details>
<summary><strong>Verifying a download</strong> — checksum, signature, and build provenance</summary>

<br>

Every release artifact ships with three independent ways to verify it, from
quickest to strongest:

```sh
# 1. Checksum — detects corruption/truncation (the install script does this for you).
sha256sum -c keyhole-x86_64-unknown-linux-gnu.tar.gz.sha256
#    …or against the aggregate file covering every artifact:
sha256sum --ignore-missing -c SHA256SUMS

# 2. Signature — sigstore/cosign keyless; proves it was signed by this repo's release workflow.
cosign verify-blob \
  --signature  keyhole-x86_64-unknown-linux-gnu.tar.gz.sig \
  --certificate keyhole-x86_64-unknown-linux-gnu.tar.gz.pem \
  --certificate-identity-regexp '^https://github\.com/AlexKasapis/Keyhole/\.github/workflows/release\.yml@refs/tags/v.*' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  keyhole-x86_64-unknown-linux-gnu.tar.gz

# 3. Build provenance — SLSA attestation tying the binary to the exact workflow run.
gh attestation verify keyhole-x86_64-unknown-linux-gnu.tar.gz --repo AlexKasapis/Keyhole
```

A [CycloneDX SBOM][cyclonedx] (`keyhole.cdx.json`) of the dependency graph is
published alongside, also signed.

</details>

[Releases page]: https://github.com/AlexKasapis/Keyhole/releases
[cyclonedx]: https://cyclonedx.org/

## Development

```sh
just setup          # install the pinned Rust toolchain
docker compose up -d redis
just run            # cargo run
```

Logs are written to `~/.local/share/keyhole/logs/`.

```sh
just fmt            # rustfmt
just lint           # clippy -D warnings
just test           # unit + snapshot tests
just test-int       # integration tests against dockerized Redis + ActiveMQ + RabbitMQ
```

<details>
<summary><strong>Build variants & feature flags</strong></summary>

<br>

```sh
just build-release  # optimized binary (default features: keyring + amqp + rabbitmq)
just build-musl     # static, headless binary (env-var secrets only; no keyring/amqp/rabbitmq)
```

| Feature    | Default | What it adds                                               |
|------------|:-------:|------------------------------------------------------------|
| `keyring`  |   on    | OS keyring backend for secret resolution                   |
| `amqp`     |   on    | AMQP 1.0 broker support (`fe2o3-amqp` + rustls for `amqps`) |
| `rabbitmq` |   on    | RabbitMQ / AMQP 0.9.1 support (`lapin` + rustls for `amqps`)|

Disable them all with `--no-default-features` for a minimal, statically-linkable
build (Redis is always included).

</details>

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full contributor workflow and
[`CHANGELOG.md`](CHANGELOG.md) for release notes.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at
your option.
