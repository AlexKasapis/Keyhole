<div align="center">

# 🔑 Keyhole

**A terminal UI for message & data brokers — browse, watch live, and record streams to disk.**

Redis · AMQP 1.0 · RabbitMQ — one self-contained binary.

[![Latest release](https://img.shields.io/github/v/release/AlexKasapis/Keyhole?label=release&color=success)](https://github.com/AlexKasapis/Keyhole/releases/latest)
[![Latest version](https://img.shields.io/github/v/tag/AlexKasapis/Keyhole?label=version&sort=semver)](https://github.com/AlexKasapis/Keyhole/tags)
[![CI](https://github.com/AlexKasapis/Keyhole/actions/workflows/ci.yml/badge.svg)](https://github.com/AlexKasapis/Keyhole/actions/workflows/ci.yml)
[![Audit](https://github.com/AlexKasapis/Keyhole/actions/workflows/audit.yml/badge.svg)](https://github.com/AlexKasapis/Keyhole/actions/workflows/audit.yml)
[![Coverage ≥90%](https://img.shields.io/badge/coverage-%E2%89%A590%25-success.svg)](.github/workflows/ci.yml)
[![Rust 1.95+](https://img.shields.io/badge/rust-1.95%2B-orange.svg)](rust-toolchain.toml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

</div>

Keyhole connects to **Redis**, **AMQP 1.0** (ActiveMQ / Amazon MQ / RabbitMQ 4.x),
and **RabbitMQ** (AMQP 0.9.1), lets you browse their data, watch realtime
activity, and record live streams to disk.

## Install

Just run the following in your terminal:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/AlexKasapis/Keyhole/releases/latest/download/keyhole-installer.sh | sh
```

Or build it yourself with Cargo:

```sh
cargo install keyhole                       # full build: keyring + AMQP + RabbitMQ
```

Other channels are covered under [Installation](#installation) below.

## Quick start

```sh
keyhole   # launch the TUI
```

## Supported brokers

| Capability                          | Redis        | AMQP 1.0          | RabbitMQ              |
|-------------------------------------|:------------:|:-----------------:|:---------------------:|
| Keyspace browser + value inspector  | ✅           | —                 | —                     |
| Read-only command console           | ✅           | —                 | —                     |
| Realtime tails                      | ✅           | ✅                | ✅                    |
| Record live tail → JSONL            | ✅           | ✅                | ✅                    |

All three brokers are always built in.

## Features

- **Connections** — saved connection profiles in TOML (`~/.config/keyhole/config.toml`).
- **Browser** (Redis) — navigate the keyspace and inspect values, with a live server-statistics band and a pinned, read-only command console.
- **Realtime** — live tails of pub/sub, pattern pub/sub, streams, keyspace events, and `MONITOR` (Redis); topic/queue tails (AMQP 1.0); and exchange taps (RabbitMQ).
- **Recording** — record any live tail to a lossless JSONL file, browsable in the in-app recordings viewer.

## Installation

Keyhole is a single self-contained binary. Pick the channel that fits your setup.

### Prebuilt binaries

The [install script](#install) above is the quickest route. A **glibc** binary
(keyring + AMQP + RabbitMQ) is published per architecture (x86_64 and aarch64).

You can also grab a tarball directly from the [Releases page].

### Nix

The repository is a flake, so you can run or install straight from GitHub
(requires flakes enabled):

```sh
nix run github:AlexKasapis/Keyhole               # try it without installing
nix profile install github:AlexKasapis/Keyhole   # install into your profile
```

### Distro packages (.deb / .rpm)

Each release attaches a Debian/Ubuntu `.deb` and an openSUSE/Fedora `.rpm` (full
**glibc** feature set, x86_64). Download from the [Releases page] and install
with your package manager so dependencies resolve:

```sh
sudo apt install ./keyhole_*_amd64.deb       # Debian / Ubuntu
sudo zypper install ./keyhole-*.x86_64.rpm   # openSUSE
sudo dnf install ./keyhole-*.x86_64.rpm      # Fedora
```

They bundle the man page, shell completions, and licenses. For other
architectures, use the prebuilt tarball or `cargo install`.

### Other package managers _(coming soon)_

Definitions live in [`packaging/`](packaging/):

- **Arch (AUR):** `keyhole` (from source) / `keyhole-bin` (prebuilt)
- **Homebrew (incl. Linuxbrew):** `brew install AlexKasapis/tap/keyhole`

### Verifying a download

Every release artifact ships with three independent ways to verify it:

```sh
# 1. Checksum — detects corruption (the install script does this for you).
sha256sum --ignore-missing -c SHA256SUMS

# 2. Signature — sigstore/cosign keyless; proves it was signed by this repo's workflow.
cosign verify-blob \
  --signature  keyhole-x86_64-unknown-linux-gnu.tar.gz.sig \
  --certificate keyhole-x86_64-unknown-linux-gnu.tar.gz.pem \
  --certificate-identity-regexp '^https://github\.com/AlexKasapis/Keyhole/\.github/workflows/release\.yml@refs/tags/v.*' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  keyhole-x86_64-unknown-linux-gnu.tar.gz

# 3. Build provenance — SLSA attestation tying the binary to the exact workflow run.
gh attestation verify keyhole-x86_64-unknown-linux-gnu.tar.gz --repo AlexKasapis/Keyhole
```

A signed [CycloneDX SBOM][cyclonedx] (`keyhole.cdx.json`) is published alongside.

[Releases page]: https://github.com/AlexKasapis/Keyhole/releases
[cyclonedx]: https://cyclonedx.org/

## Development

```sh
just setup          # install the pinned Rust toolchain
docker compose up -d redis
just run            # cargo run

just fmt            # rustfmt
just lint           # clippy -D warnings
just test           # unit + snapshot tests
just test-int       # integration tests against dockerized Redis + ActiveMQ + RabbitMQ
```

Logs are written to `~/.local/share/keyhole/logs/`.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full contributor workflow and
[`CHANGELOG.md`](CHANGELOG.md) for release notes.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at
your option.
