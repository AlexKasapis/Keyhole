# Changelog

All notable changes to Keyhole are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

- **Headless mode**: the `keyhole record` and `keyhole export` subcommands are
  gone. Keyhole is now TUI-only (the hidden `gen` packaging helper remains).
  CSV export — previously available only through `keyhole export` — is removed;
  recordings are still written to JSONL and browsable in the in-app recordings
  viewer.
- The minimal static **musl** build. The `musl` release tarballs and the
  `KEYHOLE_INSTALL_FLAVOR=musl` installer option are no longer published; the
  glibc tarballs, `.deb`/`.rpm`, Nix, and `cargo install` channels remain.

### Changed

- Keyring, AMQP 1.0, and RabbitMQ are always built in. The optional `keyring`,
  `amqp`, and `rabbitmq` Cargo features (and the `--no-default-features` build)
  have been removed; only the `integration` test-gating feature remains.

## [0.1.0] - 2026-06-23

Initial release.

### Added

- **Redis** broker support: keyspace browser with an inline server-statistics
  band, value inspector, read-only command console, and realtime tails
  (pub/sub, pattern pub/sub, streams, keyspace notifications, and `MONITOR`).
- **AMQP 1.0** support (ActiveMQ / Amazon MQ / RabbitMQ 4.x): non-destructive
  topic and queue tailing, with `amqps://` TLS. Optional `amqp` feature.
- **RabbitMQ** (AMQP 0.9.1) support: non-destructive exchange taps via a
  temporary, auto-deleting bound queue; virtual hosts and `amqps://` TLS.
  Optional `rabbitmq` feature.
- **Recording**: record any live tail to a lossless JSONL file, toggleable
  while it runs; export a finished recording to CSV.
- **Headless mode**: `keyhole record` and `keyhole export` reuse the broker and
  recording stack without a terminal.
- Connection profiles stored in TOML with comment-preserving in-app edits;
  secrets are resolved at connect time via env var, OS keyring, or interactive
  prompt (never stored in plaintext).
- Help overlay, sidebar connection/keyspace tree, mouse support, `dark`/`light`
  theming with per-style overrides (honours `NO_COLOR`), and file-only logging.
- Community packaging definitions (see `packaging/`): a **Nix flake** —
  `nix run github:AlexKasapis/Keyhole` works off any tagged commit immediately —
  plus **AUR** PKGBUILDs (`keyhole` from source and `keyhole-bin` prebuilt) and a
  **Homebrew** formula. Each release also publishes the version-stamped,
  checksum-filled PKGBUILDs/.SRCINFO and the Homebrew formula as release assets.
- Distro-native packages: each release attaches a Debian/Ubuntu **`.deb`**
  and an openSUSE/Fedora **`.rpm`** (full glibc feature set, x86_64), so users can
  `apt`/`zypper`/`dnf install ./keyhole…` from a file without adding a repository.
  Both bundle the man page, completions, and licenses, auto-detect their glibc
  dependency floor, and recommend `gnome-keyring` for the OS-keyring backend; they
  are signed, checksummed, and provenance-attested alongside the tarballs. Defined
  in `[package.metadata.deb]` / `[package.metadata.generate-rpm]` in `Cargo.toml`.

### Security

- Release artifacts are signed (sigstore/cosign keyless), carry SLSA
  build-provenance attestations, and ship a CycloneDX SBOM plus an aggregate
  `SHA256SUMS`. The install script additionally verifies the cosign signature
  when `cosign` is available. See the README "Verifying a download" section.

[Unreleased]: https://github.com/AlexKasapis/Keyhole/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AlexKasapis/Keyhole/releases/tag/v0.1.0
