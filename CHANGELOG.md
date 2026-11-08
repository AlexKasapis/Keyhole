# Changelog

All notable changes to Keyhole are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **RabbitMQ management API settings.** RabbitMQ profiles take
  `management_url`, `management_username` and `management_password`. The API
  defaults to `http://<host>:15672` (`https://<host>:15671` with `tls`) and the
  AMQP username and password; managed brokers such as Amazon MQ and CloudAMQP
  serve it on `https://<host>`, so set `management_url` for them.
  `management_password` takes `env:` or `keyring:`. If the API can't be
  reached, the connection still opens with exchange taps working, the browser
  says why, and `r` retries.
- **RabbitMQ queue and exchange browser.** The Browser lists the profile's
  vhost: queues, then exchanges, with their kind, messages (ready/unacked),
  consumers and publish rate. It loads on connect, refreshes every
  `browse_refresh_ms` and on `r`, and keeps the cursor on the same row.
- **RabbitMQ queue peek.** Enter on a queue reads up to 200 of its messages
  through the management API into the message inspector. In browse mode they
  go straight back on the queue, flagged redelivered; on RabbitMQ 3.x each
  peek counts toward a quorum queue's delivery limit, and the pane warns.
  Destructive mode removes them, and skip reads nothing. Stream queues can't
  be peeked, and the pane says what to do instead.
- **Exchange taps from the RabbitMQ list.** `t` or Enter taps a topic, fanout
  or headers exchange with `#`, and opens the Tail tab with `exchange:name/`
  for a direct exchange, to add the routing key.
- **RabbitMQ stats.** With the management API, the Server band shows the
  vhost and node alarms, and a Server Details tab shows versions, totals, node
  health and Publish/s and Deliver/s graphs.
- **RabbitMQ Connections and Consumers tabs**, listing the vhost's client
  connections and consumers.
- **Publish to RabbitMQ.** `P` publishes one message to the selected queue or
  exchange, with publisher confirms. An exchange first asks for the routing
  key, offering the last one used on it (fanout and headers exchanges skip
  it), and the footer reports a confirm, an unroutable message, a NACK or a
  refusal.
- **`keyhole dev` RabbitMQ demo queues.** The demo exchange now feeds durable
  queues `keyhole.demo.orders` (classic) and `keyhole.demo.audit` (quorum),
  capped at 1,000 messages, so the browser has queues to peek.

### Changed

- **Gruvbox is now the default theme**, applied when no `[theme]` `base` is set
  in the config. Existing profiles that pin `base = "dark"` or `"light"` are
  unaffected. The settings page's theme cycle now leads with gruvbox.
- **The settings page's "AMQP peek mode" row is now "Queue peek mode"**, since
  `peek_mode` covers RabbitMQ queues as well as AMQP 1.0.

### Fixed

- **RabbitMQ exchange taps can be started from the TUI again.** A RabbitMQ
  connection now opens on the Browser screen, whose Tail tab takes
  `exchange:name` or `exchange:name/binding-key`. Since the AMQP 1.0 browser
  landed, the tail panel lived only on the Browser screen, which RabbitMQ
  connections never reached.
- **AMQP 1.0 peek pane.** After selecting another queue, Enter and Ctrl-→
  could show the previous queue's messages under the new queue's name, and a
  failed peek stayed on "Peeking…". The pane now shows only the selected
  queue's peek, and a failed peek shows its error.
- **AMQP 1.0 publishing** sends to the destination selected when `P` was
  pressed, rather than whatever is selected when the message is submitted.
- **Closing an AMQP 1.0 or RabbitMQ tail tab** returns to the Tail tab.
  Before, Tab then skipped a tab, or stayed on the same one.
- **Long destination names end in `…` when cut**, in the AMQP 1.0 and RabbitMQ
  lists and in the right pane's titles, and a peeked queue's title keeps its
  message count and delivery-limit warning in view. Text is cut by what the
  terminal draws, so emoji and CJK names no longer lose the `…`.
- **The RabbitMQ list leaves out Keyhole's own tap queues.** The temporary
  `amq.gen-…` queues behind this session's exchange taps no longer show up in
  the list; other clients' queues still do.
- **RabbitMQ names and keys over 255 bytes no longer break the session.** A
  publish routing key, a profile's vhost, or an exchange name the management
  API listed, past AMQP's 255-byte limit, crashed a background task: the
  terminal dropped out of full-screen mode with a panic message, keys echoed
  on screen, and the connection, still shown as connected, did nothing more
  (a connect never finished). Now the publish fails in the footer, the tap's
  tab ends, or the connect fails, each saying the limit is 255 bytes, and an
  open connection stays up. The Tail tab also shows why it refused a typed
  spec, under its prompt, where the footer couldn't, and keeps the spec to
  fix.
- **RabbitMQ profiles with an IPv6 host connect to that host.** An address
  such as `2001:db8::1` was read as `localhost`, so Keyhole connected, and
  logged in with the profile's username and password, to a broker on this
  machine instead. A `.` or `..` vhost also connected to the empty vhost
  instead of the one named, and a host containing `/` fails to connect
  instead of crashing.

## [0.1.2] - 2026-06-26

### Changed

- **Add / edit connection form** now moves between fields with the **↑/↓ arrow
  keys** instead of Tab / Shift-Tab. ←/→ still toggle the focused TLS / Kind
  control, and the Tab keys are no longer bound in the form.

### Fixed

- **AMQP 1.0 connections without credentials** now negotiate SASL ANONYMOUS
  instead of attempting a bare (SASL-less) handshake. Brokers that require the
  SASL layer even for anonymous access — Apache ActiveMQ / Amazon MQ — rejected
  the bare handshake (`Expecting ProtocolHeader Amqp, found Sasl`), so a profile
  with no username/password could not connect. A credentialed profile is
  unchanged: the URL's userinfo still negotiates SASL PLAIN.

## [0.1.1] - 2026-06-23

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

[Unreleased]: https://github.com/AlexKasapis/Keyhole/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/AlexKasapis/Keyhole/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/AlexKasapis/Keyhole/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/AlexKasapis/Keyhole/releases/tag/v0.1.0
