# Keyhole

A terminal UI to connect to message/data brokers — **Redis**, **AMQP 1.0**
(ActiveMQ / Amazon MQ / RabbitMQ 4.x), and **RabbitMQ** (AMQP 0.9.1, all
versions) — to browse their data, watch realtime activity, and **record live
streams to disk** for later inspection. One self-contained binary you can run
locally or over SSH.

All operations are **read-only / non-destructive** by design: the command
console rejects writes, AMQP queues are opened in browse mode, topic
subscriptions take their own copy, and a RabbitMQ exchange is observed through a
temporary bound queue — observing never consumes or mutates data.

## Features

### Brokers

- **Redis** — full support: keyspace browser with an inline server-statistics
  band, value inspector, read-only command console, and realtime tails.
  Multiplexed, auto-reconnecting connection with per-database `SELECT`.
- **AMQP 1.0** — read + record surface (ActiveMQ / Amazon MQ / RabbitMQ 4.x):
  non-destructively tail a **topic** (multicast — each subscriber gets a copy)
  or **queue** (browse mode — messages are read, not consumed). `amqps://` TLS
  supported for Amazon MQ. Optional at build time (`amqp` feature).
- **RabbitMQ** — read + record surface over AMQP 0.9.1 (every RabbitMQ version):
  non-destructively tap an **exchange** (`exchange:name`, or
  `exchange:name/binding-key`) by binding a temporary, auto-deleting queue and
  consuming the copies routed to it — real queues and their consumers never lose
  a message. Virtual hosts and `amqps://` TLS supported. Reuses the same Realtime
  page as AMQP 1.0. Optional at build time (`rabbitmq` feature).

The broker is abstracted behind a trait, so each screen lights up only for the
brokers that support it.

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

Keyhole is a single self-contained binary. The first tagged release is still in
progress, so the channels marked _(coming soon)_ below are placeholders — track
the [Releases page] for status.

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

> The first tagged release is still in progress — these links go live once
> `v0.1.0` is published. Track the [Releases page] for status.

### Distro & package managers _(coming soon)_

- **Arch (AUR):** `keyhole` (from source) / `keyhole-bin` (prebuilt)
- **openSUSE / Fedora:** `zypper install ./keyhole.rpm` / `dnf install ./keyhole.rpm`
- **Debian / Ubuntu:** `apt install ./keyhole.deb`
- **Homebrew (incl. Linuxbrew):** `brew install AlexKasapis/tap/keyhole`
- **Nix:** `nix run github:AlexKasapis/Keyhole`

[Releases page]: https://github.com/AlexKasapis/Keyhole/releases

## Development quick start

```sh
just setup          # install the pinned Rust toolchain
docker compose up -d redis
just run            # cargo run
```

Press `Esc` to go back (Browser → Connections → quit), or `Ctrl-C` to quit
outright from anywhere. The footer lists the keys for the current screen, and
`?` opens a full help overlay.
Logs are written to `~/.local/share/keyhole/logs/`.

## Development

```sh
just fmt            # rustfmt
just lint           # clippy -D warnings
just test           # unit + snapshot tests
just test-int       # integration tests against dockerized Redis + ActiveMQ + RabbitMQ
```

## Build variants

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

## License

MIT OR Apache-2.0
