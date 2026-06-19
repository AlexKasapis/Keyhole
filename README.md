# BrokerTUI

A terminal UI to connect to message/data brokers — **Redis** and **AMQP 1.0**
(ActiveMQ / Amazon MQ / RabbitMQ 4.x) — to browse their data, watch realtime
activity, and **record live streams to disk** for later inspection. One
self-contained binary you can run locally or over SSH.

All operations are **read-only / non-destructive** by design: the command
console rejects writes, AMQP queues are opened in browse mode, and topic
subscriptions take their own copy — observing never consumes or mutates data.

## Features

### Brokers

- **Redis** — full support: keyspace browser, value inspector, server
  dashboard, read-only command console, and realtime tails. Multiplexed,
  auto-reconnecting connection with per-database `SELECT`.
- **AMQP 1.0** — read + record surface (ActiveMQ / Amazon MQ / RabbitMQ 4.x):
  non-destructively tail a **topic** (multicast — each subscriber gets a copy)
  or **queue** (browse mode — messages are read, not consumed). `amqps://` TLS
  supported for Amazon MQ. Optional at build time (`amqp` feature).

The broker is abstracted behind a trait, so each screen lights up only for the
brokers that support it.

### Screens

- **Connections** — manage saved connection profiles, connect/disconnect.
- **Browser** — navigate the keyspace and inspect values (Redis).
- **Dashboard** — live server statistics from `INFO` (Redis).
- **Realtime** — live tails (see below).
- **Recordings** — browse on-disk `.jsonl` recordings.
- **Console** — read-only command console (Redis).

### Realtime tails & recording

- Live tails of **pub/sub** (`pubsub:ch`), **pattern pub/sub** (`psub:ch.*`),
  and **streams** (`stream:key`); plus **keyspace event** notifications and a
  **MONITOR** command tail (Redis), and **topic**/**queue** tails (AMQP).
- **Record** any live tail to a binary-safe **JSONL** file (lossless), toggleable
  on/off while it runs.
- **Export** a finished recording to **CSV** for spreadsheets.

### Connections & secrets

- Connection profiles stored in TOML (`~/.config/brokertui/config.toml`), edited
  in-app via a form modal with comment-preserving writes.
- Secrets are **never** stored in plaintext — a profile's password is a *spec*
  resolved at connect time, in order: **env var** (`env:VAR`) → **OS keyring**
  (`keyring`) → **interactive prompt** (`prompt`).
- `--connect PROFILE` to auto-connect on startup.

### Interface & polish

- **Command palette** — fuzzy/substring launcher for every action.
- **Help overlay**, sidebar connection/keyspace tree, and **mouse** support
  (scroll wheel).
- **Theming** — `dark`/`light` base plus per-style colour overrides in config;
  honours `NO_COLOR`.
- **File-only logging** (daily rolling, under the data dir) — the TUI owns the
  terminal, so logs never touch stdout/stderr.

### Headless mode

Run without a terminal, reusing the same broker + recording stack:

```sh
brokertui record --connect prod --source stream:events --out ./caps   # record until Ctrl-C
brokertui export caps/events-….jsonl --csv --out events.csv           # convert to CSV
```

## Quick start

```sh
just setup          # install the pinned Rust toolchain
docker compose up -d redis
just run            # cargo run
```

Press `q` or `Ctrl-C` to quit. Open the command palette to reach any action.
Logs are written to `~/.local/share/brokertui/logs/`.

## Development

```sh
just fmt            # rustfmt
just lint           # clippy -D warnings
just test           # unit + snapshot tests
just test-int       # integration tests against dockerized Redis + ActiveMQ
```

## Build variants

```sh
just build-release  # optimized binary (default features: keyring + amqp)
just build-musl     # static, headless binary (env-var secrets only; no keyring/amqp)
```

| Feature   | Default | What it adds                                              |
|-----------|:-------:|-----------------------------------------------------------|
| `keyring` |   on    | OS keyring backend for secret resolution                  |
| `amqp`    |   on    | AMQP 1.0 broker support (`fe2o3-amqp` + rustls for `amqps`)|

Disable both with `--no-default-features` for a minimal, statically-linkable
build (Redis is always included).

## License

MIT OR Apache-2.0
