# BrokerTUI — Roadmap & Status

A terminal UI to connect to message/data brokers (**Redis** first, **AMQP** later),
browse their data, watch realtime activity, and **record live streams to disk**.
Single self-contained binary, usable locally or over SSH.

---

## 📍 Current status — you are here

**Phase 1 complete and committed.** The app connects to Redis and is fully usable
for browsing. v1 = Phases 0–2; **Phase 2 (realtime + recording) is next.**

| | |
|---|---|
| Branch | `main` |
| Commits | `a5c9e30` Phase 0 scaffold · `2d83f33` Phase 1 (Redis browse + UI) |
| Tests | 18 pass (15 unit + 3 integration) |
| Quality gate | `clippy -D warnings` clean · `rustfmt` clean |
| Verified | End-to-end through a PTY harness (connect → browse → dashboard → quit) |

### What works today
- Connect to Redis (saved TOML profiles or `--connect`), auto-reconnect, liveness ping → disconnect detection.
- **Browse**: non-blocking `SCAN` with `MATCH`/`COUNT` paging + pipelined `TYPE`/`TTL`; server-side filter (`/`); DB switching (`[`/`]`); load-more.
- **Value viewers** for every type: string (JSON pretty-print, binary→base64, size-capped), list, set, hash, zset, stream — auto-loaded into a detail pane as you move the selection.
- **Dashboard** from `INFO`: memory & hit-ratio gauges, version/uptime/clients/ops-sec, per-DB key counts (auto-refresh).
- **Add-connection modal** (`a`): saves profiles to the config file; passwords stay as *specs* (`env:VAR`/`keyring`/`prompt`) or session-only — never written as plaintext.
- Screen-based TUI (Connections / Browser / Dashboard) + help overlay (`?`), vim + arrow keys, panic-safe terminal restore, file-only logging.

### Not done yet (Phase 2+)
Realtime tailing (pub/sub, streams), recording to disk, keyspace-notification monitor, read-only command console, AMQP, write/destructive ops.

---

## Locked decisions

| Decision | Choice |
|---|---|
| Language / UI | Rust + `ratatui` 0.30 + `crossterm` + `tokio` |
| Brokers | **Redis first (full)**, AMQP later behind one `BrokerConnection` trait |
| v1 mode | **Read + record only** — no publish/inject, no delete/purge/overwrite (deferred) |
| "Record" | Capture live streams to disk as **JSONL** with a metadata envelope (binary-safe) |
| Connections/secrets | TOML profiles in `~/.config/brokertui/config.toml`; secrets via env-var + OS keyring (no plaintext) |

Full original plan (not in repo): `~/.claude/plans/i-wanna-create-a-agile-curry.md`.

---

## Architecture

**Async↔UI contract:** a single render task owns all `App` state and only draws +
drains a bounded `mpsc<AppEvent>`. Background work (terminal input, tick, and one
**per-connection actor** per connection) lives in tokio tasks tracked by a
`TaskTracker` and stopped via a `CancellationToken`. Actors own the broker client
(keeping `!Sync` state off the render thread), serve `ConnCommand`s, and emit
results as `AppEvent`s. High-rate paths use `try_send` (lossy for the UI); the
recorder path (Phase 2) will be lossless.

```
src/
├─ main.rs           # tokio main · logging · terminal init/restore · the run loop
├─ cli.rs            # clap: --config, --connect, --log-level
├─ tui.rs            # raw mode + alt screen + panic-hook restore
├─ event.rs          # AppEvent enum + input & tick tasks
├─ logging.rs        # tracing → rolling file (never stdout)
├─ app/
│  ├─ mod.rs         # App: event handling, key dispatch, connect/browse/inspect flow
│  ├─ state.rs       # Screen, InputMode, Connection, ConnForm, Status
│  └─ action.rs      # normal-mode keymap → Action
├─ broker/
│  ├─ mod.rs         # ★ BrokerConnection trait + shared types (BrowseReq/Page,
│  │                 #   EntryMeta, ValueView, ServerStats, Ttl, Capabilities)
│  ├─ actor.rs       # ★ ConnCommand, ConnHandle, spawn_connection(), actor loop
│  └─ redis/
│     ├─ mod.rs      # RedisConnection (impl trait) + integration tests
│     ├─ info.rs     # INFO parser → ServerStats (unit tested)
│     └─ value.rs    # string→ValueView rendering (unit tested)
├─ config/
│  ├─ mod.rs         # Config / RedisProfile / Settings, load/save, Paths (XDG)
│  └─ secret.rs      # SecretSpec: env → keyring → prompt
├─ recording/mod.rs  # placeholder — Phase 2
└─ ui/
   ├─ mod.rs         # render(): header · screen dispatch · footer · overlays
   ├─ theme.rs       # Theme styles
   └─ views/mod.rs   # connections · browser · dashboard · conn_form · help
```
★ = the keystone files to read first.

**Keys:** `q`/`Ctrl-c` quit · `j`/`k`/arrows move · `g`/`G` top/bottom · `Ctrl-u`/`Ctrl-d` page · `Enter` connect (Connections) · `a` add · `c`/`b`/`d` screens · `/` filter · `[`/`]` DB · `n` load more · `r` refresh · `?` help · `Esc` dismiss.

---

## Build · run · test

Toolchain is pinned (`rust-toolchain.toml`, stable). A `justfile` wraps the common
commands; raw equivalents below in case `just` isn't installed.

```sh
# Local dev Redis (this machine's real stack uses 6379, so we bind 6380 via a
# gitignored .env — see BROKERTUI_REDIS_PORT). Seeds itself from the tests.
docker compose up -d redis            # -> 127.0.0.1:6380

cargo build
cargo run -- --config <file.toml> --connect <profile>   # or just `cargo run` and add a connection with `a`

cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test                            # unit tests (no broker needed)
cargo test --features integration     # needs Redis on $BROKERTUI_TEST_REDIS_PORT (default 6380)
```

**Verifying the TUI:** there is no TTY in an agent/CI shell, so the app can't be
run directly there. Use the PTY harness `scripts/tui_smoke.py` — it launches the
binary in a pseudo-terminal, sends a timed key script, and asserts on exit code,
alt-screen enter/leave, and expected on-screen text. Example:

```sh
cargo build
docker compose up -d redis
printf '[[connection]]\ntype="redis"\nname="local"\nhost="127.0.0.1"\nport=6380\ndb=0\n' > /tmp/bt.toml
python3 scripts/tui_smoke.py --cmd "./target/debug/brokertui --config /tmp/bt.toml --connect local" \
  --send 2.0:d --send 3.5:b --send 4.5:q \
  --expect "Connected to local" --expect Keys --expect Dashboard --expect Version
```
> Note: ratatui only redraws changed cells, so the captured byte stream can
> fragment a string that *is* visible on screen (a coincidental matching cell
> splits the redraw). Assert on freshly-drawn text, not on labels that merely
> changed from a previous frame.

---

## Phased roadmap

- **Phase 0 — Scaffold** ✅ (`a5c9e30`): event loop, logging, terminal lifecycle, tooling, CI.
- **Phase 1 — Redis browse** ✅ (`2d83f33`): config/secrets, `BrokerConnection` trait, per-connection actor, SCAN browser, value viewers, INFO dashboard, full TUI.
- **Phase 2 — Redis realtime + recording** ⏭ NEXT — completes v1. See below.
- **Phase 3 — Monitoring & polish**: keyspace-notification monitor (needs `notify-keyspace-events`), `MONITOR` tab, read-only command console (whitelist + `COMMAND INFO` flag check), command palette, theming from config, mouse, snapshot UI tests, musl build.
- **Phase 4 — AMQP**: `AmqpConnection` behind the trait, browse exchanges/queues, **non-destructive tail** (temp exclusive queue bound to the exchange), record through the unchanged recorder. ⚠ See AMQP note below.
- **Phase 5 — Mutations & scale-out**: write/destructive ops (publish, set, delete/purge) behind confirmations + a read-only lock; Redis cluster/sentinel; cargo-dist releases.

---

## ⏭ Phase 2 — concrete next steps

Goal: watch live broker activity and record it to disk. Reuse the actor + AppEvent plumbing.

1. **Shared event type** — add `BrokerEvent { ts, source, payload (Utf8|Binary|Json), meta }` to `broker/mod.rs`, and a `subscribe(spec) -> stream of BrokerEvent` method on `BrokerConnection`. The recorder and the UI both consume this same stream (so AMQP reuses it unchanged later).
2. **Redis tails** (dedicated sockets — blocking ops can't share the `ConnectionManager`):
   - Pub/Sub: `client.get_async_pubsub()` + `SUBSCRIBE`/`PSUBSCRIBE`.
   - Streams: a loop of `XREAD BLOCK <ms> COUNT <n> STREAMS <key> $`, advancing the last id.
   - Each spawns a task (tracked, cancellable per-tail) that maps replies → `BrokerEvent`.
3. **Actor/commands** — `ConnCommand::Subscribe { spec }` / `StopSubscription`; new `AppEvent::Realtime { sub, event }`, `SubscriptionStatus`, `RecordingStatus`.
4. **Recorder** (`recording/`): a `Recorder` + `RecordSink` writing append-only **JSONL** (`Record` envelope: ts, seq, connection, source type/name, encoding utf8|base64|json, payload, meta) under `~/.local/share/brokertui/recordings/`. `BufWriter`, flush on interval/size, optional size rotation, live counters. Lossless (awaited) channel — never drop while the UI view may.
5. **UI** — `PubSubTail` / `StreamTail` tabs with capped scrollback ring buffers; a Recordings view; `r` toggles recording on any tail.
6. **CLI** — `export <file.jsonl> --csv` and headless `record --connect <p> --source pubsub:foo --out <dir>` subcommands (`cli.rs`). These reuse the stack minus `ui/`.
7. **Likely refactor** — promote `broker` + `recording` into a small `lib.rs` (lib+bin) so the `record`/`export` binaries and `tests/` integration tests can link the crate. (Phase 1 integration tests currently live as in-crate `#[cfg(all(test, feature="integration"))]` modules.)
8. **Tests** — pub/sub round-trip, `XADD`→tail, recorder→valid JSONL; keep them self-seeding under unique key namespaces (deterministic + parallel-safe).

---

## Gotchas & environment notes

- **This machine already runs a dev stack** (project `compose`): Redis on **6379**, an **ActiveMQ-based AMQP broker on 5672**, Postgres, DynamoDB, etc. Don't disturb it. BrokerTUI's own brokers use overridden host ports via a gitignored `.env` (Redis **6380**, RabbitMQ **5673**/**15673**).
- **⚠ AMQP (Phase 4):** the local AMQP broker is **ActiveMQ → AMQP 1.0**, not RabbitMQ's **0-9-1**. The `lapin` crate (AMQP 0-9-1) will *not* work against it. Confirm the real target with the user and pick an AMQP 1.0 client (or test against a genuine RabbitMQ) before building Phase 4.
- **Trimmed for now (YAGNI):** the `BrokerKind` tag and per-value paging `offset`s were removed to keep the lint gate clean; reintroduce in Phase 4 / when value paging lands.
- **Secrets:** never stored as plaintext. Form literals are used for the session only and persisted as a `prompt` spec. `keyring` v4 uses a pure-Rust zbus Secret Service backend (no system libdbus); it's behind a default-on `keyring` Cargo feature with an env-var fallback — build headless/musl with `--no-default-features`.
- **redis 1.2:** `ConnectionInfo` fields are private, so connections are built from a percent-encoded `redis://` URL.
- **Logging** goes to `~/.local/share/brokertui/logs/` only — never stdout/stderr (the TUI owns the terminal). Don't add `println!`/`eprintln!` in the running app.
