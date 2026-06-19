# BrokerTUI — Roadmap & Status

A terminal UI to connect to message/data brokers (**Redis** first, **AMQP** later),
browse their data, watch realtime activity, and **record live streams to disk**.
Single self-contained binary, usable locally or over SSH.

---

## 📍 Current status — you are here

**Phase 2 complete.** The app connects to Redis, browses, **tails live pub/sub &
streams, and records them to disk** — plus headless `record`/`export`.
v1 = Phases 0–2, so **v1 is feature-complete; Phase 3 (monitoring & polish) is next.**

| | |
|---|---|
| Branch | `main` |
| Commits | `a5c9e30` Phase 0 · `2d83f33` Phase 1 · Phase 2 (this commit) |
| Tests | 42 pass (35 unit + 7 integration) |
| Quality gate | `clippy --all-targets --all-features -D warnings` clean · `rustfmt` clean |
| Verified | PTY harness: connect → subscribe → live tail → record → recordings; pub/sub + stream tails; headless `record`/`export` against live Redis |

### What works today
- Connect to Redis (saved TOML profiles or `--connect`), auto-reconnect, liveness ping → disconnect detection.
- **Browse**: non-blocking `SCAN` with `MATCH`/`COUNT` paging + pipelined `TYPE`/`TTL`; server-side filter (`/`); DB switching (`[`/`]`); load-more.
- **Value viewers** for every type: string (JSON pretty-print, binary→base64, size-capped), list, set, hash, zset, stream — auto-loaded into a detail pane as you move the selection.
- **Dashboard** from `INFO`: memory & hit-ratio gauges, version/uptime/clients/ops-sec, per-DB key counts (auto-refresh).
- **Realtime tails** (`w`): pub/sub (`SUBSCRIBE`/`PSUBSCRIBE`) and stream (`XREAD BLOCK … $`) tails on dedicated sockets, each a tab with a capped scrollback ring buffer. Start with `s` (spec prompt: `pubsub:ch` · `psub:ch.*` · `stream:key`) or `t` on a stream key in the Browser. `Tab`/`[`/`]` switch tabs, `↑↓`/`G` scroll/follow, `x` stops a tail.
- **Recording** (`r` on any tail): lossless append-only **JSONL** envelopes under the recordings dir, with live counters; the **Recordings** view (`R`) lists files.
- **Headless CLI**: `record --connect <p> --source <spec> --out <dir>` (Ctrl-C to stop) and `export <file.jsonl> --csv` — reuse the broker + recording stack with no UI.
- **Add-connection modal** (`a`): saves profiles to the config file; passwords stay as *specs* (`env:VAR`/`keyring`/`prompt`) or session-only — never written as plaintext.
- Screen-based TUI (Connections / Browser / Dashboard / Realtime / Recordings) + help overlay (`?`), vim + arrow keys, panic-safe terminal restore, file-only logging.

### Not done yet (Phase 3+)
Keyspace-notification monitor, `MONITOR` tab, read-only command console, command palette, config theming, mouse, AMQP, write/destructive ops.

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
- **Phase 2 — Redis realtime + recording** ✅ — completes v1. See "delivered" below.
- **Phase 3 — Monitoring & polish** ⏭ NEXT: keyspace-notification monitor (needs `notify-keyspace-events`), `MONITOR` tab, read-only command console (whitelist + `COMMAND INFO` flag check), command palette, theming from config, mouse, snapshot UI tests, musl build.
- **Phase 4 — AMQP**: `AmqpConnection` behind the trait, browse exchanges/queues, **non-destructive tail** (temp exclusive queue bound to the exchange), record through the unchanged recorder. ⚠ See AMQP note below.
- **Phase 5 — Mutations & scale-out**: write/destructive ops (publish, set, delete/purge) behind confirmations + a read-only lock; Redis cluster/sentinel; cargo-dist releases.

---

## ✅ Phase 2 — delivered

Watch live broker activity and record it to disk, reusing the actor + AppEvent plumbing.

1. **Shared event type** — `BrokerEvent { ts, source, payload (Utf8|Json|Binary), meta }` + `Payload`/`SubSpec` in `broker/mod.rs`; `subscribe(&mut self, spec) -> BrokerEventStream` on `BrokerConnection`. The recorder and UI consume the same stream, so AMQP reuses it unchanged later.
2. **Redis tails** (`broker/redis/tail.rs`, dedicated sockets):
   - Pub/Sub: `get_async_pubsub()` + `SUBSCRIBE`/`PSUBSCRIBE`; `into_on_message()` owns the backing task so the stream stays alive on its own.
   - Streams: `futures::stream::unfold` over `XREAD BLOCK 5000 COUNT 100 STREAMS <key> $`, advancing `last_id`; pure `channel_event`/`stream_event` mappers are unit-tested.
3. **Actor/commands** — `ConnCommand::Subscribe`/`SetRecording`/`StopSubscription`; the actor is now stateful (a `sub_id → tail` registry) and spawns one tracked, per-tail-cancellable task each. `AppEvent::Realtime`/`SubscriptionStarted`/`SubscriptionEnded`/`RecordingUpdate`. UI forward is lossy (`try_send`); the recorder write is lossless (awaited in the tail task). Recording toggles live via a per-tail `watch<bool>`.
4. **Recorder** (`recording/`) — `Recorder<W: Write>` (generic for tests) + `RecordSink` (`BufWriter<File>`) writing append-only JSONL `Record` envelopes under the recordings dir; flush every 50 records and on a 2s interval; live counters; plus `export_csv`.
5. **UI** — Realtime screen with per-source tabs + capped scrollback ring buffers (follow/scroll), a Recordings view, `r` toggles recording on any tail. Start tails with `s` (spec prompt) or `t` (stream key in Browser).
6. **CLI** — `export <file.jsonl> --csv` and headless `record --connect <p> --source <spec> --out <dir>` (Ctrl-C to stop) in `cli.rs`/`main.rs`, reusing the stack minus `ui/`.
7. **Tests** — pub/sub round-trip, pattern→meta, `XADD`→stream tail, tail→valid JSONL (integration); `Payload`/`SubSpec` parsing, recorder JSONL + CSV, ring-buffer (unit). Self-seeding under unique namespaces (deterministic + parallel-safe).

**Decisions & gotchas (Phase 2):**
- **`subscribe` takes `&mut self`, not `&self`** — a `&mut dyn BrokerConnection` is `Send` (the trait is `Send`); a shared `&dyn` would require `Sync`, which the actor design deliberately avoids. The returned stream owns a *fresh* socket, so it's still `'static`.
- **⚠ `redis` response timeout vs `XREAD BLOCK`** — the async connection's `DEFAULT_RESPONSE_TIMEOUT` is **500ms**, which aborts any `XREAD BLOCK` that idles longer. Stream tails open with `AsyncConnectionConfig::set_response_timeout(None)`. (A fast-arriving test entry can hide this — the stream integration test deliberately delays its `XADD` past 500ms.)
- **`lib.rs` refactor skipped** (it was only "likely") — `record`/`export` are subcommands of the one binary and integration tests stay in-crate (`#[cfg(all(test, feature="integration"))]`), as in Phase 1. Revisit if external crates need to link the stack.
- **Stream payloads** are emitted as a JSON object of the entry's fields (order preserved); field values must be UTF-8 (same constraint as the inspect path). Pub/sub payloads are fully binary-safe (base64). New setting: `settings.tail_scrollback` (default 2000).

---

## Gotchas & environment notes

- **This machine already runs a dev stack** (project `compose`): Redis on **6379**, an **ActiveMQ-based AMQP broker on 5672**, Postgres, DynamoDB, etc. Don't disturb it. BrokerTUI's own brokers use overridden host ports via a gitignored `.env` (Redis **6380**, RabbitMQ **5673**/**15673**).
- **⚠ AMQP (Phase 4):** the local AMQP broker is **ActiveMQ → AMQP 1.0**, not RabbitMQ's **0-9-1**. The `lapin` crate (AMQP 0-9-1) will *not* work against it. Confirm the real target with the user and pick an AMQP 1.0 client (or test against a genuine RabbitMQ) before building Phase 4.
- **Trimmed for now (YAGNI):** the `BrokerKind` tag and per-value paging `offset`s were removed to keep the lint gate clean; reintroduce in Phase 4 / when value paging lands.
- **Secrets:** never stored as plaintext. Form literals are used for the session only and persisted as a `prompt` spec. `keyring` v4 uses a pure-Rust zbus Secret Service backend (no system libdbus); it's behind a default-on `keyring` Cargo feature with an env-var fallback — build headless/musl with `--no-default-features`.
- **redis 1.2:** `ConnectionInfo` fields are private, so connections are built from a percent-encoded `redis://` URL.
- **Logging** goes to `~/.local/share/brokertui/logs/` only — never stdout/stderr (the TUI owns the terminal). Don't add `println!`/`eprintln!` in the running app.
