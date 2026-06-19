# BrokerTUI — Roadmap & Status

A terminal UI to connect to message/data brokers (**Redis** first, **AMQP** later),
browse their data, watch realtime activity, and **record live streams to disk**.
Single self-contained binary, usable locally or over SSH.

---

## 📍 Current status — you are here

**Phase 3 complete.** On top of v1 (browse + realtime + record), the app now has
the **deferred Redis features and polish**: a **keyspace-notification monitor**
and **MONITOR firehose** (both as recordable tails), a **read-only command
console** (deny-by-default allowlist + `COMMAND INFO` flag check), a **command
palette**, **config theming** (`[theme]` + `NO_COLOR`), and **mouse-wheel
scrolling**. **Phase 4 (AMQP) is next.**

| | |
|---|---|
| Branch | `main` |
| Commits | `a5c9e30` Phase 0 · `2d83f33` Phase 1 · `be6ecac` Phase 2 · Phase 3 (this commit) |
| Tests | 209 pass (198 unit/snapshot + 11 integration) |
| Quality gate | `clippy --all-targets --all-features -D warnings` clean · `clippy --no-default-features` clean · `rustfmt` clean |
| Verified | PTY harness: connect → console `PING`→`PONG` (live read-only exec) → MONITOR tail → command palette → clean exit; keyspace "disabled" banner with notifications off; all integration tests against Redis on 6380 |

### What works today
- Connect to Redis (saved TOML profiles or `--connect`), auto-reconnect, liveness ping → disconnect detection.
- **Browse**: non-blocking `SCAN` with `MATCH`/`COUNT` paging + pipelined `TYPE`/`TTL`; server-side filter (`/`); DB switching (`[`/`]`); load-more.
- **Value viewers** for every type: string (JSON pretty-print, binary→base64, size-capped), list, set, hash, zset, stream — auto-loaded into a detail pane as you move the selection.
- **Dashboard** from `INFO`: memory & hit-ratio gauges, version/uptime/clients/ops-sec, per-DB key counts (auto-refresh).
- **Realtime tails** (`w`): pub/sub (`SUBSCRIBE`/`PSUBSCRIBE`), stream (`XREAD BLOCK … $`), **keyspace notifications** (`PSUBSCRIBE __keyevent@db__:*`), and **`MONITOR`** — all on dedicated sockets, each a tab with a capped scrollback ring buffer. Start with `s` (spec prompt: `pubsub:ch` · `psub:ch.*` · `stream:key` · `keyspace[:N]` · `monitor`), `m` (MONITOR), `K` (keyspace), or `t` on a stream key. `Tab`/`[`/`]` switch tabs, `↑↓`/`G` scroll/follow, `x` stops a tail.
- **Keyspace monitor**: when a server has `notify-keyspace-events` disabled, the tab shows a non-fatal **banner** explaining it (brokertui never sets it — that's a write).
- **Read-only command console** (`e`): type a command (`i`), it's validated against a **deny-by-default allowlist** *and* the server's own `COMMAND INFO` flags (refusing `write`/`admin`/`blocking`/`pubsub`) before running; replies render redis-cli-style. History recall (`↑↓`), `r` clears.
- **Command palette** (`:`): fuzzy-filter and run any action (go to screen, subscribe, start monitor/keyspace, …).
- **Recording** (`r` on any tail): lossless append-only **JSONL** envelopes under the recordings dir, with live counters; the **Recordings** view (`R`) lists files. Keyspace/MONITOR tails record through the unchanged recorder.
- **Headless CLI**: `record --connect <p> --source <spec> --out <dir>` (Ctrl-C to stop, accepts `monitor`/`keyspace` specs) and `export <file.jsonl> --csv` — reuse the broker + recording stack with no UI.
- **Add-connection modal** (`a`): saves profiles to the config file; passwords stay as *specs* (`env:VAR`/`keyring`/`prompt`) or session-only — never written as plaintext.
- **Theming**: a `[theme]` config section (`dark`/`light` base + per-style colour overrides) plus `NO_COLOR` support (colourless, modifier-only palette).
- **Mouse**: scroll wheel navigates the focused list/pane (suppressed during text entry).
- Screen-based TUI (Connections / Browser / Dashboard / Realtime / Recordings / Console) + help overlay (`?`), vim + arrow keys, panic-safe terminal restore, file-only logging.

### Not done yet (Phase 4+)
AMQP broker (behind the `BrokerConnection` trait), write/destructive ops (publish, set, delete/purge), Redis cluster/sentinel, `cargo-dist` releases.

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
├─ theme.rs          # Theme styles + [theme]/NO_COLOR resolution (dark/light/plain)
├─ app/
│  ├─ mod.rs         # App: event handling, key dispatch, connect/browse/console/palette
│  ├─ state.rs       # Screen, InputMode, Connection, Console, PaletteState, Subscription
│  └─ action.rs      # normal-mode keymap → Action + command-palette table
├─ broker/
│  ├─ mod.rs         # ★ BrokerConnection trait (subscribe/tail_notice/exec_readonly)
│  │                 #   + shared types (SubSpec incl. Keyspace/Monitor, ValueView, …)
│  ├─ actor.rs       # ★ ConnCommand (incl. Exec), ConnHandle, spawn_connection(), loop
│  └─ redis/
│     ├─ mod.rs      # RedisConnection (impl trait) + integration tests
│     ├─ info.rs     # INFO parser → ServerStats (unit tested)
│     ├─ value.rs    # string→ValueView rendering (unit tested)
│     ├─ tail.rs     # pub/sub · stream · keyspace · MONITOR tails + pure mappers
│     └─ command.rs  # ★ read-only console: allowlist + COMMAND INFO + reply render
├─ config/
│  ├─ mod.rs         # Config / RedisProfile / Settings / ThemeConfig, load/save, Paths
│  └─ secret.rs      # SecretSpec: env → keyring → prompt
├─ recording/mod.rs  # Recorder + JSONL RecordSink + CSV export
└─ ui/
   ├─ mod.rs         # render(): header · screen dispatch · footer · overlays · snapshots
   └─ views/mod.rs   # connections · browser · dashboard · realtime · recordings · console · palette · help
```
★ = the keystone files to read first.

**Keys:** `q`/`Ctrl-c` quit · `j`/`k`/arrows move (mouse wheel too) · `g`/`G` top/bottom · `Ctrl-u`/`Ctrl-d` page · `Enter` connect · `a` add · `c`/`b`/`d`/`w`/`R`/`e` screens · `s` subscribe · `m` MONITOR · `K` keyspace · `i` console input · `:` palette · `/` filter · `[`/`]` DB/tab · `n` load more · `r` refresh/record/clear · `x` stop tail · `?` help · `Esc` dismiss.

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
- **Phase 3 — Monitoring & polish** ✅ — keyspace-notification monitor, `MONITOR` tail, read-only command console, command palette, config theming, mouse, snapshot UI tests, musl CI. See "delivered" below.
- **Phase 4 — AMQP** ⏭ NEXT: `AmqpConnection` behind the trait, browse exchanges/queues, **non-destructive tail** (temp exclusive queue bound to the exchange), record through the unchanged recorder. ⚠ See AMQP note below.
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

## ✅ Phase 3 — delivered

The deferred Redis features + UX polish, all reusing the Phase-1/2 plumbing.

1. **New tail kinds as `SubSpec` variants** — `Keyspace { db }` and `Monitor` join `Channel`/`Pattern`/`Stream` in `broker/mod.rs`. Because they're just specs, they flow through the *entire* existing path unchanged: dedicated socket, `BrokerEvent` stream, scrollback ring buffer, recorder (new `source_type` tags), CSV export, and headless `record`. `parse` accepts bare `monitor`/`keyspace` and `keyspace:N`.
2. **Redis tails** (`broker/redis/tail.rs`) — `open_keyspace` (`PSUBSCRIBE __keyevent@<db>__:*`; the channel suffix is the event, the message is the key) and `open_monitor` (`client.get_async_monitor().into_on_message::<String>()`). Pure `keyspace_event`/`monitor_event`/`parse_monitor_line` mappers are unit-tested.
3. **Keyspace "disabled" banner** — a new default-`None` trait method `BrokerConnection::tail_notice(&spec)`; the Redis impl runs `CONFIG GET notify-keyspace-events` and returns an advisory when empty. The actor emits `AppEvent::SubscriptionNotice`, stored on the `Subscription` and rendered as a banner. UI-only — never recorded. brokertui never enables notifications itself (that's a write).
4. **Read-only command console** (`broker/redis/command.rs`, `Screen::Console`) — two-layer safety: a **deny-by-default static allowlist** (`validate_readonly`, with a per-subcommand gate for `CONFIG`/`CLIENT`/`OBJECT`/…) *plus* `ensure_server_readonly` checking the server's own `COMMAND INFO` flags. New `BrokerConnection::exec_readonly`, `ConnCommand::Exec`, `AppEvent::CommandResult`; per-connection history + scrollback; replies rendered redis-cli-style (binary → base64). `tui-textarea` was **avoided** — single-line manual input like the connection form, so the ratatui-0.30 pin risk never materialised.
5. **Command palette** (`:`, `InputMode::Palette`) — substring filter over a static `(label, Action)` table in `app/action.rs`; `Enter` dispatches the chosen `Action`.
6. **Theming** (`src/theme.rs`, promoted out of `ui/`) — `Theme::from_config(&ThemeConfig, no_color)` with `dark`/`light` bases + per-style colour overrides (named/`#rrggbb`/indexed via `Color::from_str`); `NO_COLOR` yields a colourless, modifier-only palette. Built once at startup and stored on `App` (`Theme` is `Copy`).
7. **Mouse** — `EnableMouseCapture`/`DisableMouseCapture` in `tui.rs` (panic-hook safe); scroll-wheel events route through the existing `nav` logic (ignored during text entry; click selection intentionally not tracked — immediate-mode render keeps no hit-test map).
8. **Snapshot UI tests** — `insta` (dev-dep) renders key screens (connections/help/palette/console/dashboard/keyspace-notice) to `TestBackend` with a pinned clock + fixed data → `src/ui/snapshots/*.snap`. Regenerate with `INSTA_UPDATE=always cargo test`.
9. **musl / CI** — a new `musl` CI job builds `--release --no-default-features --target x86_64-unknown-linux-musl` and runs headless clippy; the `build` job now runs the integration suite (Redis service + `notify-keyspace-events KEA`). A `--no-default-features` dead-code gate on `KEYRING_SERVICE` was fixed.

**Decisions & gotchas (Phase 3):**
- **`SubSpec::target()` now returns `String`** (was `&str`) — `Keyspace`/`Monitor` have no backing string (`db0`/`all` are synthesised). The one call site (`recording_filename`) takes `&spec.target()`.
- **MONITOR is a firehose & server-wide** (all dbs, every command incl. our own liveness pings) — fine via the existing lossy-UI / lossless-recorder split; no `SELECT` needed.
- **The console runs on the actor's shared manager**, whose `db` follows the last browse `SELECT`. Acceptable for v1; revisit if per-console db isolation is wanted.
- **`COMMAND INFO` reports `CONFIG`/`CLIENT` as `admin`** as a whole, so the dynamic flag check is *skipped* for subcommand-bearing commands — their specific safe subcommands (`CONFIG GET`, `CLIENT LIST`, …) are gated by the static allowlist instead.
- **Mouse capture suppresses the terminal's native text selection** while the app runs — a known crossterm tradeoff for receiving scroll events.

---

## Gotchas & environment notes

- **This machine already runs a dev stack** (project `compose`): Redis on **6379**, an **ActiveMQ-based AMQP broker on 5672**, Postgres, DynamoDB, etc. Don't disturb it. BrokerTUI's own brokers use overridden host ports via a gitignored `.env` (Redis **6380**, RabbitMQ **5673**/**15673**).
- **⚠ AMQP (Phase 4):** the local AMQP broker is **ActiveMQ → AMQP 1.0**, not RabbitMQ's **0-9-1**. The `lapin` crate (AMQP 0-9-1) will *not* work against it. Confirm the real target with the user and pick an AMQP 1.0 client (or test against a genuine RabbitMQ) before building Phase 4.
- **Trimmed for now (YAGNI):** the `BrokerKind` tag and per-value paging `offset`s were removed to keep the lint gate clean; reintroduce in Phase 4 / when value paging lands.
- **Secrets:** never stored as plaintext. Form literals are used for the session only and persisted as a `prompt` spec. `keyring` v4 uses a pure-Rust zbus Secret Service backend (no system libdbus); it's behind a default-on `keyring` Cargo feature with an env-var fallback — build headless/musl with `--no-default-features`.
- **redis 1.2:** `ConnectionInfo` fields are private, so connections are built from a percent-encoded `redis://` URL.
- **Logging** goes to `~/.local/share/brokertui/logs/` only — never stdout/stderr (the TUI owns the terminal). Don't add `println!`/`eprintln!` in the running app.
