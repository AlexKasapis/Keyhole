# Keyhole — Complete Code Review

## Context

You asked for a full, clean-code-focused review of the repo: high-level architecture
first, then implementation details per component, plus a rigorous assessment of whether
the tests give real value. Keyhole is a ~26k-line Rust TUI (ratatui + tokio) for
connecting to message/data brokers (Redis, AMQP 1.0, RabbitMQ), browsing data, watching
realtime activity, and recording streams to disk.

**Methodology.** I read the architectural spine in full (`main.rs`, `app.rs`, `event.rs`,
`broker.rs`, `broker/actor.rs`, `config.rs`, `config/secret.rs`, `recording.rs`) and ran
focused deep-dives on the UI layer, the app state machine/input layer, the non-Redis
brokers, and the test suite. I cross-checked every structural claim against the source —
and corrected two that didn't hold up (noted inline). Findings below are scoped honestly:
**nothing here is a correctness or security bug** — the issues are maintainability/clarity.

---

## Verdict

**This is a high-quality, well-tested codebase — clearly above average for a Rust app of
this size.** The architecture is sound, error handling is disciplined, and the test suite
is large and mostly high-value. The cleanup opportunities are real but small, and one
*theme* dominates them: the repo is visibly mid-refactor, carrying **vestigial code (and
tests for it)** left behind by a removed headless `record` command and a deferred
AMQP/RabbitMQ browser rework.

Overall grade: **A− / 8.5 of 10** on cleanliness. The single highest-leverage improvement
is removing/quarantining the vestigial code and the false test coverage it creates.

**Headline strengths**
- Clean actor + message-passing architecture; UI state has a single owner, no locks.
- Production code is essentially `unwrap`/`expect`/`panic`-free — everything is
  `anyhow::Result` surfaced to the UI. (All the `unwrap`s a naive grep finds are in
  `#[cfg(test)]` / integration-gated modules.)
- No `unsafe`, no `TODO`/`FIXME`/`HACK`, no clippy suppressions beyond a few justified
  `too_many_arguments`/`dead_code`.
- Security-conscious secret handling (no plaintext, env→keyring→prompt, keyring off the
  async runtime).
- 489 test functions, including a real `MockBroker` harness and integration tests gated
  behind a `feature = "integration"` flag.

**Headline weaknesses (all maintainability, not correctness)**
- Vestigial/dead code from the removed `record` command + deferred rework, plus tests
  that exercise it (false coverage).
- A meaningful slice of `app/tests.rs` reaches into private fields (brittle to refactor).
- Very dense doc-comments — a strength, but several reference removed features and will rot.
- Minor duplication (text-entry key handlers, scroll-clamp) and a couple of dual-boolean
  states that should be enums.

---

## 1. High-Level Architecture — *excellent*

The core design is the best part of the codebase.

**Render-loop / actor / event model** (`main.rs`, `event.rs`, `broker/actor.rs`)
- A single render loop owns `App` and is the *only* writer of UI state, so there is **no
  locking** anywhere in the UI path (`app.rs:1`). This is the right call for a TUI and is
  rigorously upheld.
- Background work talks to the loop only through a **bounded** `mpsc<AppEvent>` channel
  (capacity 1024) — a firehose applies backpressure rather than exhausting memory
  (`main.rs:41`).
- Each connection runs as an **actor task** holding a `Box<dyn BrokerConnection>` and
  serving `ConnCommand`s; subscriptions/tails each get their own cancellable task and
  dedicated socket (`broker/actor.rs:1-10`). This keeps `!Sync` client state off the
  render thread and makes the whole app broker-agnostic.
- Two nice performance details: the loop **coalesces** a burst of queued events into one
  repaint and holds repaints to a 16 ms frame budget (`main.rs:157-191`), and the UI feed
  is **lossy** (`try_send`) while the recorder is **lossless** (awaited write) — the right
  trade-off, and explicitly documented (`broker/actor.rs:324-330`).

**Broker abstraction** (`broker.rs`)
- `BrokerConnection` is a capability-gated trait: optional operations
  (`browse`/`inspect`/`stats`/`peek`/`publish`/`exec_readonly`) have default `bail!`
  implementations, and a `Capabilities` struct drives which UI screens appear
  (`broker.rs:730-789`). A broker only implements what it supports; the UI never offers a
  screen a broker can't serve. This is textbook trait design.

**Module layout.** `app` (state machine), `broker` (connections + actor), `ui` (pure
rendering), `config`/`recording`/`event`/`theme` (support). `impl App` is split across
submodules to keep `app.rs` a thin spine — a reasonable way to manage a large state
machine.

**Architectural tension worth noting (minor):** the render path performs a few *writes* —
viewport scroll-offset clamping that depends on the rendered area height
(`ui/views.rs` ~290/385). It's documented as the only state the renderer mutates and is a
defensible trade-off (only render knows the height), but it does mean the "all mutation
happens before render" invariant has two narrow exceptions. Acceptable; just be aware.

---

## 2. Component-by-Component

### Render loop & `main` — *clean*
Tight, well-commented entry point. `gen` (packaging) and `dev` (fake-data) subcommands are
dispatched *before* logging/terminal setup so they run in minimal environments
(`main.rs:56-68`). Graceful shutdown cancels tasks and waits on a `TaskTracker`. The
`frame_wait` helper is pure and unit-tested.

### `event.rs` — *clean*
A well-modeled `AppEvent` enum (the entire async↔UI vocabulary) plus two tiny spawned
tasks (input, tick) that own no state. Easy to read.

### Broker actor (`broker/actor.rs`) — *exemplary*
The actor loop, the per-tail recording lifecycle (open/flush/progress/close on timers),
and teardown-on-cancel are all clean. The `unreachable!` for subscription commands in
`process()` documents an invariant rather than silently no-op'ing (`broker/actor.rs:485`).
Its in-module `MockBroker` test harness is genuinely good (see §4).

### Broker implementations — *clean, no correctness issues found*
- **Redis** (`broker/redis.rs` + `redis/{command,tail,value,info}.rs`): production code
  has **zero** `unwrap`/`expect` (the 51 a grep finds are all inside
  `#[cfg(all(test, feature = "integration"))] mod integration_tests`). Nicely decomposed
  into submodules.
- **AMQP 1.0 / RabbitMQ** (`broker/amqp.rs`, `broker/rabbitmq.rs`): consistent trait
  impls; dedicated sockets for tails/peeks; RAII cleanup via `TailState`/`TapState` (drop
  closes link/session/connection). The **non-destructive guarantees are enforced**, not
  assumed: AMQP validates the broker negotiated `distribution_mode: copy` and refuses the
  tail otherwise; RabbitMQ taps via an `exclusive`, `auto_delete` spy queue so real queues
  never lose a message. Peek modes (browse/skip/destructive) are distinct and tested.
- **Jolokia** (`broker/jolokia.rs`): blocking `ureq` client correctly offloaded via
  `spawn_blocking`; all `unwrap`s have safe fallbacks (`unwrap_or`/`unwrap_or_default`).
- *Minor:* `amqp.rs` uses `map_err(|e| anyhow!(...))` where `rabbitmq.rs` uses the more
  idiomatic `.context()`; `message_meta()` (~60 lines of `if let Some(..) { push }`) is
  repetitive but obviously correct. Both cosmetic.

### App state machine (`app/state.rs`, `input.rs`, `connection.rs`, `realtime.rs`, `events.rs`) — *solid, moderate debt*
- **State is mostly enum-modeled** (`Screen`, `InputMode`, `PaneFocus`, `SubState`,
  `ScanStep`), which makes many illegal states unrepresentable. Good.
- **Input dispatch** is a clean hierarchy of per-screen/per-mode `handle_*_key` methods
  (overlays → text modes → screen handlers → pane handlers). *Correction to an earlier
  automated finding: there is no monolithic `apply()` function — dispatch is well
  factored.*
- Debt items (all minor, see §3): duplicated text-entry handlers, repeated
  `active_conn_mut()`-then-one-call patterns, and two dual-boolean states that want to be
  enums.

### UI layer (`ui.rs`, `ui/views.rs`, `ui/anim.rs`, `theme.rs`) — *good*
Disciplined separation: rendering is a near-pure function of `&App`. Theming is fully
centralized — no hardcoded colors leak into widgets; `NO_COLOR` and per-style overrides are
supported; animations (`anim.rs`) are pure functions of a clock. Weaknesses are
cosmetic: a few long render functions (e.g. the Browser screen ~200 lines), some magic
layout numbers (column-width constraint arrays; a stray `Length(44)` status width), and a
scroll-clamp idiom repeated ~3×.

### Config & secrets (`config.rs`, `config/secret.rs`) — *clean, security-conscious*
TOML model with serde defaults and a tagged `ConnectionConfig` enum. Secrets are **never
plaintext**: a profile's `password` is a *spec* (`env:VAR` / `keyring[:account]` /
`prompt`), resolved env→keyring→prompt; unknown forms safely fall back to `Prompt` (never
treated as a literal password); keyring access is moved off the async runtime via
`spawn_blocking`. There's even a backward-compat alias migrating the old `slow`/`fast`
animation values to `on`. Thoughtful.

### Recording (`recording.rs`) — *clean, very testable*
`Recorder<W: Write>` is generic over the writer so it unit-tests against an in-memory
buffer; `RecordSink` is the on-disk JSONL target. Binary-safe (base64), lossless, filename
sanitized, timestamp injected for determinism, and the reader tolerates malformed lines
(renders a marker instead of aborting). Exemplary small module.

### CLI & dev tooling (`cli.rs`, `dev/*`) — *fine*
`clap`-derived CLI; man-page/completions generated from the live `Command` so they can't
drift. `dev` is a headless fake-data publisher/seeder for local brokers. Lower-risk;
skimmed, no concerns.

---

## 3. Cross-Cutting Clean-Code Findings

### 🟠 [Medium] Vestigial code from an in-flight refactor — *the dominant theme*
A removed headless `record` command and a deferred AMQP/RabbitMQ browser rework have left
behind code that no production path reaches, kept alive with `#[allow(dead_code)]` and
explanatory comments:
- `SubSpec::parse`, `SubSpec::supported_type`, and the `SubSpec::Topic`/`Queue`/`Exchange`
  variants (`broker.rs:430-595`); `AMQP_SHORTSTR_MAX` (`broker.rs:36`);
  `ConnectionConfig::broker_type` (`config.rs:142`); `Capabilities::databases`
  (`broker.rs:119`).

This is honestly commented, but it (a) clutters the abstraction, (b) creates **false test
coverage** (see §4), and (c) the "kept for the pending rework" rationale is exactly the
kind of speculative retention that tends to rot. Recommend: delete it, or move it behind a
clearly-labeled `#[cfg(feature = "...")]`/`mod planned` so it isn't mistaken for live code.

*Note:* I verified that `pubsub_spec`/`stream_key`/`classify_password` (in `app.rs`) are
**not** dead — they're called from `realtime.rs:365/373` and `connection.rs:139`. (An
automated pass mislabeled them; they're fine.)

### 🟡 [Low] Doc-comment density & drift risk
Comments are unusually thorough and capture real "why" rationale — a genuine strength. But
the volume sometimes buries the code, and several comments narrate *removed* features
("now that the headless `record` command is gone", "the `[`/`]` switcher was removed",
"their realtime tails were removed pending a rework"). Historical comments tied to deleted
code drift out of truth. Recommend trimming "what used to be" narration; keep the "why it
is this way now."

### 🟡 [Low] `#[allow(clippy::too_many_arguments)]` → params structs
`App::new` (7 args, `app.rs:167`), `spawn_connection` (8 args, `broker/actor.rs:118`), and
`start_subscription` (8 args, `broker/actor.rs:185`) suppress the lint. The codebase
already shows the better pattern (`TailParams` in `actor.rs`); applying it here removes the
suppressions and is more readable.

### 🟡 [Low] Duplication
- **Text-entry key handlers** share a near-identical skeleton (Esc→Normal, Enter→submit,
  Char/Backspace): `handle_add_destination_key` (`input.rs:371`), `handle_publish_key`
  (`input.rs:399`), `handle_peek_filter_key` (`input.rs:421`), and similar in
  `handle_rename_key`/`handle_filter_key`. A small `handle_text_input(buf, on_submit)`
  helper would collapse most of it (submit bodies stay distinct).
- **Scroll-clamp idiom** (`max = lines.len().saturating_sub(viewport); scroll.min(max)`)
  appears ~3× in `ui/views.rs`; a `clamp_scroll` helper would dedupe.
- **`active_conn_mut()`-then-one-call** appears many times in `input.rs`/`realtime.rs`; an
  `fn with_active_conn(|c| ...)` helper would cut the boilerplate.

### 🟡 [Low] Two dual-boolean states should be enums
- `KeyBrowser` has both `scanning` and `complete` bools, always written in tandem
  (`state.rs:689/696`, set at `1189-1190`/`1249-1250`) — three valid states encoded in two
  bools with one illegal combination. A `ScanPhase { NotStarted, InProgress, Complete }`
  enum makes it airtight.
- `quit_armed` and `recordings_delete_armed` (`app.rs:158/154`) are independent
  confirm-chord flags; a single `ConfirmState` enum would be clearer (and there will likely
  be more such chords).

### 🟡 [Low] UI magic numbers
Column-width constraint arrays and a stray status-width `Length(44)` in `ui/views.rs` are
unlabeled literals; promoting them to named `const`s (the file already does this for some,
e.g. `SERVER_BAND_HEIGHT`) would make layout intent explicit.

---

## 4. Test Suite Quality — *large and mostly high-value, with real but fixable weaknesses*

**Accurate counts:** 489 `#[test]`/`#[tokio::test]` functions. Distribution:
`app/tests.rs` 168, `ui.rs` 61, `broker/amqp.rs` 24, `app/state.rs` 30, `broker.rs` 19,
`broker/actor.rs` 18, `ui/views.rs` 18, with the rest spread across modules. (An automated
pass under-counted `app/tests.rs` at "52" by sampling — the real figure is 168.)

### What's genuinely good
- **A real mock harness, not stubs.** `broker/actor.rs::mock` provides a scriptable
  `MockBroker` + `handle()`/`amqp_handle()` helpers, so `App`/`ui` tests drive a *real
  `ConnHandle` and actor* without a live broker. The actor tests **synchronize on
  observable events** (`wait_for`, `drain_for`) instead of fixed sleeps — the right way to
  test async (`broker/actor.rs:795`).
- **Integration tests are correctly gated** behind `feature = "integration"` and
  self-seed uniquely-namespaced keys, so the default `cargo test` is fast and
  deterministic while real-broker coverage exists on demand (`redis.rs:418`).
- **High-value behavioral tests** abound: connection lifecycle/health transitions, the
  event-drain batching under a simulated flood (guards the render loop against starvation),
  form validation + on-disk persistence *verifying secrets aren't written in plaintext*,
  recording round-trips, the actor stamping the scan epoch to discard stale pages, and the
  recorder's exact-count progress reporting.
- **`recording.rs`, `config.rs`, `config/secret.rs`, `broker.rs` tests** are tight and
  meaningful (serde round-trips, malformed-line handling, secret-spec parsing, default
  application, legacy-value migration).

### Weaknesses (in priority order)
1. **🟠 Tests of dead code = false coverage.** Tests for `SubSpec::parse` and
   `supported_type` (`broker.rs` — e.g. `parses_amqp_specs`,
   `parses_rabbitmq_exchange_specs`, `rejects_overlong_exchange_name_or_binding_key`,
   `sub_spec_supported_kind_maps_each_spec_to_its_broker`) exercise functions **no
   production path calls**. They pass and inflate the count while guarding nothing the app
   actually does. They should go when the dead code goes (§3).
2. **🟡 Implementation-coupled tests.** A meaningful slice of `app/tests.rs` reaches into
   private fields (`app.connections[0].browser.table.select(...)`,
   `app.form.as_ref().unwrap().fields[0]`, mutating `status.shown_at` directly) rather than
   asserting observable output. These break on harmless refactors without catching bugs.
   Where practical, prefer driving via key/app events and asserting via the existing
   render-to-`TestBackend` approach used in `ui.rs`.
3. **🟡 Trivial pure-function tests.** A handful assert a 3–5-line pure fn against
   restated cases (`refresh_ticks`, `move_selection`, simple enum `label()`/`is_on()`
   getters). Low cost, low value; fine to keep but they're not "coverage" in any meaningful
   sense.
4. **🟡 A few timing-dependent tests.** The sustained-flood drain test and an
   "AMQP tick skips stats" test rely on `sleep`/timeout windows and a spawned producer —
   potential flakiness on a loaded CI box. They're written carefully, but watch them.
5. **🟡 Monolithic test file.** `app/tests.rs` is 3,908 lines / 168 tests in one module.
   Splitting into `tests/{connection,browser,subscriptions,forms,recordings,notifications}`
   submodules would aid navigation (pure ergonomics).

### Coverage gaps
Console command execution, the keyspace tail integration path, multi-connection focus
switching, and error-recovery/retry paths are comparatively thin relative to how well the
connection/browser/recording paths are covered.

### Test verdict
**Better than typical.** The mock harness, event-synchronized async tests, gated
integration suite, and persistence/serde round-trips are the marks of someone who tests
behavior, not line counts. The two things actually worth doing: **(1) delete the dead-code
tests** (they're the only outright misleading part), and **(2) reduce private-field
coupling** in `app/tests.rs` so the suite survives refactors.

---

## 5. Prioritized Recommendations

**P1 — highest leverage**
1. Remove (or clearly quarantine) the vestigial `record`/deferred-rework code in
   `broker.rs`/`config.rs`, and delete the tests that only exercise it. Eliminates the
   false coverage and the largest source of "is this live?" confusion.
2. Reduce private-field coupling in `app/tests.rs`; prefer event-driven + render-output
   assertions.

**P2 — small, clean wins**
3. Replace the three `too_many_arguments` suppressions with params structs (follow the
   existing `TailParams` pattern).
4. Collapse the duplicated text-entry key handlers; add `clamp_scroll` and
   `with_active_conn` helpers.
5. Turn `scanning`+`complete` into a `ScanPhase` enum and the two confirm-chord bools into
   a `ConfirmState` enum.

**P3 — polish**
6. Trim doc-comments that narrate removed features; name the UI layout magic numbers.
7. Split `app/tests.rs` into per-area submodules.
8. Add tests for console execution, keyspace-tail integration, and error-recovery paths.

None of these are urgent; the codebase is shippable as-is. They're about keeping it clean
as it grows — and about not letting an in-flight refactor leave permanent residue.

---

## Appendix — How findings were gathered / verified
- Read in full: `main.rs`, `app.rs`, `event.rs`, `broker.rs`, `broker/actor.rs`,
  `config.rs`, `config/secret.rs`, `recording.rs` (+ their test modules).
- Deep-dives (cross-checked against source): UI layer; app state/input layer; AMQP/
  RabbitMQ/Jolokia brokers; the `app/tests.rs` suite.
- Smell sweeps: production `unwrap`/`expect`/`panic` (≈0 outside tests/dev), `unsafe`
  (none), `#[allow(...)]` (few, justified), `TODO`/`FIXME` (none), clone density.
- Corrected two automated mis-findings before writing this: `pubsub_spec`/`stream_key` are
  **live** (not dead), and there is **no** monolithic `apply()` in `input.rs`.
