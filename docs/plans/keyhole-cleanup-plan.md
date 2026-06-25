# Keyhole — Clean-up Implementation Plan

## Context

This plan supersedes the earlier `keyhole-code-review.md`. That review graded the codebase
**A− / 8.5** and flagged maintainability debt across the sections rated below "clean": the
**app state machine** ("moderate debt"), the **UI layer** ("good, cosmetic weaknesses"),
**CLI/dev tooling** ("fine", only skimmed), the **test suite** ("fixable weaknesses"), plus a
set of cross-cutting findings. This document turns those findings into executable work.

A deep re-investigation confirmed most findings **but overturned the review's headline P1**:

> **The review's "dead code from the removed `record` command" is mostly wrong.**
> `SubSpec::parse`, the `SubSpec::Topic`/`Queue`/`Exchange` variants, and `AMQP_SHORTSTR_MAX`
> are **live production code** (called from `app/connection.rs:489`, `app/input.rs:382`,
> `app/realtime.rs:355`, `dev/amqp.rs:51/56`, `broker/jolokia.rs:154/156`, `app/state.rs:807`).
> They are merely **mislabeled** with stale `#[allow(dead_code)]` attributes and "test-only"
> comments. Only **three** items are genuinely removable: `SubSpec::supported_type`,
> `ConnectionConfig::broker_type`, and the `Capabilities::databases` field. Three of the four
> tests the review said to delete must be **kept** — they cover the live `parse`.

Two scope corrections:
- **Error-recovery/retry is a *feature* gap, not a test gap** — there is no reconnect/backoff in
  production (errors surface as status only). **Out of scope** here; don't add tests for logic
  that doesn't exist.
- The two "flaky" timing tests in the default run are **watch-not-fix** (one proves a negative
  via a fixed sleep). The broker spawn/sleep tests the review worried about are
  `feature = "integration"`-gated and don't run in normal CI.

**Definition of Done (per `CLAUDE.md` + `/scripts`):** every phase ends green on `scripts/test.sh`
and `scripts/lint.sh` (`cargo fmt --check` + `cargo clippy --all-targets --all-features -- -D
warnings`). Because clippy runs with `-D warnings`, every `#[allow(dead_code)]` /
`#[allow(clippy::too_many_arguments)]` removal must be backed by a real fix — a surprise clippy
warning means the reachability read was wrong **for that item**; investigate, don't re-add the
attribute. Work on a git worktree; commit each phase.

---

## Phase 1 — Dead code & misleading comments *(corrects the review's P1)*

**1a. Remove genuinely-dead code (+ only the tests that exclusively cover it):**
- `SubSpec::supported_type` — delete method (`broker.rs:584-595`) **and** its sole test
  `sub_spec_supported_kind_maps_each_spec_to_its_broker` (`broker.rs:938-967`).
- `ConnectionConfig::broker_type` — delete (`config.rs:138-150`). No test references it.
- `Capabilities::databases` — a **cascade**, do last and in this order: drop field
  (`broker.rs:116-120`) → drop the 3 production writes (`broker.rs:139/157/174`) → drop the
  `redis(databases: u32)` param (`broker.rs:136`) and its caller (`redis.rs:106-107`) → drop
  `database_count()` (`redis.rs:78-91`). Its sole consumer is the dead field, so removal also
  drops a now-purposeless `CONFIG GET databases` connect-time round-trip (safe; nothing reads
  `databases` — the `[`/`]` db switcher was removed). **Edit, don't delete, these tests:** remove
  the `.databases` assertions at `broker.rs:1005/1012/1023` (keep `capabilities_constructors` for
  its other asserts) and the `caps.databases >= 1` check at `redis.rs:466`.

**1b. Fix the mislabeled-live-code clutter (keep the code):**
- Remove the redundant `#[allow(dead_code)]` on the live items: `broker.rs:36`
  (`AMQP_SHORTSTR_MAX`), `broker.rs:430/433/440` (the three `SubSpec` variants), `broker.rs:456`
  (`SubSpec::parse`). Verify with `cargo check`.
- **Correct** (don't just trim) the comments that falsely call live code "test-only":
  `broker.rs:32-35`, `broker.rs:423-429`, `broker.rs:453-455`. Reword to describe the real live
  callers (destination add/seed/tail).

**1c. Fix two drifted comments:**
- `views.rs:285-288` overclaims "the **only** state writes the render path performs" — there are
  ~5 render-time scroll writes (see Phase 4). Reword.
- `app.rs:405-408` says AMQP/RabbitMQ "realtime tails were removed pending a rework" — AMQP tails
  are **live** (`input.rs:361`). Reword to cover only what's actually deferred.

---

## Phase 2 — App state-machine enums & helpers

**2a. `ScanPhase` enum** (replaces `KeyBrowser { scanning, complete }`): add
`enum ScanPhase { NotStarted, InProgress, Complete }`; replace the two bools (`state.rs:689/696`,
init `740/742`). `InProgress` in `begin_scan` (`state.rs:1189-1190`), `Complete` in `apply_page`'s
terminal page (`state.rs:1249-1250`). `complete` has zero production reads (its doc at
`state.rs:687` is stale); `scanning` has one — `events.rs:123` → `phase != ScanPhase::InProgress`.
Update tests (`state.rs:2231`; `tests.rs` 360/361/374/375/436/437/553/576/642/689/712; `ui.rs`
1462/1465 reads + the 5 `browser.complete = true` writes 2039/2111/2150/2174/2214). Leave
`scan_live`/`scan_buf` alone.

**2b. `ConfirmState` enum** (replaces `quit_armed` + `recordings_delete_armed`): add
`enum ConfirmState { None, Quit, DeleteRecording }` on `App` (`app.rs:154/158`, init `228/230`).
The two are never armed simultaneously, so the disarm guards (`input.rs:555-564`) collapse into
one. Update `input.rs:583/586`, `recordings.rs:152-163`, tab enter/leave `input.rs:1008/1020`, and
tests `tests.rs:2460/2465/2485/2487/2600/2618/2640`. **Do not merge** the form-scoped
`ConnForm::confirm_delete` (`state.rs:1547`).

**2c. `handle_text_input` helper:** the naive `(buf, on_submit: impl FnOnce(&mut Self))` **won't
compile** (double borrow). Use an outcome enum: `enum TextEdit { Editing, Cancelled,
Submitted(String) }`, `fn handle_text_input(buf: &mut String, key, clear_on_esc: bool) ->
TextEdit`. The caller does its own mode change + submit after the borrow ends. Collapses the 4
clean handlers (`input.rs:371/399/532/702`; `clear_on_esc=false` only for filter). Leave
`handle_peek_filter_key` (`input.rs:421`) and `handle_subscribe_key` (`input.rs:720`).

**2d. `with_active_conn` helper:** `fn with_active_conn<R>(&mut self, f: impl FnOnce(&mut
Connection) -> R) -> Option<R>` (closure borrows only `conn` — no double-borrow). `browser_view`
(`input.rs:931`) already is this shape. Apply at the ~10 pure conn-only sites (e.g. `realtime.rs:159`,
`input.rs:266/951/245/256/273/282/292/456/899`). Be selective: leave the sites needing a trailing
`self.` call or a returned borrow / `else`-return.

---

## Phase 3 — Params structs (kill the 3 `too_many_arguments` allows)

Mirror `TailParams` (`broker/actor.rs:242-249`). Bundle `App::new` (`app.rs:167`, 7 args) into an
`AppParams` (updates 10 call sites: `main.rs:133`, `tests.rs:16`, `settings.rs:198`, and 7 in
`ui.rs`); `spawn_connection` (`actor.rs:118`) and `start_subscription` (`actor.rs:185`, 11 args)
into their own context bundles. Remove all three `#[allow(clippy::too_many_arguments)]`.

---

## Phase 4 — UI layer cleanup

**4a. Scroll-clamp:** there is already a free `visible_offset(...)` (`views.rs:179`, follow+clamp)
and a method `ValueInspector::clamp_scroll(max)` (`state.rs:771`, applies a given max). Don't add a
third `clamp_scroll`. Add `fn max_scroll(total, viewport) -> usize` and use it to dedupe the 3
byte-identical raw idioms (`views.rs:540-541`, `643-644`, `1194-1195`) + the value-pane site
(`383-385`). Leave the variant sites (`console_content` 1250-1253, `server_clients` 1696,
`tail_content` 1038-1039) and the selection-follow blocks.

**4b. Name the meaningful layout magic numbers** (follow `SERVER_BAND_HEIGHT` / `CLIENT_*_COL`
style): `STATUS_WIDTH = 44` (`ui.rs:154`), connection-list columns (`views.rs:119-123`), browser
key-table columns (`views.rs:340-343`), AMQP destinations table (`views.rs:441`). Leave
self-evident single-row splits.

**4c. Shorten the `browser` render fn** (`views.rs:193-404`, ~212 lines): extract the inline Redis
key-table window+row build (`~279-360`) into `key_table_pane` and the value pane (`~362-395`) into
`value_pane`, mirroring the AMQP branch's delegation.

---

## Phase 5 — CLI / dev tooling *(the "fine"-rated area, dug into)*

- **Stale docs:** `main.rs:42-49` says `TICK_PERIOD` is "250 ms" but it's **33 ms** (`main.rs:39`).
  `cli.rs:1-2` and `main.rs:4-5` say "the only subcommand is the hidden `gen`" — there are now two
  (`Gen`, `Dev`).
- **Test passing for the wrong reason:** `man_page_renders_and_mentions_the_binary` (`cli.rs:319`)
  asserts `man.contains("record")` — but there's no `record` subcommand; the string comes from the
  `about` tagline (`cli.rs:11`). Re-point at the binary name and assert symmetrically that **both**
  hidden subcommands (`gen`, `dev`) don't leak.
- **Dedupe the dev publisher loop** (3×: `dev/redis.rs:156-166`, `dev/amqp.rs:34-44`,
  `dev/rabbitmq.rs:74-83`) into a `run_publisher` helper.
- **Make the pure builders unit-testable** (mirror `dev/fixtures.rs`): `url()` (`dev/redis.rs:18-37`,
  `dev/rabbitmq.rs:21-32`) and publish-target selection (`dev/amqp.rs:48-62`,
  `dev/rabbitmq.rs:89-106`), currently reachable only under `feature = "integration"`.
- Minor: surface (don't silently `.ok()`) the close error at `dev/rabbitmq.rs:84`.

---

## Phase 6 — Test-suite coupling reduction *(selective)*

Use the render seam in `ui.rs`: `screen_text(app)` (`ui.rs:392`), `find_label` (`ui.rs:404`),
`render_lines` (`ui.rs:1820`).
- **Bucket 2 — the clean win (do fully):** tests that assert observably via `status_fade()` but
  arrange by backdating private `status.shown_at` → advance `app.now` instead
  (`tests.rs:2512/2532/2536/2541/2556/2634`).
- **Bucket 1 — assert-on-internal (the brittle ones):** migrate form/selection reads to
  `screen_text`/`find_label` where it de-brittles (e.g. `tests.rs:1828/1917/941`). Don't chase
  every `.selected()` read.
- **Bucket 3 — arrange-by-bypass (mostly skip):** leave `browser.table.select(..)` arrange-only
  calls and `amqp_with_messages` direct-field setup unless they actively break.

*(Optional P3: a console `Exec`-round-trip test via the mock, and a two-live-connection
active-switch test. **Not** retry — out of scope.)*

---

## Phase 7 — Split `app/tests.rs` *(LAST — pure relocation)*

3,908 lines / 168 tests, declared at `app.rs:488-489`, with 24 `// --` section banners mapping
~1:1 to submodules. Split into `app/tests/{…}.rs`: move shared harness helpers (`tests.rs:8-120`)
to a shared prelude; move theme-local helpers with their theme; verify `use super::*;` visibility.
Fix the one mis-grouping (the headerless recordings-view block `tests.rs:2644-2838` under the
"notifications" banner). Done after the test-editing phases to avoid splitting-then-re-editing.

---

## Verification

After **each** phase: `scripts/test.sh` (full default suite green) and `scripts/lint.sh` clean.
Phases 1/3 specifically prove the `#[allow(…)]` removals. Sanity-run the TUI for Phase 4
(`cargo run`) and the dev publishers for Phase 5. Optionally run the integration suite if the
docker broker stack is up. Commit each phase with a focused message.

## Out of scope (explicit)
- **Retry/reconnect/backoff** — a feature gap, not a cleanup item.
- **Stabilizing the 2 default-run timing tests** — watch, don't rewrite.
- Trivial pure-function/getter tests — keep as-is.
