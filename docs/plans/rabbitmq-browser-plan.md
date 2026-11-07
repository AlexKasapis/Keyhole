# Keyhole — RabbitMQ Browser Implementation Plan

## Context

RabbitMQ (AMQP 0.9.1) is the one broker without a browser. `Capabilities::rabbitmq()`
(`broker.rs:162`) has every flag `false`, so `initial_screen` (`app.rs:407`) leaves a
RabbitMQ connection on the Home screen.

**There is also a regression to fix.** Since the AMQP 1.0 browser landed (a391666), RabbitMQ
tails can't be reached from the TUI. `can_tail()` (`broker.rs:184`) is true only for Redis and
AMQP, and the bottom panel exists only on the Browser screen. The broker-level exchange tap
(`broker/rabbitmq.rs:157`) works and its integration tests pass, but no key starts one. The
README, the docs site (`tails.astro`) and the CHANGELOG all say RabbitMQ tails work.

AMQP 0.9.1 can't list queues or exchanges, and it has no stats plane. Browsing needs the
**HTTP Management API** (`rabbitmq_management` plugin, default `:15672`). The dev stack
already runs `rabbitmq:3-management` and maps the API to host port `15673`
(`docker-compose.yml`).

### Decisions (agreed 2026-11-07)

| Topic | Decision |
|---|---|
| v1 scope | Queue + exchange list, queue peek, exchange tap from the list, restored tails, **plus** an overview stats band, connections/consumers tabs, and publish |
| Management endpoint | **Derived, with overrides.** By default `http://<host>:15672` (`https://<host>:15671` when `tls`), using the AMQP username and password (RabbitMQ has one user database). Optional `management_url` / `management_username` / `management_password` override these. If the API can't be reached, the connection stays usable for tails only and the pane says why. |
| Peek in `browse` mode | **Get + requeue** through `POST /api/queues/{vhost}/{q}/get`. Messages go back on the queue with `redelivered` set. On quorum queues the pane warns that a requeue may count toward `delivery-limit`. |
| Vhost scope | Only the profile's `vhost`. Overview and node stats are cluster-wide. |

### Reused building blocks

- **HTTP:** `ureq` 3 (blocking, rustls), run inside `spawn_blocking`, the same way as
  `broker/jolokia.rs`. Do not add reqwest.
- **Browser UI:** the AMQP destination browser. It provides `DestinationBrowser`,
  `PeekInspector` (message list, preview, filter), `amqp_destination_pane` /
  `amqp_peek_pane` (`ui/views.rs:505/557`), the `t` / `a` / `r` / `d` keys
  (`app/input.rs:230`), and the single-Tail-anchor bottom panel (`state.rs:1408`).
- **Peek mode:** `config::PeekMode` (browse / skip / destructive) and `ConnCommand::Peek` →
  `AppEvent::Peeked`.
- **Stats:** the stats clock (`STATS_REFRESH_TICKS`, `events.rs` `on_tick`), `StatsPanel`, the
  Server band, and the Server Details tab.
- **Profile fields:** the field names and secret rules from `AmqpProfile.management_*`
  (`config.rs:206-216`), and the edit-form carry-over (`app/connection.rs:160-177`).

---

## Phase 0 — Spike: confirm API behaviour (½ day, nothing merged)

Run these against `rabbitmq:3-management` **and** `rabbitmq:4-management`. Save real responses
as test fixtures (`src/broker/rabbitmq/testdata/*.json`) and write the answers into this
document.

1. **Requeue semantics.** Does `/get` with `ackmode=ack_requeue_true` raise a quorum queue's
   delivery count, and does it trigger `delivery-limit` (default 20 on 4.x)? Compare it with
   `reject_requeue_true` and pick whichever disturbs the queue least.
2. **Response trimming.** Do `columns=` and `page`/`page_size` work on `/api/queues/{vhost}`
   and `/api/exchanges/{vhost}` on both versions? Is `message_stats.*_details.rate` still
   present on 4.x?
3. **Permissions.** What does `/api/whoami` return (`tags`)? Which endpoints a user tagged
   only `management` can call, versus `monitoring`: `/api/nodes`, all connections, all
   consumers.
4. **URL encoding.** Does ureq 3 keep `%2F` (the default vhost) and percent-encoded queue
   names in the request path exactly as given?
5. **Streams.** What does `/get` return for a stream queue (`x-queue-type=stream`)? An error is
   expected; record its exact shape.

---

## Phase 1 — Restore RabbitMQ tails (ship on its own)

This fixes the regression without the management API. After it, a RabbitMQ connection opens
on the Browser screen. The destination pane shows an empty-state message and the bottom panel
has the Tail anchor.

- `Capabilities::rabbitmq()`: set `can_browse: true`. `can_tail()`: also return true for
  `BrokerType::Rabbitmq`. `uses_key_scan()` stays Redis-only.
- Replace `active_is_amqp()` (`input.rs:215`) and the `is_amqp` branch in `submit_subscribe`
  (`realtime.rs:342`) with one predicate, `Capabilities::has_destinations()` (true for
  `!uses_key_scan() && can_browse`). The Tail anchor then accepts `exchange:` specs for
  RabbitMQ through the existing `SubSpec::parse`.
- `on_connected` (`events.rs:136`): split the first-load branch on `caps.r#type`. Only AMQP
  seeds curated destinations and runs Jolokia discovery.
- The destination pane's empty state is type-aware. RabbitMQ says "Type `exchange:name` in
  the Tail tab", and Phase 3 replaces that with the management status.
- Update the stale comments that say RabbitMQ has no browser: `app.rs:403-406`,
  `broker.rs:158-161` and `broker.rs:954`.
- **Tests:**
  - App test: a RabbitMQ handle (`mock::rabbitmq_handle`) lands on Browser, and submitting
    `exchange:ex` on the Tail anchor sends `ConnCommand::Subscribe` with
    `SubSpec::Exchange`.
  - `capabilities_constructors` asserts the new flags.
  - New UI snapshot of the RabbitMQ browser with no management API.
- CHANGELOG `Fixed`: "RabbitMQ exchange taps can be started from the TUI again."

---

## Phase 2 — Management client and config

**2a. Shared HTTP plumbing.** Move what Jolokia uses into a new `broker/http.rs`:

- the agent builder (timeouts, `http_status_as_error(false)`, `proxy(None)`)
- the Basic-auth header
- `status_error`
- the `stub_console` loopback test server (as a `#[cfg(test)] pub(crate)` helper)

Jolokia's own tests must pass unchanged. That's a pure refactor and proves the move.

**2b. `broker/rabbitmq/mgmt.rs`.** Follow the repo's `redis.rs` + `redis/` layout: add
`mod mgmt;` to `rabbitmq.rs`. Contents:

- `MgmtClient { base_url, vhost, username, password }`. Every method is blocking and returns
  `anyhow::Result`. Vhost and names are percent-encoded as single path segments.
- `whoami()` → `/api/whoami`
- `overview()` → `/api/overview`
- `nodes()` → `/api/nodes`
- `queues()` and `exchanges()` → `/api/{queues,exchanges}/{vhost}?columns=…&page=N&page_size=500`.
  Collect pages up to a cap of 5,000 rows and report when the list is truncated.
- `get(queue, count, ackmode, truncate)` → `POST /api/queues/{vhost}/{q}/get` with
  `encoding: "base64"`, so binary bodies survive.
- `connections()` → `/api/vhosts/{vhost}/connections`
- `consumers()` → `/api/consumers/{vhost}`
- Parsing is pure `serde` over the Phase 0 fixtures. Missing stats fields become `None`, never
  an error. Error text names the fix: 401 means wrong credentials ("set
  management_username/management_password"); 403 means the user lacks the management tag.

**2c. Config.** Add `management_url`, `management_username` and `management_password` to
`RabbitmqProfile`, using the same serde attributes and docs as `AmqpProfile`, plus:

- `RabbitmqProfile::management_base_url()`: the override, or the URL derived from `host` and
  `tls`.
- `management_password_spec()`

**2d. Secret resolution.** `start_connect` (`connection.rs:29`) also resolves the management
secret for RabbitMQ:

- If `management_password` is set, resolve it. Like AMQP, `prompt` isn't supported for this
  field.
- Otherwise reuse the resolved AMQP password.
- Pass the result to `RabbitmqConnection::new` through `factory::connection_for`.

`submit_form` (`connection.rs:191`) carries the three fields over when a profile is edited,
the same way the AMQP branch does. The form gets no new fields, because the defaults cover
the common case.

**2e. Probe at connect.** `RabbitmqConnection::connect` runs the AMQP connect and then
`whoami()` with the short Jolokia timeouts (3 s connect, 5 s read):

- **Probe succeeds:** keep the client and set `can_dashboard` / `can_publish`. Remember the
  tags; without `monitoring`, skip `nodes()` and show only the user's own connections.
- **Probe fails:** store the error as `mgmt_error`. Every later management call returns that
  error immediately without touching the network, so a dead endpoint never stalls the
  serial actor. The connection stays usable for tails. The `r` key re-runs the probe.

**Tests:**

- Fixture-based parsing for each endpoint.
- Request-shape tests on the stub server: path, `%2F`, `columns`, auth header, POST body.
- URL derivation table: plain, `tls`, override, IPv6 host.
- Edit-form carry-over.
- Missing stats fields stay `None` and don't error.

---

## Phase 3 — Queue and exchange list

- **Trait.** Add `BrokerConnection::list_destinations(&mut self) ->
  anyhow::Result<DestinationList>`. The default bails. `DestinationList { items:
  Vec<DestinationInfo>, truncated: bool }`, where `DestinationInfo` has:
  - `name`, `kind`
  - optional `messages`, `ready`, `unacked`, `consumers`
  - `queue_type` (classic / quorum / stream) or `exchange_type`
  - `publish_rate`

  Add `ConnCommand::ListDestinations` → `AppEvent::DestinationsListed { id, result }`. AMQP
  keeps its Jolokia path; folding that into this trait method is a follow-up.
- **State** (`state.rs:802-865`):
  - Add `DestKind::Exchange`, and add `info: Option<DestinationInfo>` to `Destination`.
  - Dedupe on `(name, kind)`, not derived `PartialEq`, because the stats change on every
    refresh.
  - Add `Connection::replace_destinations(list)`, which keeps the selection by
    `(name, kind)`. AMQP's append-only `merge_destinations` doesn't fit, because RabbitMQ
    queues get deleted on the server.
  - `Destination::spec()` maps an exchange to `SubSpec::Exchange { name, "#" }`. A queue keeps
    `SubSpec::Queue`; update its doc comment so it no longer says the variant is AMQP 1.0 only.
- **List contents:** queues first, then exchanges. Hide the default exchange (`""`), which
  can't be bound. Keep `amq.*`.
- **Columns:** Name · Kind (`queue` / `quorum` / `stream` / `topic` / `direct` / `fanout` /
  `headers`) · Msgs · Ready/Unacked · Consumers · Rate. Columns drop off from the right on
  narrow terminals. Name the widths as constants (cleanup plan 4b).
- **Refresh:** list on connect. `r` lists again (and re-probes after a failure). The list also
  auto-refreshes on the `browse_refresh_ms` clock while the Browser screen is showing. Make
  the `on_tick` gate (`events.rs:119`) a small `Capabilities::auto_refresh()` instead of
  `uses_key_scan()`. Never start a refresh while one is in flight.
- **`a` / `d` (add / delete).** Not offered on RabbitMQ, because the list comes from the
  broker. The footer shouldn't advertise them.
- **`t` on a row:**
  - **topic / fanout exchange:** tap with `#`.
  - **direct exchange:** open the Tail anchor pre-filled with `exchange:name/` so the user
    types the routing key.
  - **headers exchange:** show a status that header-based taps aren't supported.
  - **queue:** explain that a queue can't be tailed without consuming from it, and suggest
    tapping an exchange that routes to it (queue taps are listed under follow-ups).

  Because the exchange type is now known, `tail_notice` only warns about `#` when the type is
  unknown.
- **Empty and error states in the pane:** "management API unavailable at <url>: <error>
  (tails still work)", "no queues or exchanges in vhost /", and "showing the first 5,000".
- **Tests:**
  - App tests with `MockBroker` extended with `list_err` and a canned list: selection kept
    across a refresh, a deleted queue removed, and every `t` branch.
  - Snapshots of the full list, a narrow terminal, and the error state.

---

## Phase 4 — Queue peek

- `RabbitmqConnection::peek` takes `SubSpec::Queue`. Peek modes map to these `ackmode`
  values:

  | `PeekMode` | `ackmode` |
  |---|---|
  | `Browse` | `ack_requeue_true` (or whatever Phase 0 recommends) |
  | `Destructive` | `ack_requeue_false` |
  | `Skip` | no request (handled in `request_peek`, as today) |

  Use `count = VALUE_LIMIT`, and set `truncate` to the `value_preview_bytes` setting.
- Each message maps to a `BrokerEvent`:
  - Payload: base64-decoded, then `Payload::classify`.
  - `source`: the queue.
  - `meta`: `exchange`, `routing_key`, `redelivered`, and when present `content_type`,
    `message_id`, `correlation_id`, `delivery_mode` and `headers.*`. These feed
    `PeekInspector`'s existing filter.
- `request_peek` (`connection.rs:428`) learns per-queue-type rules:
  - **stream queue:** no peek. The hint suggests tapping its exchange.
  - **quorum queue in browse mode:** peek, and show a one-line warning in the peek pane title
    that requeueing counts toward `delivery-limit`.
- **Tests:**
  - Map the `/get` fixture to events, including binary and JSON bodies and headers.
  - Each peek mode sends the right `ackmode`.
  - The stream-queue hint.
  - Integration test: after a browse peek the real queue still holds every message, with
    `redelivered` set. This mirrors `exchange_tap_is_live_only_and_leaves_the_real_queue_intact`.

---

## Phase 5 — Overview stats band and Server Details

- Make stats broker-shaped. Add `enum BrokerStats { Redis(ServerStats), Rabbitmq(RabbitmqStats) }`
  and use it for `BrokerConnection::stats()`, `AppEvent::StatsUpdated` and
  `StatsPanel.stats`. `ServerStats` stays Redis's type, which keeps churn low (about 40
  references across 9 files). `StatsPanel::record` matches on the variant:
  - Redis: ops and network, unchanged.
  - RabbitMQ: publish/s and deliver/s, stored as rounded u64.

  Rename the two history fields to neutral names.
- `RabbitmqStats` is built from `overview()` plus `nodes()` when the user has the
  `monitoring` tag. It holds:
  - RabbitMQ and Erlang versions, cluster name
  - publish / deliver / ack rates
  - totals for queued, ready and unacked messages
  - queue, connection and consumer counts
  - per-node memory used / limit, fd used / total, disk free / limit, and alarms
- **Server band** (`views.rs:816`): name · address · `vhost /` (Redis keeps `db`). An alarm
  turns the health dot to warning.
- **Server Details tab:**
  - Fact lines: version / cluster; totals; node health (memory, fd, disk, alarms). Absent
    values are left out.
  - Graphs: "Publish/s" and "Deliver/s".
  - The right column holds the Connections tab (Phase 6) instead of the Redis client list.
- **Tests:**
  - Build `RabbitmqStats` from the fixture, including when node data is missing (no
    `monitoring` tag).
  - `StatsPanel::record` for both variants.
  - Snapshots of the band and Server Details.
  - Update the existing Redis stats tests mechanically for the enum.

---

## Phase 6 — Connections and consumers

- Bottom panel slots for RabbitMQ (`state.rs:1408`): Server Details, Connections, Consumers,
  the Tail anchor, then one tab per tail. AMQP's slots don't change. Add
  `PanelTab::Connections` and `PanelTab::Consumers`.
- Add `BrokerConnection::roster(kind: RosterKind) -> anyhow::Result<Roster>` and
  `ConnCommand::RefreshRoster(RosterKind)` → `AppEvent::RosterUpdated`. A roster is only
  requested on the stats clock while its tab is focused, the same way Monitor only runs while
  its tab is focused. That keeps load low on brokers with many connections.
- **Connections:** name, peer address, user, channel count, state, receive/send rate, age.
- **Consumers:** queue, consumer tag, channel, ack mode, prefetch, active.
- Both lists scroll with the `details_scroll` pattern (`handle_details_key`).
- **Tests:**
  - Parse the fixtures.
  - Slot order for RabbitMQ versus AMQP.
  - Focusing a roster tab sends `RefreshRoster`; unfocused tabs send nothing.
  - Snapshots of both tabs.

---

## Phase 7 — Publish

Publishing goes over AMQP, so it doesn't depend on the management API. Use lapin
`basic_publish` on a channel opened from the actor's main connection, with publisher confirms
on, so a broker NACK or `unroutable` (with `mandatory`) shows up in `AppEvent::Published`.

- Add `routing_key: String` to `PublishReq`. AMQP 1.0 ignores it.
- **`P` on a queue:** publish through the default exchange with `routing_key = queue name`.
  Only one prompt, for the body, as AMQP does today.
- **`P` on an exchange:** a two-step prompt. Add `InputMode::PublishKey` for the routing key
  (pre-filled with the last key used for that exchange), then `InputMode::Publish` for the
  body. On a fanout exchange, skip the key step.
- **Tests:**
  - Prompt sequencing for queue, exchange and fanout.
  - `PublishReq` contents.
  - The actor reports a NACK / unroutable result.
  - Integration test: publish to a queue, then peek it back.

---

## Phase 8 — Dev stack, docs, release notes

- `config.dev.toml`: add `management_url = "http://127.0.0.1:15673"` to `dev-rabbitmq`
  (the host port differs from the default).
- `keyhole dev` (`dev/rabbitmq.rs`): also declare durable queues `keyhole.demo.orders`
  (classic) and `keyhole.demo.audit` (quorum), bound to `keyhole.demo`, so the browser has
  queues to peek. Add the names to `dev/fixtures.rs`.
- `docker-compose.yml`: consider `rabbitmq:4-management`, or a second profile that runs both
  versions, so the integration tests cover 4.x.
- Integration tests (`feature = "integration"`): list, peek, overview, and connections against
  the docker management API (`KEYHOLE_TEST_RABBITMQ_MGMT_PORT`, default 15673).
- Docs:
  - README capability table and Features list
  - `docs-site`: management settings in `connecting.astro`, a RabbitMQ browser section, and
    the peek-mode semantics, including the quorum caveat
  - `website/src/lib/capabilities.ts`
- CHANGELOG `Added` entry for each user-visible piece.

---

## Verification

After **each** phase:

- `scripts/test.sh` and `scripts/lint.sh` pass. Also run `cargo +<newest> clippy`, because
  CI's Clippy is newer than the local stable toolchain.
- Commit the phase on its own with a focused message, then push.

From Phase 3 on, also check it in the running app: `just dev`, then
`keyhole --config config.dev.toml --connect dev-rabbitmq`. Phases 2–7 also run
`scripts/test.sh --features integration -- --include-ignored` with the docker stack up.

## Risks

- **Serial actor.** Management calls block the connection actor's command loop for up to the
  read timeout. Mitigations: short timeouts, the cached `mgmt_error` fail-fast, and rosters
  only fetched while their tab is focused. If that's still too slow, move management calls to
  their own task that reports through `AppEvent`.
- **Large brokers.** The 5,000-row cap and `columns=` keep payloads bounded. A name filter
  (the API's `name` + `use_regex` parameters) is a follow-up.
- **Managed brokers** (Amazon MQ for RabbitMQ, CloudAMQP) serve the API on `https://<host>`
  (port 443), so the derived URL is wrong for them. The docs must show the
  `management_url` override, and the "unavailable" message should mention it.
- **4.x metrics.** Message rates may be missing when metrics collection is turned down. Show
  "—" for them; never fail.

## Out of scope / follow-ups

- **Queue taps:** tail a queue non-destructively by binding a spy queue to each of its source
  exchanges with the same keys (`/api/queues/{vhost}/{q}/bindings`).
- **Bindings pane** for the selected exchange or queue (declined for v1).
- Multi-vhost browsing.
- Non-destructive reads of stream queues through a stream consumer with an offset.
- Changing capabilities mid-session after a successful re-probe. Today you reconnect.
- Moving AMQP 1.0 Jolokia discovery behind `list_destinations`.
- Server-side writes: purge, delete, move.
