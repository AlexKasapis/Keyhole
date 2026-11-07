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
| Peek in `browse` mode | **Get + requeue** through `POST /api/queues/{vhost}/{q}/get`. Messages go back on the queue with `redelivered` set. On quorum queues the pane warns that a requeue may count toward `delivery-limit`. (Phase 0: only 3.x counts it, so the warning is limited to 3.x quorum queues that have a limit. See Phase 4.) |
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

### Findings (2026-11-07)

Tested against `rabbitmq:3-management` = **RabbitMQ 3.13.7** (Erlang 26.2.5.16) and
`rabbitmq:4-management` = **RabbitMQ 4.4.0** (Erlang 29.1.2), both with the image's default
config, and **ureq 3.5.0** (the `Cargo.lock` version, default features). "3.x" and "4.x" below
mean these two builds.

#### 1. Requeue semantics

| | 3.13.7 | 4.4.0 |
|---|---|---|
| Default quorum `delivery-limit` | none (unlimited) | 20 |
| Where the limit shows in queue info | no `delivery_limit` field; read `arguments["x-delivery-limit"]`, else `effective_policy_definition["delivery-limit"]` | `delivery_limit` (the effective value: argument, policy or the default 20) |
| `ack_requeue_true` on a quorum queue | `redelivered: true`, `x-delivery-count` +1 per peek. Over the limit the message is dropped, or dead-lettered with `x-death` reason `delivery_limit` | `redelivered: true`, only `x-acquired-count` +1. `x-delivery-count` is untouched, so the limit is never reached (6 peeks on a limit-3 queue and 22 on a default-20 queue: message kept) |
| `reject_requeue_true` on a quorum queue | identical to `ack_requeue_true` | `x-delivery-count` and `x-acquired-count` +1. Over the limit the message is dropped (default 20, so lost unless there is a DLX) |
| Classic queue (either mode) | `redelivered: true`, no count header | same |

**Recommendation: `ack_requeue_true`.** It's never worse than `reject_requeue_true`, and on 4.x
it doesn't touch the delivery count at all. What remains on both versions: `redelivered` is
set, a count header is added (`x-delivery-count` on 3.x, `x-acquired-count` on 4.x), the
queue's `get` / `redeliver` stats move, and the messages are briefly unavailable to consumers
while peeked. Requeued messages kept their order. The delivery-limit warning is only true for
**3.x quorum queues that have a limit** (Phase 4 updated).

#### 2. Response trimming

- **`columns=`** works on both versions, on lists and single objects, including dotted nested
  paths: `message_stats.publish_details.rate` returns
  `{"message_stats":{"publish_details":{"rate":9.6}}}`. Absent keys are omitted, not `null`
  (`delivery_limit` on 3.x, `message_stats` on a queue that never saw traffic). It also works
  on `/api/nodes`, `/api/vhosts/{v}/connections` and `/api/consumers/{v}`.
- **Paging.** `page` + `page_size` return an envelope:
  `{filtered_count, item_count, items, page, page_count, page_size, total_count}`, items sorted
  by name. `page_size` is capped at 500 (501 → 400 `invalid_pagination_parameters`), `page=0`
  → 400, and a page past `page_count` → 400 `page_out_of_range`. An empty vhost answers
  `page=1` with `page_count: 0, items: []`. `page_size` without `page` is ignored and returns a
  bare array.
- **Rates** (`message_stats.*_details.rate`) are present on 4.4.0 with the default config, with
  the same keys as 3.13.7. They are floats and can be negative (`-0.2` seen).
- **Metrics collector off** (4.x `management_agent.disable_metrics_collector = true`, the
  deprecated `management_metrics_collection` feature) removes far more than rates:
  `/api/overview` reports `disable_stats: true` and has no `message_stats` / `queue_totals`;
  queue rows lose `messages*`, `consumers` and `message_stats`; nodes lose memory / fd / disk;
  `/api/vhosts/{v}/connections` returns `[]`; `/api/consumers/{v}` returns 400 "Stats in
  management UI are disabled on this node". `/get` still works.
- Columns used for the fixtures, recommended for Phase 2:
  - queues: `name,type,state,durable,auto_delete,exclusive,arguments,policy,effective_policy_definition,delivery_limit,messages,messages_ready,messages_unacknowledged,consumers,message_stats.publish_details.rate,message_stats.deliver_get_details.rate,message_stats.ack_details.rate,message_stats.redeliver_details.rate`
  - exchanges: `name,type,durable,auto_delete,internal,arguments,policy,message_stats.publish_in_details.rate,message_stats.publish_out_details.rate`
- Other version differences: 4.x adds `x-queue-type` to every queue's `arguments` (3.x has
  `{}` for a plain classic queue), so read the kind from `type`. 4.x has an extra internal
  exchange, `amq.rabbitmq.log`; `amq.rabbitmq.trace` is `internal: true` on both. Publishing to
  an internal exchange fails with `403 ACCESS_REFUSED - cannot publish to internal exchange`.

#### 3. Permissions

`/api/whoami` returns `{"name":"keyhole","tags":["administrator"],"is_internal_user":true}` on
both versions. `tags` is a JSON array.

| Endpoint | No tags | `management` only | `monitoring` or `administrator` |
|---|---|---|---|
| `/api/whoami`, `/api/overview` | 401 | 200 (overview has no `node`, `listeners`, `contexts`) | 200 |
| `/api/nodes` | 401 | 401 "Not monitor user" | 200 |
| `/api/vhosts/%2F/connections` (and `/api/connections`) | 401 | 200, **own connections only** | 200, all |
| `/api/consumers/%2F` (and `/api/consumers`) | 401 | 200, **own channels' consumers only** | 200, all |
| `/api/queues/%2F`, `/api/exchanges/%2F` | 401 | 200, all (needs a vhost permission) | 200 |
| `POST /api/queues/%2F/{q}/get` | 401 | 200 with read permission on the queue | same |

`administrator` includes everything `monitoring` can do. **None of the tested endpoints returned
403.** Every auth failure was a 401, told apart by `reason`:

- `{"error":"not_authorized","reason":"Not_Authorized"}`: wrong username or password (note
  the `z`; the others spell `not_authorised`)
- `"Not management user"`: no `management` tag (or stronger)
- `"Not monitor user"`, `"Not administrator user"`
- `"ACCESS_REFUSED - read access to queue 'q' in vhost '/' refused for user 'u'"`: `/get`
  without read permission
- `"User not authorised to access virtual host"` on 3.x. On 4.x the same case is a **404**
  `{"error":"not_found","reason":"Not Found"}`.

Only a request without an `Authorization` header gets `www-authenticate: Basic realm="RabbitMQ
Management"`.

#### 4. URL encoding

ureq 3.5.0 sends the path and query exactly as given. Checked on a raw TCP listener: no
re-encoding, decoding or normalisation (`%2F`, `%2f`, `%2E%2E` and `..` all go out verbatim).
It also doesn't encode unsafe input: a raw space fails with `http: invalid uri character`, raw
non-ASCII goes out as raw UTF-8 bytes, and a raw `#` silently cuts the path off as a fragment.

Both brokers decode segments built with the repo's existing
`utf8_percent_encode(s, NON_ALPHANUMERIC)`, for GET and `POST /get`: `%2F` (vhost),
`spike%2Fslash`, `spike%20space`, `spike%23hash`, `spike%25pct`,
`spike%2Ecaf%C3%A9%20%E2%9C%93`, `spike%2Bplus%3Fq%3D1`, `spike%2Dempty`. **Recommendation:**
encode every vhost and name segment with `NON_ALPHANUMERIC`, and never put a raw name in a URL.

**Keep-alive quirk (3.13.7 only).** When `POST /get` answers 404 (missing queue), Cowboy closes
the connection without sending `connection: close`. A request sent right after on the same ureq
agent fails with `io: Peer disconnected` (3 of 3 runs). With a 100 ms gap ureq notices and
reconnects, and curl retries silently. It wasn't seen after a 400 (stream), a 401 or a GET 404,
nor on 4.4.0. Sending `Connection: close` on the POST fixes it (verified).

#### 5. Streams

`POST /api/queues/%2F/{stream}/get` returns HTTP 400 with
`{"error":"bad_request","reason":"540 basic.get not supported by stream queues queue 'spike.stream' in vhost '/'"}`.
It's identical on both versions and for every `ackmode`, and the stream is left unchanged.
Detect streams from the queue `type` (`"stream"`) before peeking, and keep this 400 as the
fallback.

#### Other `/get` facts (for Phase 4)

- **`properties` is `[]`** (an empty JSON array, not an object) for a message that has no
  properties and hasn't been redelivered. The serde type must accept both.
- `truncate` cuts the body before base64. `payload_bytes` is the full size, so the body is
  truncated when the decoded length is less than `payload_bytes`.
- `message_count` is the number of messages left after this one.
- Header values are arbitrary JSON: int, float, bool, array, nested object, and `x-death` (an
  array of objects). `timestamp` is integer seconds.
- Missing queue: `POST .../get` returns 404 `{"error":"not_found","reason":"queue_not_found"}`
  on both versions. `GET /api/queues/%2F/{missing}` returns 404 with `error` `"Object Not Found"`
  (3.x) or `"not_found"` (4.x).

#### Fixtures

All fixtures are in `src/broker/rabbitmq/testdata/`, named `<endpoint>[.<variant>].v<major>.json`
(`v3` = 3.13.7, `v4` = 4.4.0). Error bodies are named `error.<http-status>.<case>.v<major>.json`.
Every file is a real response, pretty-printed with `jq --indent 2` and otherwise unedited. Unless
marked "v4 only", each file exists for both versions. Queues and exchanges use the column lists
above.

| File | Request (user) | What it holds |
|---|---|---|
| `whoami.{administrator,monitoring,management}` | `GET /api/whoami` | One per tag |
| `overview` | `GET /api/overview` (admin) | Full overview with live rates |
| `overview.management` | same (management user) | No `node` / `listeners` / `contexts` |
| `nodes` | `GET /api/nodes` (admin) | One node, unfiltered (~12 KB) |
| `queues` | `GET /api/queues/%2F?columns=…&page=1&page_size=500` | Paged envelope with 17–18 queues: classic, quorum (limit by argument, policy, default), stream, awkward names, and two queues with consumers and live rates |
| `queues.p1-of-2`, `queues.p2-of-2` (v4 only) | same, `page_size=10`, pages 1 and 2 | Multi-page collection |
| `queues.empty-vhost` (v4 only) | an empty vhost | `page_count: 0` |
| `exchanges` | `GET /api/exchanges/%2F?columns=…&page=1&page_size=500` | Every type, the default `""`, `amq.*`, internal exchanges, rates |
| `connections` | `GET /api/vhosts/%2F/connections` (admin) | Two lapin connections with `user_provided_name` |
| `connections.management` | same (management user) | Only that user's own connection |
| `consumers` | `GET /api/consumers/%2F` (admin) | Manual-ack prefetch-10 consumers on a classic queue, auto-ack consumers on a quorum queue |
| `get.classic` | `POST …/spike.classic/get` (count 3, `ack_requeue_true`, base64) | JSON body with every property and typed headers; a binary non-UTF-8 body; plain text with `properties: []` |
| `get.quorum` | first fetch from a quorum queue | `redelivered: false` |
| `get.quorum.requeued` | second fetch | 3.x `x-delivery-count: 1`, 4.x `x-acquired-count: 1` |
| `get.quorum.reject-requeued` (v4 only) | fetch after one `reject_requeue_true` | Both count headers |
| `get.truncated` | `truncate: 10` | `payload_bytes` 81, 10-byte payload |
| `get.dead-lettered` | from the DLQ | `x-death` with reason `delivery_limit` |
| `get.empty` | empty queue | `[]` |
| `{overview,nodes,queues,exchanges,connections}.no-metrics` (v4 only) | same requests with the metrics collector disabled | Missing counts, rates and node stats; `connections` is `[]` |
| `error.401.bad-credentials` | wrong password | `not_authorized` / `Not_Authorized` |
| `error.401.not-management` | user with no tags | "Not management user" |
| `error.401.not-monitoring` | `/api/nodes` (management user) | "Not monitor user" |
| `error.401.not-administrator` | `/api/users` (management user) | "Not administrator user" |
| `error.401.queue-read-refused` | `/get` without read permission | `ACCESS_REFUSED …` |
| `error.401.vhost-access` (v3), `error.404.vhost-access` (v4) | vhost without a permission | 3.x 401, 4.x 404 |
| `error.400.stream-get` | `/get` on a stream | 540 basic.get not supported |
| `error.404.queue-get` | `/get` on a missing queue | `queue_not_found` |
| `error.404.queue` | `GET` a missing queue | 3.x `Object Not Found`, 4.x `not_found` |
| `error.400.page-out-of-range`, `error.400.page-size` | `page=99`; `page_size=501` | Pagination errors |
| `error.400.stats-disabled` (v4 only) | `/api/consumers/%2F` with the metrics collector disabled | "Stats in management UI are disabled on this node" |

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
  `anyhow::Result`. Vhost and names are percent-encoded as single path segments with
  `utf8_percent_encode(_, NON_ALPHANUMERIC)`. ureq sends paths verbatim and encodes nothing
  itself; a raw `#` would silently cut the path (Phase 0, Q4).
- `whoami()` → `/api/whoami`
- `overview()` → `/api/overview`
- `nodes()` → `/api/nodes`. Pass `columns=` for the Phase 5 fields, since an unfiltered node is
  about 12 KB.
- `queues()` and `exchanges()` → `/api/{queues,exchanges}/{vhost}?columns=…&page=N&page_size=500`,
  with the column lists from the Phase 0 findings. A paged reply is an envelope (`items`,
  `page_count`, …). Request pages `1..=page_count` and never past it, because that's a 400
  `page_out_of_range`. An empty vhost returns `page_count: 0`. 500 is the server's maximum
  `page_size`. Collect pages up to a cap of 5,000 rows and report when the list is truncated.
- `get(queue, count, ackmode, truncate)` → `POST /api/queues/{vhost}/{q}/get` with
  `encoding: "base64"`, so binary bodies survive. Send `Connection: close` on this POST. On
  3.13, a 404 here leaves a dead pooled connection, and the next call fails with "Peer
  disconnected" (Phase 0, Q4). `properties` can be `[]` instead of an object.
- `connections()` → `/api/vhosts/{vhost}/connections`
- `consumers()` → `/api/consumers/{vhost}`
- Parsing is pure `serde` over the Phase 0 fixtures. Missing stats fields become `None`, never
  an error. That covers counts as well as rates: with the 4.x metrics collector off
  (`overview.disable_stats: true`), queue rows have no `messages*` / `consumers`, connections
  come back `[]`, and consumers return a 400. Read the queue kind from `type`, not
  `arguments`.
- Error text names the fix. Phase 0 (Q3) saw no 403 at all: every auth failure was a 401,
  told apart by `reason`. Treat any 403 like a 401 with the body's `reason`.
  - `Not_Authorized`: wrong credentials ("set management_username/management_password").
  - `Not management user`: the user lacks the `management` tag.
  - `Not monitor user`: the user lacks the `monitoring` tag.
  - `ACCESS_REFUSED …`: no read permission on the queue. Show the reason.
  - A vhost the user can't access is a 401 on 3.x but a 404 on 4.x.

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
`whoami()` with the short Jolokia timeouts (3 s connect, 5 s read). It also reads
`rabbitmq_version` (`overview()?columns=rabbitmq_version`), because peek semantics differ
between 3.x and 4.x (Phase 0, Q1; used in Phase 4).

- **Probe succeeds:** keep the client and set `can_dashboard` / `can_publish`. Remember the
  tags. Without `monitoring` or `administrator` (which includes it), skip `nodes()`, because
  it's a 401. The connections and consumers endpoints already return only the user's own
  rows, so label those lists rather than filtering them (Phase 0, Q3).
- **Probe fails:** store the error as `mgmt_error`. Every later management call returns that
  error immediately without touching the network, so a dead endpoint never stalls the
  serial actor. The connection stays usable for tails. The `r` key re-runs the probe.

**Tests:**

- Fixture-based parsing for each endpoint, both `.v3` and `.v4`.
- Request-shape tests on the stub server: path, `%2F`, `columns`, auth header, POST body,
  `Connection: close` on the `/get` POST, and page collection that stops at `page_count`.
- 401 `reason` → message mapping, using the `error.*` fixtures.
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
  narrow terminals. Name the widths as constants (cleanup plan 4b). Take the queue kind from
  the `type` field, because 4.x adds `x-queue-type` to every queue's `arguments`.
- **Refresh:** list on connect. `r` lists again (and re-probes after a failure). The list also
  auto-refreshes on the `browse_refresh_ms` clock while the Browser screen is showing. Make
  the `on_tick` gate (`events.rs:119`) a small `Capabilities::auto_refresh()` instead of
  `uses_key_scan()`. Never start a refresh while one is in flight.
- **`a` / `d` (add / delete).** Not offered on RabbitMQ, because the list comes from the
  broker. The footer shouldn't advertise them.
- **`t` on a row:**
  - **topic / fanout / headers exchange:** tap with `#`. A headers exchange is tapped like
    fanout (user decision 2026-11-07): the spy queue's binding has no header arguments, so
    it matches every message routed through the exchange (verified on 3.13.7).
  - **direct exchange:** open the Tail anchor pre-filled with `exchange:name/` so the user
    types the routing key.
  - **queue:** explain that a queue can't be tailed without consuming from it, and suggest
    tapping an exchange that routes to it (queue taps are listed under follow-ups).

  Because the exchange type is now known, `tail_notice` only warns about `#` when the type is
  unknown.
- **Empty and error states in the pane:** "management API unavailable at <url>: <error>
  (tails still work)", "no queues or exchanges in vhost /", and "showing the first 5,000".
  When the broker has management metrics disabled, every count and rate is missing, not just
  the rates (Phase 0, Q2). Show "—" in those columns and say why in the pane.
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
  | `Browse` | `ack_requeue_true` (confirmed by Phase 0: never worse than `reject_requeue_true`, and on 4.x it doesn't raise the delivery count) |
  | `Destructive` | `ack_requeue_false` |
  | `Skip` | no request (handled in `request_peek`, as today) |

  Use `count = VALUE_LIMIT`, and set `truncate` to the `value_preview_bytes` setting.
- Each message maps to a `BrokerEvent`:
  - Payload: base64-decoded, then `Payload::classify`. The body was truncated when the decoded
    length is less than `payload_bytes`.
  - `source`: the queue.
  - `meta`: `exchange`, `routing_key`, `redelivered`, and when present `content_type`,
    `message_id`, `correlation_id`, `delivery_mode` and `headers.*`. These feed
    `PeekInspector`'s existing filter. `properties` may be `[]`. Header values are arbitrary
    JSON (numbers, bools, arrays, nested objects, `x-death`), so render non-strings as compact
    JSON.
- `request_peek` (`connection.rs:428`) learns per-queue-type rules:
  - **stream queue** (`type == "stream"`): no peek. The hint suggests tapping its exchange. If
    a `/get` still reaches a stream, the broker answers 400 "540 basic.get not supported by
    stream queues …". Map that to the same hint.
  - **quorum queue in browse mode on 3.x with a delivery limit** (`arguments["x-delivery-limit"]`
    or `effective_policy_definition["delivery-limit"]`): peek, and show a one-line warning in
    the peek pane title that each peek counts toward the limit. On 4.x, `ack_requeue_true`
    only raises `x-acquired-count` and never reaches the limit, so there's no warning. Tell the
    versions apart by the `rabbitmq_version` that the Phase 2e probe records.
- **Tests:**
  - Map the `get.*` fixtures to events: binary, JSON and plain-text bodies, `properties: []`,
    headers (including `x-death`) and truncation.
  - Each peek mode sends the right `ackmode`.
  - The stream-queue hint, from `type` and from the `error.400.stream-get` fixture.
  - The quorum warning: shown on 3.x with an argument or policy limit; not shown on 3.x
    without a limit or on 4.x.
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
  `monitoring` or `administrator` tag. A management-only user's overview has no `node`. With
  metrics disabled, the overview has no `message_stats` / `queue_totals` and nodes have no
  memory, fd or disk figures (Phase 0, Q2). It holds:
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
  - Build `RabbitmqStats` from the fixtures, including when node data is missing (no
    `monitoring` tag: `overview.management`) and with metrics disabled (`*.no-metrics.v4`).
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
  The API's `name` is the `peer -> broker` address pair. Show the client's own name from
  `user_provided_name` when it's present.
- **Consumers:** queue, consumer tag, channel, ack mode, prefetch, active.
- A management-only user gets only their own connections and consumers; label the tab. With
  metrics disabled, connections come back `[]` and consumers return a 400 "Stats in
  management UI are disabled". Show that instead of an empty list (Phase 0, Q2/Q3).
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
- **Internal exchanges** (`internal: true`, e.g. `amq.rabbitmq.trace`, and `amq.rabbitmq.log`
  on 4.x) reject client publishes with `403 ACCESS_REFUSED`, which closes the channel (Phase
  0). Don't offer `P` on them.
- **Tests:**
  - Prompt sequencing for queue, exchange and fanout, and no `P` on an internal exchange.
  - `PublishReq` contents.
  - The actor reports a NACK / unroutable result.
  - Integration test: publish to a queue, then peek it back.

---

## Phase 8 — Dev stack, docs, release notes

- `config.dev.toml`: add `management_url = "http://127.0.0.1:15673"` to `dev-rabbitmq`
  (the host port differs from the default).
- ~~`keyhole dev` (`dev/rabbitmq.rs`): also declare durable queues `keyhole.demo.orders`
  (classic) and `keyhole.demo.audit` (quorum), bound to `keyhole.demo`, so the browser has
  queues to peek. Add the names to `dev/fixtures.rs`.~~ Done in Phase 4:
  `fixtures::RABBITMQ_QUEUES` binds `keyhole.demo.orders` with `order.*` and
  `keyhole.demo.audit` with `#`.
- `docker-compose.yml`: consider `rabbitmq:4-management`, or a second profile that runs both
  versions, so the integration tests cover 4.x. Phase 0 found real differences between the
  versions: the quorum delivery count on peek, the vhost-denied status, and the 3.13
  keep-alive quirk. That makes a both-versions profile the safer choice.
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
- **4.x metrics.** Rates are present on 4.4.0 by default. With the metrics collector disabled,
  message counts, consumer counts, node stats and the connection list also disappear, and
  `/api/consumers` returns a 400 (Phase 0, Q2). Show "—" for missing values and never fail.
  `overview.disable_stats: true` tells the user why.

## Out of scope / follow-ups

- **Queue taps:** tail a queue non-destructively by binding a spy queue to each of its source
  exchanges with the same keys (`/api/queues/{vhost}/{q}/bindings`).
- **Bindings pane** for the selected exchange or queue (declined for v1).
- Multi-vhost browsing.
- Non-destructive reads of stream queues through a stream consumer with an offset.
- Changing capabilities mid-session after a successful re-probe. Today you reconnect.
- Moving AMQP 1.0 Jolokia discovery behind `list_destinations`.
- Server-side writes: purge, delete, move.
