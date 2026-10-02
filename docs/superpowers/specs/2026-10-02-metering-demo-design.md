# usage-metering: the demo and its console

**Status:** design, awaiting review.
**Repository:** `turbolytics/usage-metering`, public. Runs locally only; we do
not host it.
**Builds on:** `2026-09-29-usage-metering-design.md` (the reference stack).
This spec changes some of its decisions; see "Changes to the reference stack"
below and the Revisions section added to that spec.
**Depends on:** the SQLFlow release that carries #414 (`ack: after_flush`,
Kafka sink `key`, `partition_owned`, the low-watermark commit). The latest tag,
`v2026.09.30.1`, does not; the repo pins a placeholder until the release is cut.
**Pairs with:** turbolytics.io `/use-cases/metering`.

## The message

Your usage events flow through Kafka. SQLFlow handles everything around it:
one binary, one YAML file per role.

```
                            ┌─▶ [archive] ─▶ your blob store   (raw events as Parquet: audit, recompute)
your app ─SDK─▶ [ingest]* ─▶ your Kafka
                            └─▶ [count: windowing] ─▶ your Postgres ◀─ [rollups: minute→hour→day]
                                                            │
                                     your readers ◀── [serve: read path]
* optional on-ramp: skip if your app already produces to Kafka
```

- **The hard part, and where SQLFlow earns its place:** windowing (exact
  minute counts from Kafka that survive crashes, rebalances, retries and late
  data) and rollups (minute → hour → day, verified, with freshness).
- **Completing the picture:** raw capture to blob storage for audit and
  recompute, and the read path that serves one number to every reader.
- **The easy on-ramp:** ingest, an HTTP endpoint that writes to Kafka and
  answers 200 only once the event is durable.

The demo and README say plainly who this is for. Streaming pre-aggregation
pays off when a customer produces many events per meter per minute: at 50,000
events a second, about 9M raw rows a minute become about 90k minute rows. Below
a few events per customer per minute, a Postgres table keyed by event id is
simpler, and the README says so.

## What runs

The repo separates the production shape from the demo scaffolding, so the
first file an architect opens shows the real footprint.

| File | Services | Purpose |
|---|---|---|
| `docker-compose.yml` | `kafka`, `postgres`, `minio`, `ingest`, `count`, `archive`, `rollups`, `serve` | The production shape. Grey boxes are infrastructure the customer already runs; blue boxes are SQLFlow. |
| `docker-compose.demo.yml` | `count-2`, `demo-console` | Demo scaffolding: a second count worker to show failover, and the console. |

`make demo` runs both files (10 services). The prospect opens
`http://localhost:8080`.

The core file runs one count worker with the same config `count-2` uses,
`partition_owned` included, so the production shape scales out by adding
replicas with no config change. With one worker, `partition_owned` costs only
a recount from Kafka at restart.

## Changes to the reference stack

- **Two count workers, not three.** `count` and `count-2` share one consumer
  group; the 6-partition topic splits 3/3. Two is the fewest that shows a
  handoff.
- **`partition_owned: true`**, required because there is more than one count
  worker. The minute table keeps `kafka_partition` in its key, and readers sum
  over it.
- **`allowed_lateness_seconds: 120`**, up from 60. A crashed worker's
  partitions move only after the ~45s session timeout, and lateness must exceed
  the rebalance time (sql-flow #417). This lengthens the dedupe horizon to
  window + grace + lateness, 185 seconds; the SDK's 60-second retry budget
  stays well inside it.
- **`sample-app` gains a control API and a sent ledger** (below).
- **The SDK gains `onAck`** (below).
- **`serve` gains one dataset, `minute_totals`**: per-minute totals per meter
  across all users. Invoice, quota and usage are unchanged.

## The console

### Audience and success

The audience is the architect a director forwarded the use-case page to, and
the director watching over their shoulder. Success: in under ten minutes,
without opening a terminal, they watch their own synthetic users' usage come
back exact through every reader, through a worker crash, and recomputed from
the raw archive.

### Layout

Approved mockup: layout A, with the tour on the left.

```
┌───────────────────┬──────────────────────────────────────────────────────────┐
│ How it works 3/7  │ Users [500]  Rate [200 ev/s]  [Generate load] [Stop]     │
│ ✓ 1 On-ramp       │                          ● running · 200 ev/s · 0 dropped │
│ ✓ 2 Raw capture   ├──────────────────────────────────────────────────────────┤
│ ▶ 3 Windowing     │ app+SDK → ingest → Kafka → count  P0 P1 P2 [stop][crash] │
│   what's happening│                            count-2 P3 P4 P5 [stop][crash] │
│   where to look   │                     → Postgres ← rollups                  │
│   [Next]          │ archive → blob store      serve → readers                 │
│   4 Rollups       │ grey = yours · blue = SQLFlow · orange = this step        │
│   5 Read path     ├─────────────────────────────────────┬────────────────────┤
│   6 Failures      │ Usage per minute (all users)        │ user_00042         │
│   7 Recompute     │ ▇ ▇ ▇ ▇ ⬚   solid = counted          │ sent    1,234      │
│ Exit tour →       │            dashed = sent, still open │ invoice 1,234 ✓    │
│ free dashboard    │ 12:04 sent 11,820 counted 11,820 ✓  │ quota   1,234 ✓    │
│                   │                                     │ usage   1,234 ✓    │
└───────────────────┴─────────────────────────────────────┴────────────────────┘
```

- The **tour rail** lists all seven steps, with the current one expanded: what
  is happening, where to look, and Next. It exits to a **free dashboard** with
  every control.
- The **diagram** is live: each box's container state, the partitions each
  count worker owns, and the Parquet file count. The current step's components
  are outlined in orange. Failure buttons sit on the count boxes, and each
  shows the equivalent command, e.g. `docker compose kill count`; the backend
  performs it through the Docker Engine API.
- The **minute graph** shows a dashed "sent" bar for the open minute, from the
  load generator, which becomes a solid "counted" bar from `serve` when the
  minute closes, with both numbers.
- The **drill-down** shows one user's sent total next to the invoice, quota and
  usage readers.

### The tour

Each step completes on a real signal, never a timer.

| Step | What the prospect does and sees | Completes when |
|---|---|---|
| 1 On-ramp | Generate load; a 200 means the event is in Kafka. | The first acknowledged events appear. |
| 2 Raw capture | Parquet files land in the blob store. | The first Parquet file lands. |
| 3 Windowing | A minute closes; counted lands on sent. | The first minute closes with counted = sent. |
| 4 Rollups | Minute → hour → day; `rollup verify` runs. | `rollup verify` reports zero drift. |
| 5 Read path | Drill into a user; four numbers match. | A drill-down shows sent = invoice = quota = usage. |
| 6 Failures | Stop `count-2`, restart it, crash `count`, then restart it. | All four actions are done and the affected minutes settle exact. |
| 7 Recompute | Recompute a minute from raw Parquet. | The recomputed total matches the counted one. |

Progress is kept in the browser's local storage; a returning visitor lands on
the dashboard with "replay the tour".

The crash is shown honestly. A stopped worker hands its partitions over within
seconds; after a crash Kafka waits out the ~45s session timeout first. The
diagram shows "P0–P2 unowned, waiting for session timeout (~45s)", then
`count-2` taking over. Minutes spanning the crash take longer to settle and
still end exact. Restarting the crashed worker recounts its partitions from
Kafka, as `partition_owned` does at every start.

## Components

### `demo-console` (new, TypeScript)

- **Frontend:** Preact for the tour, controls and drill-down; uPlot for the
  graph; the diagram as SVG.
- **Backend:** a small Node server, and the only thing the browser talks to.
  One module per job:
  - **reads:** `minute_totals`, invoice, quota and usage through `serve`; never
    Postgres directly.
  - **load:** start and stop, forwarded to `sample-app`; the sent ledger read
    from it.
  - **docker:** the allowlist, over the mounted Docker socket via the Docker
    Engine API, targeting compose services by label.
  - **kafka:** the consumer group's partition assignment.
  - **archive:** Parquet file listing from MinIO.
  - **recompute:** embedded DuckDB reading one minute's raw Parquet from MinIO
    and counting over distinct ids. Embedded rather than an `exec` into a
    SQLFlow container, so the Docker allowlist stays narrow.

### `sample-app` (additions to the reference-stack spec)

- **Control API:** `POST /load {users, rate}`, `POST /load/stop`,
  `GET /status` (rate, sent, dropped).
- **Sent ledger:** in memory, per minute, customer and meter, built only from
  events ingest acknowledged (`onAck`), served at `GET /sent`. It records when
  it started; a restart clears it, and the console shows "sent unknown" for
  minutes before that.

### SDK (addition)

`onAck(events)`, called once for each batch ingest answered 200, and never for
a batch that failed. The ledger is built on it; real customers can use it to
log what was billed.

## Console API

| Endpoint | Does |
|---|---|
| `POST /api/load {users, rate}`, `POST /api/load/stop` | Start and stop load through `sample-app`. |
| `GET /api/status` | Load status, container states, partition ownership, Parquet file count. |
| `GET /api/minutes?from&to` | Per minute and meter: sent (ledger) and counted (`minute_totals`). |
| `GET /api/users?search=`, `GET /api/users/:id` | The user picker; one user's sent, invoice, quota and usage. |
| `POST /api/actions/:name` | One of `stop-count`, `stop-count-2`, `start-count`, `start-count-2`, `kill-count`, `kill-count-2`, `rollup-verify`. Returns the command, its output and success. |
| `POST /api/recompute {minute}` | The SQL, the recomputed total, the counted total, and whether they match. |

The browser polls `/api/status` and `/api/minutes` every 2 seconds. Defaults
are 500 users at 200 events/s, capped at 10,000 users and 2,000 events/s so a
laptop stays responsive; the README says how to raise the caps.

## Error handling

The console never shows a ✓ it has not earned.

- **Reconciliation states.** ✓ when counted = sent. "Counting…" while counted
  is below sent inside the minute's settle window: 95 seconds after the minute
  ends (the close at about 65 seconds, plus 30 seconds of margin), extended by
  75 seconds (the 45-second session timeout plus 30) for any minute that
  overlaps a worker handoff. A red mismatch only after the settle window passes. A red
  overcount immediately if counted ever exceeds sent, because that means a
  double count. Red states link to the relevant container's logs and say this
  should not happen.
- **Stack starting:** boxes show "starting"; Generate stays disabled until
  `ingest`, both count workers and `serve` are healthy.
- **A dependency unreachable:** the affected panel says so and retries; stale
  numbers are never shown as live. Unknown partition ownership shows
  "unknown".
- **Ledger reset:** minutes before `sample-app` started show "sent unknown",
  not a mismatch.
- **SDK drops** (buffer full): shown as "N dropped by the SDK"; never counted
  as sent.
- **Docker actions:** one at a time, buttons disabled while one runs. Socket
  missing or permission denied: the error is shown with the exact command, so
  the step can be done in a terminal. Anything off the allowlist is refused.
- **Recompute before the archive has flushed the minute:** "archive hasn't
  written 12:04 yet", and it retries.

## Security

The console mounts the Docker socket, which is root-equivalent on the host.
That is acceptable for a demo run locally, and the README says so. The backend
allows only the actions listed above, and the console publishes only port 8080
on localhost.

## Testing

- **Unit:**
  - the Docker allowlist, with the most cases: listed actions only, rejecting
    look-alike service names, extra arguments and other containers;
  - reconciliation states, including the handoff extension and ledger reset;
  - the recompute SQL against fixture Parquet, including an id delivered twice;
  - the sent ledger (acknowledged only, a retry counted once);
  - SDK `onAck` (once per acknowledged batch, never for a failed one);
  - the tour state machine (each step completes on its signal and no earlier).
- **End-to-end:**
  - the reference-stack `make test` suite stays the proof of the page's claims;
  - one Playwright smoke test: generate load, the first minute closes with
    counted = sent, drill into a user and see four matching numbers;
  - one Playwright failure test: crash `count` and the affected minutes settle
    exact.
- **The demo and the tests assert the same things:** each tour step's success
  condition is the same assertion as an end-to-end test, so a ✓ in the demo is
  never weaker than what CI checks.

## Out of scope

Hosting the demo, multi-tenancy, authentication, a Render Blueprint, the npm
release of the SDK, in-request quota `check()`, period close and freeze,
ClickHouse, and a per-partition watermark (sql-flow #417).
