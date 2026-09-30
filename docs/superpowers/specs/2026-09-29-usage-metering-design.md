# usage-metering: the reference stack for SQLFlow usage metering

**Status:** design, awaiting review.
**Repository:** `turbolytics/usage-metering`, public.
**Depends on:** the SQLFlow release that carries `metering/durable-counts`
(sql-flow spec `2026-09-29-metering-durable-counts-design.md`). The repo pins
that release and does not ship before it exists.
**Pairs with:** turbolytics.io `/use-cases/metering` (PR #24). The page makes
the claims; this repo is where an architect checks them.

## Who it is for

The first user is an architect. A director who owns the billing number gets
the outreach email, reads the use-case page, and forwards both to an architect
to evaluate. The architect clones this repo, runs one command, and checks two
things: that one number reaches every reader, and that the page's guarantees
are tests that pass.

Success is that architect, in under ten minutes, running `docker compose up`,
watching the sample app's usage reach Postgres, reading the same total from
the invoice, quota and usage endpoints, and running `make test` to see every
shipped guarantee pass.

Not for v1: a pilot customer deploying it to production, a Render Blueprint,
or a published SDK. Those follow a design partner.

## What runs

```
sample-app ─SDK─▶ ingest ──▶ Kafka usage.events (6 partitions, keyed by id)
                                  │            │
                                  │            └─▶ archive ──▶ MinIO (Parquet)
                                  ▼
                        count × 3 (one consumer group)
                                  │
                                  ▼
          Postgres usage_per_minute (minute, customer, meter, kafka_partition)
                                  │
                   rollups daemon ┤ hour and day totals, verify, freshness
                                  ▼
                        api (sqlflow serve) ──▶ invoice / quota / usage page
```

| Service | Image | What it does |
|---|---|---|
| `kafka` | a single-broker KRaft image | `usage.events`, 6 partitions |
| `postgres` | `postgres:18` | minute table, rollups, migrations |
| `minio` | MinIO | stands in for S3 |
| `ingest` | SQLFlow | webhook with `ack: after_flush` → Kafka, `key: id` |
| `count` | SQLFlow, 3 replicas | Kafka → one-minute window → Postgres upsert |
| `archive` | SQLFlow | Kafka → Parquet on MinIO, every raw event |
| `rollups` | SQLFlow `rollup run` | hour and day totals, drift, freshness |
| `api` | SQLFlow `serve` | the three read endpoints |
| `sample-app` | Node | three fake customers calling a fake API, metered by the SDK |

Kafka runs one broker, so every topic has one replica. `after_flush` then
means the event is on that broker's log, which survives an `ingest` crash but
not the loss of the broker. The README says so, and says a deployment needs
replication factor 3 with `min.insync.replicas=2` for the same guarantee to
hold against a broker failure.

Every SQLFlow service runs the pinned release image with a config from
`config/`. Nothing in this repo patches the engine. A gap found here becomes a
sql-flow issue.

## The event

```json
{"id": "evt_8f2a41", "customer": "acme", "meter": "api_calls",
 "quantity": 1, "status": 200, "ts": "2026-09-24T12:07:13Z"}
```

- `id` is required. The SDK generates it once per event and reuses it on
  every retry, which is what makes retries safe.
- `quantity` defaults to 1. It is summed per distinct id, so a meter can
  count tokens as well as calls.
- `ts` is set by the SDK from the app's clock. `ingest` clamps it to at most 5
  seconds ahead of its own clock, because one far-future timestamp moves the
  watermark and closes every minute until it.

## The pipelines

**ingest** (`config/ingest.yml`): webhook source with `ack: after_flush`. The
handler unnests a batch body (`{"events": [...]}`, since the webhook makes one
message per request), drops events with no `id`, clamps `ts`, and writes each
event to Kafka with `key: id`. A retry lands on its original's partition.

**count** (`config/count.yml`): Kafka source, one-minute window,
`partition_owned: true`, `allowed_lateness_seconds: 300`. The window holds raw
`(minute, customer, meter, kafka_partition, id, quantity)` rows and emits
`sum(quantity)` over `DISTINCT id` per `(minute, customer, meter,
kafka_partition)`. The Postgres sink upserts on those four columns. Three
replicas share the partitions, and each partition's row has one owner at a
time.

**archive** (`config/archive.yml`): Kafka source under its own group,
Parquet to MinIO, partitioned by date and hour. Every raw event, unmodified.

**rollups** (`config/rollups.yml`): hour and day totals over dimensions
`[customer, meter]`, summing the per-partition minute rows. `rollup run`
installs, backfills, verifies and reports freshness.

**api** (`config/serve.yml`): three datasets, one per reader, all reading the
same totals:

- `invoice`: a customer's total for a meter over a period.
- `quota`: a customer's total for the current month, for a quota check.
- `usage`: a customer's totals by day, for their usage page.

The README states plainly that `serve` has no authentication: client ids
identify callers and do not authenticate them. It runs on a private network,
behind the app's own backend.

## The SDK

TypeScript, in `sdk/`, not published to npm in v1. The HTTP API is the
contract. The SDK exists to make the client side reliable.

```ts
const meter = new Meter({ url: 'http://localhost:8001', flushMs: 1000 })

meter.track('acme', 'api_calls')              // never blocks, never throws
meter.track('acme', 'tokens', { quantity: 812 })
await meter.usage('acme', 'api_calls', { period: '2026-09' })
await meter.close()                           // flushes what is buffered
```

- `track` appends to an in-memory buffer and returns. It never awaits the
  network and never throws.
- A background loop sends the buffer as one request every `flushMs` or at
  `maxBatch` events. A non-2xx or network error retries with the same ids,
  with backoff and jitter, for at most 60 seconds. That stays inside the count
  window's retention (60 + 60 + 300 seconds), so a retry is always
  deduplicated.
- The buffer is bounded (`maxBuffered`, default 10,000). When it is full, the
  oldest events are dropped and counted, and `meter.stats()` reports it. An
  app is never blocked by metering.
- `usage` reads the `invoice` dataset from `serve`.
- Not in v1: `check()` for quota gating in the request path. It must fail open
  with a local cache, and it is pilot work.

## Tests

`make test` runs all of them against the compose stack.

| Test | Guarantee on the page | Status |
|---|---|---|
| The sample app's known workload reaches every reader with the same total | one number for every reader | shipped |
| The count SQL, `dev invoke` against fixtures | a change to SQL logic breaks billing | shipped |
| `rollup test` fixtures for the minute → hour → day cascade | double counting (counts) | shipped |
| `rollup verify` after a hand edit reports drift | rollups drift | shipped |
| A killed `count` replica, disk kept and disk lost, then exact totals | a crash mid-stream | shipped with the sql-flow release |
| A replica added mid-minute, then exact totals | rebalance | shipped with the sql-flow release |
| The SDK retries a batch whose 200 was lost, totals exact | double counting (retries) | shipped with the sql-flow release |
| Kill `ingest` between receive and flush, every acknowledged event counted | a 200 means it is recorded | shipped with the sql-flow release |
| Recompute a day from the Parquet archive and match the rollup | usage can be recomputed | shipped |
| SDK unit tests: never throws, bounded buffer, same ids on retry | in-app overhead | new |

Rows that depend on the sql-flow release flip the metering page's status
table from pilot to shipped when that release is out. Period close and freeze,
and an authenticated usage API, stay pilot and are listed in the README as
such.

## The README

It follows the page's five steps, with the commands at each one:

1. The app sends an event: the SDK call and the request it makes.
2. Every raw event is archived: the Parquet file in MinIO.
3. SQLFlow counts it: the minute rows, one per partition.
4. The totals roll up: hour and day, and `rollup verify`.
5. Every reader gets the same number: three `curl` calls, one total.

Then: what is shipped, what is pilot (linking the page's status table), and
the limits, with measured numbers: `count` memory at the demo's rate, the
replay cost of a rebalance, and the `after_flush` request latency.

## Layout

```
usage-metering/
  docker-compose.yml
  Makefile              up, down, test, load
  config/               ingest.yml count.yml archive.yml rollups.yml serve.yml
  migrations/
  sdk/                  TypeScript SDK and its unit tests
  sample-app/           the metered fake API and its load generator
  tests/                end-to-end tests against the stack
  docs/superpowers/specs/
  README.md
```

## Out of scope for v1

- A Render Blueprint and production hardening.
- Publishing the SDK to npm, and SDKs for other languages.
- `check()` in the request path.
- Period close and freeze.
- Authentication on the usage API.
- ClickHouse.
