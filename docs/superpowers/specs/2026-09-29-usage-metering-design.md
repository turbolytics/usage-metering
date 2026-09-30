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
sample-app ─SDK─▶ ingest ──▶ Kafka usage.events (6 partitions, keyed by customer)
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
| `ingest` | SQLFlow | webhook with `ack: after_flush` → Kafka, `key: customer` |
| `count` | SQLFlow, 3 replicas | Kafka → one-minute window → Postgres upsert |
| `archive` | SQLFlow | Kafka → Parquet on MinIO, every raw event |
| `rollups` | SQLFlow `rollup run` | hour and day totals, drift, freshness |
| `api` | SQLFlow `serve` | the three read endpoints |
| `sample-app` | Node | a fake LLM API with many users, metered per user by the SDK |

Kafka runs one broker, so every topic has one replica. `after_flush` then
means the event is on that broker's log, which survives an `ingest` crash but
not the loss of the broker. The README says so, and says a deployment needs
replication factor 3 with `min.insync.replicas=2` for the same guarantee to
hold against a broker failure.

Every SQLFlow service runs the pinned release image with a config from
`config/`. Nothing in this repo patches the engine. A gap found here becomes a
sql-flow issue.

## The event

The scenario is an AI company that bills each user individually for requests
and tokens. One LLM request is one event:

```json
{"id": "evt_8f2a41", "customer": "user_48213", "status": 200,
 "ts": "2026-09-24T12:07:13Z",
 "quantities": {"requests": 1, "input_tokens": 812, "output_tokens": 214}}
```

- `customer` is the billed user.
- `id` is required. The SDK generates it once per event and reuses it on every
  retry, which is what makes retries safe.
- `quantities` holds one number per meter. `ingest` unnests it into one row
  per meter, and dedupe is on `(id, meter)`.
- `ts` is set by the SDK from the app's clock. `ingest` clamps it to at most 5
  seconds ahead of its own clock, because one far-future timestamp moves the
  watermark and closes every minute until it.

## The pipelines

**ingest** (`config/ingest.yml`): webhook source with `ack: after_flush`. The
handler unnests a batch body (`{"events": [...]}`, since the webhook makes one
message per request), drops events with no `id`, clamps `ts`, and writes each
event to Kafka with `key: customer`. A retry carries the same customer, so it
lands on its original's partition. Keying by customer also means each user
writes one row per meter per minute, where keying by id would multiply that by
the partition count.

**count** (`config/count.yml`): Kafka source, one-minute window,
`partition_owned: true`, `allowed_lateness_seconds: 60`. The window holds
`(minute, customer, meter, kafka_partition, id_hash, quantity)` rows, where
`id_hash` is `md5_number(id)`, a fixed 128-bit value, and emits
`sum(quantity)` over distinct `id_hash` per `(minute, customer, meter,
kafka_partition)`. The Postgres sink upserts on those four columns. Three
replicas share the partitions, and each partition's row has one owner at a
time.

**archive** (`config/archive.yml`): Kafka source under its own group,
Parquet to MinIO, partitioned by date and hour. Every raw event, unmodified.

**Retention:** a scheduled job drops minute rows older than 7 days. Hour and
day totals are kept.

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

## Accuracy

The invoice total is exact and reproducible: the same events always produce
the same total, so a total recomputed from the Parquet archive matches the
rollup. Three consequences for the design:

- **No probabilistic dedupe.** A Bloom filter errs only by treating a new
  event as seen, which silently drops real usage, and its answer depends on
  insertion order, so a replay can disagree with the original. HyperLogLog
  counts within about 1%. Both are fine for analytics and wrong for an
  invoice.
- **Hashed ids, not raw ids.** `md5_number(id)` is deterministic, so replays
  agree, and fixed at 16 bytes. At 50,000 events a second over 180 seconds of
  retention, the chance of any collision is about 10^-20. A 64-bit hash would
  save 8 bytes a row and give about a 3% chance of one miscounted event a
  month at that rate, so it is not used.
- **A stated dedupe horizon.** A duplicate is caught while its minute is
  retained: window + grace + lateness, 180 seconds. The SDK's 60-second retry
  budget sits inside it. A duplicate sent later, such as a customer replaying
  yesterday's log, is counted twice. OpenMeter dedupes on `source` + `id` in
  Redis for 32 days. A long-horizon dedupe store is a separate design, listed
  as a limit in the README, not stretched onto the window.

Readers differ in what they need. The invoice needs exact totals. The quota
check needs speed and may lag by a flush interval. The usage page needs to
match the invoice at period close. All three read the same totals in v1.

## Scale

The SDK runs in the app's backend servers, so ingest load is batched requests
from those servers, not traffic from each user. The sizing scenario:

- 1,000,000 users billed individually, 10% active on a given day.
- At peak, 30,000 users active in a minute, several requests a second each:
  about 50,000 events a second, two token meters and a request meter each.

| Component | At 50,000 events/s | Holds? |
|---|---|---|
| ingest | ~50 app servers × 1 batch/s | yes, stateless, scale by replicas |
| Kafka | ~10 MB/s | yes |
| count, window memory | 50k × 180 s × 16 B ≈ 150 MB of id hashes across workers | yes |
| Postgres writes | 30k users × 3 meters ≈ 90k rows/min, ~1.5k upserts/s, plus the rollup cascade | yes, on a modest instance |
| Postgres storage | minute rows, up to ~130M/day at sustained peak | only with retention: minutes kept 7 days, hours and days kept |
| serve | a quota read per LLM request | yes, with the response cache |

These are estimates. `make load RATE=<events/s>` measures them, and the
README publishes what was measured on a laptop at 1k, 10k and 50k events a
second: `count` memory, Postgres upserts a second, and the p50 and p99 of an
`after_flush` 200. The same run finds where Postgres stops keeping up, and the
README states that ceiling. Past it, the answer is ClickHouse, as OpenMeter
uses at millions of events a second, and the README says so.

## The SDK

TypeScript, in `sdk/`, not published to npm in v1. The HTTP API is the
contract. The SDK exists to make the client side reliable.

```ts
const meter = new Meter({ url: 'http://localhost:8001', flushMs: 1000 })

meter.track('user_48213', { requests: 1, input_tokens: 812, output_tokens: 214 })
                                              // never blocks, never throws
await meter.usage('user_48213', 'input_tokens', { period: '2026-09' })
await meter.close()                           // flushes what is buffered
```

- `track` appends to an in-memory buffer and returns. It never awaits the
  network and never throws.
- A background loop sends the buffer as one request every `flushMs` or at
  `maxBatch` events. A non-2xx or network error retries with the same ids,
  with backoff and jitter, for at most 60 seconds. That stays inside the count
  window's retention (60 + 60 + 60 seconds), so a retry is always
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
| Replay the same events twice from the archive, totals identical | reproducible totals | shipped |
| `make load` at 1k, 10k and 50k events/s, published in the README | scale | new |

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
