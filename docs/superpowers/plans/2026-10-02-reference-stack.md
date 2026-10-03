# Reference Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `docker compose` stack an architect clones and runs in ten minutes, in which a known workload of usage events reaches Postgres as exact per-minute, per-hour and per-day totals, every reader serves the same number, and `make test` proves the use-case page's guarantees.

**Architecture:** Five SQLFlow roles around the infrastructure a customer already runs. `ingest` (webhook, `ack: after_flush`) writes events to Kafka keyed by customer; `count` windows them into exact per-minute rows in Postgres with `partition_owned`; `archive` writes every raw event to Parquet on MinIO; `rollups` keeps hour and day tables current with triggers; `serve` is the read path. A TypeScript SDK and a fake LLM API (`sample-app`) are the customer side. The demo console is a second plan.

**Tech Stack:** SQLFlow (`turbolytics/sql-flow` image; see the pin note below), Kafka (`confluentinc/confluent-local:7.5.0`, KRaft, one broker), Postgres 18, MinIO, Node 22 + TypeScript for the SDK, sample-app and tests, vitest, `pg`, `kafkajs`.

**Spec:** `docs/superpowers/specs/2026-09-29-usage-metering-design.md` with its "Revisions (2026-10-02)" section, and `docs/superpowers/specs/2026-10-02-metering-demo-design.md` for the stack changes it introduces (two count workers, 120s lateness, the compose split, sample-app's control API and sent ledger, the SDK's `onAck`, serve's `minute_totals`).

## Global Constraints

- **The SQLFlow image pin.** Every SQLFlow service runs `${SQLFLOW_IMAGE}` from `.env`. The features this stack needs (`ack: after_flush`, Kafka sink `key`, `partition_owned`, the low-watermark commit) merged in sql-flow #414 on 2026-10-02, after the latest tag `v2026.09.30.1`. Until a newer release is cut, `.env.example` pins `v2026.09.30.1` with a loud comment, and `make sqlflow-image` builds `sqlflow:dev` from a local sql-flow checkout for development. Tasks 2 to 5 validate configs with that dev image; Task 8's `make test` cannot pass on `v2026.09.30.1`.
- **Every SQLFlow config pins the session time zone:** `SET TimeZone='UTC';` as its first command. Verified during planning: without it, `strftime` and `epoch_ms` render in the host zone, which put archive partitions four hours off and would mis-bucket events.
- **The event:** `{"id","customer","status","ts","quantities":{"requests":1,"input_tokens":812,"output_tokens":214}}`. `id` and `customer` are required; a body missing either is dropped at ingest. `ts` is RFC 3339 and is clamped to at most 5 seconds ahead of ingest's clock. Dedupe is on `(id, meter)`.
- **The minute table and its key:** `usage_per_minute(minute TIMESTAMPTZ, customer TEXT, meter TEXT, kafka_partition INTEGER, quantity BIGINT, PRIMARY KEY (minute, customer, meter, kafka_partition))`. Readers sum over `kafka_partition`.
- **Window settings:** `size_seconds: 60`, `grace_seconds: 5`, `idle_close_seconds: 5`, `allowed_lateness_seconds: 120`, `partition_owned: true`. The dedupe horizon is 185 seconds; the SDK's retry budget is 60 seconds.
- **Two count workers** in one consumer group, `metering-count`, over a 6-partition topic `usage.events`. `count` is in `docker-compose.yml`; `count-2` only in `docker-compose.demo.yml`, with the same config.
- **Generated rollup tables** are `usage_by_customer_1h` and `usage_by_customer_1d`, columns `(minute, customer, meter, quantity)`: the bucket column keeps the source's name `minute` at every grain. Verified with `sqlflow rollup ddl` during planning.
- **`serve` responses** are `{"dataset","columns":[{name,type}],"rows":[{...}],"row_count","truncated"}`; the client id travels as `?client_id=`. Zoned timestamps are UTC.
- **Prose** follows the repo's voice: plain sentences, no bullet-point marketing. "SQLFlow" in prose, `sqlflow` in code.
- **Commits** name the defect or the deliverable and the evidence. Push with an explicit refspec after each task: `git push origin reference-stack`. Check `git branch --show-current` first.

## Review Focus

The inputs the spec implies but no happy-path test reaches. Each has its test in the named task.

1. **A body whose `quantities` has a null or non-numeric value.** It must be dropped for that meter and the other meters kept, never fail the batch. Test: `ingest` fixture case `evt_badq` (Task 2).
2. **A `ts` with no offset, or far in the future.** No-offset reads as UTC; the future is clamped to now + 5s so one bad clock cannot drag the watermark. Test: fixture cases `evt_noz` and `evt_future` (Task 2).
3. **The same body posted twice with a lost 200 in between.** Counted once, on the same partition. Test: `retries dedupe` (Task 8).
4. **A customer whose events span the two count workers' partitions.** Impossible by construction (keyed by customer), and the e2e asserts no customer has rows under two partitions. Test: `one partition per customer` (Task 8).
5. **`rollup install` racing `serve` at start.** `serve` prepares statements against tables `install` creates; compose orders them. Test: `make up` from clean brings `serve` healthy (Task 5).

---

## File structure

| path | responsibility |
|---|---|
| `docker-compose.yml` | the production shape: `kafka`, `postgres`, `minio`, `minio-init`, `ingest`, `count`, `archive`, `rollups-install`, `rollups`, `serve` |
| `docker-compose.demo.yml` | the overlay: `count-2` (and, in the console plan, `demo-console`) |
| `.env.example` | `SQLFLOW_IMAGE` and the DSNs; copied to `.env` by `make up` |
| `Makefile` | `up`, `down`, `demo`, `test`, `load`, `sqlflow-image`, `validate` |
| `config/ingest.yml` | webhook → Kafka, `ack: after_flush`, `key: customer` |
| `config/count.yml` | Kafka → one-minute window → Postgres upsert, `partition_owned` |
| `config/archive.yml` | Kafka → Parquet on MinIO, partitioned by `day`/`hour` |
| `config/rollups.yml` | the rollup declaration: `usage` from `usage_per_minute`, grains `1h`, `1d` |
| `config/serve.yml` | datasets `minute_totals`, `invoice`, `quota`, `usage` |
| `migrations/001_usage_per_minute.sql` | the minute table; applied by Postgres on first start |
| `fixtures/ingest.jsonl` | `dev invoke` cases for the ingest handler |
| `sdk/src/meter.ts`, `sdk/src/meter.test.ts` | the TypeScript SDK and its unit tests |
| `sample-app/src/server.ts`, `sample-app/src/ledger.ts`, `sample-app/src/ledger.test.ts` | the fake LLM API, load generator, control API and sent ledger |
| `tests/stack.test.ts`, `tests/helpers.ts` | the end-to-end suite against the running stack |
| `README.md` | the five steps, shipped vs pilot, limits |

---

### Task 1: Repository scaffold, infrastructure services, and the minute table

**Files:**
- Create: `docker-compose.yml`, `.env.example`, `Makefile`, `migrations/001_usage_per_minute.sql`, `.gitignore`, `package.json`, `tsconfig.json`
- Modify: `README.md`

**Interfaces:**
- Produces: the compose service names `kafka`, `postgres`, `minio`; the env names `SQLFLOW_IMAGE`, `SQLFLOW_POSTGRES_URI`, `SQLFLOW_KAFKA_BROKERS`, `SQLFLOW_S3_ENDPOINT`; the table `usage_per_minute`. Every later task adds services to this file and reads these names.

- [ ] **Step 1: Branch**

```bash
cd <repo>   # the usage-metering checkout, on spec/reference-stack-and-demo
git checkout -b reference-stack
```

- [ ] **Step 2: The infrastructure services**

`docker-compose.yml`:

```yaml
# The production shape of the metering stack. Grey boxes on the use-case page
# are the three services a customer already runs: Kafka, Postgres and a blob
# store (MinIO here). Blue boxes are SQLFlow, one binary with one config per
# role; Tasks 2 to 5 add them below. docker-compose.demo.yml adds the demo's
# second count worker and the console.
services:
  kafka:
    image: confluentinc/confluent-local:7.5.0
    environment:
      # Six partitions for every auto-created topic, so usage.events splits
      # 3/3 across two count workers. One broker: see the README's note on
      # replication.
      KAFKA_NUM_PARTITIONS: "6"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    ports: ["9092:9092"]
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 5s
      retries: 20

  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: metering
      POSTGRES_PASSWORD: metering
      POSTGRES_DB: metering
    volumes:
      # Applied once, on first start, in filename order.
      - ./migrations:/docker-entrypoint-initdb.d:ro
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U metering -d metering"]
      interval: 3s
      timeout: 3s
      retries: 20

  minio:
    image: minio/minio:RELEASE.2025-09-07T16-13-09Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 3s
      timeout: 3s
      retries: 20

  # Creates the bucket the archive writes to, then exits.
  minio-init:
    image: minio/mc:RELEASE.2025-08-13T08-35-41Z
    depends_on:
      minio: {condition: service_healthy}
    entrypoint: >
      /bin/sh -c "mc alias set local http://minio:9000 minioadmin minioadmin &&
                  mc mb --ignore-existing local/usage"
```

- [ ] **Step 3: The environment file**

`.env.example`:

```bash
# Copied to .env by `make up`; edit .env, not this file.

# The SQLFlow image every role runs.
#
# This stack needs features that merged in sql-flow #414 on 2026-10-02:
# ack: after_flush, the Kafka sink key, partition_owned and the low-watermark
# commit. The latest release, v2026.09.30.1, predates them: with it the stack
# starts but `make test` fails. Until the next release is cut, run
# `make sqlflow-image` to build sqlflow:dev from a sql-flow checkout and set
# SQLFLOW_IMAGE=sqlflow:dev here.
SQLFLOW_IMAGE=turbolytics/sql-flow:v2026.09.30.1

# Where the SQLFlow services find the infrastructure, on the compose network.
SQLFLOW_POSTGRES_URI=postgresql://metering:metering@postgres:5432/metering
SQLFLOW_KAFKA_BROKERS=kafka:9092
SQLFLOW_S3_ENDPOINT=minio:9000
SQLFLOW_S3_ACCESS_KEY_ID=minioadmin
SQLFLOW_S3_SECRET_ACCESS_KEY=minioadmin

# serve's client id. It identifies a caller; it does not authenticate one.
SQLFLOW_SERVE_CLIENT_ID=demo-console
```

- [ ] **Step 4: The minute table**

`migrations/001_usage_per_minute.sql`:

```sql
-- The finest grain: one row per minute, customer, meter and Kafka partition.
-- The count workers upsert on this key, so a republished minute replaces its
-- row. Readers sum over kafka_partition. The rollup tables usage_by_customer_1h
-- and usage_by_customer_1d are created by `sqlflow rollup install`, not here.
CREATE TABLE IF NOT EXISTS usage_per_minute (
  minute          TIMESTAMPTZ NOT NULL,
  customer        TEXT        NOT NULL,
  meter           TEXT        NOT NULL,
  kafka_partition INTEGER     NOT NULL,
  quantity        BIGINT      NOT NULL,
  PRIMARY KEY (minute, customer, meter, kafka_partition)
);
```

- [ ] **Step 5: The Makefile**

```makefile
# The stack: `make up` is the production shape, `make demo` adds the second
# count worker and (in the console plan) the console.
COMPOSE      := docker compose
COMPOSE_DEMO := docker compose -f docker-compose.yml -f docker-compose.demo.yml

.PHONY: env up demo down logs validate sqlflow-image test load

env:
	@test -f .env || cp .env.example .env

up: env
	$(COMPOSE) up -d --wait

demo: env
	$(COMPOSE_DEMO) up -d --wait

down:
	$(COMPOSE_DEMO) down -v

logs:
	$(COMPOSE_DEMO) logs -f --tail=100

# Checks every config against the pinned image's schema and rules.
validate: env
	@for f in config/*.yml; do \
	  $(COMPOSE) run --rm --no-deps -T ingest validate /config/$$(basename $$f) || exit 1; \
	done

# Builds sqlflow:dev from a sql-flow checkout, for work ahead of a release.
# Usage: make sqlflow-image SQLFLOW_SRC=../sql-flow
SQLFLOW_SRC ?= ../sql-flow
sqlflow-image:
	docker build -t sqlflow:dev $(SQLFLOW_SRC)

# The end-to-end suite, against a running `make demo`.
test:
	cd tests && npm ci && npx vitest run

# A load run: make load USERS=1000 RATE=5000
USERS ?= 500
RATE  ?= 200
load:
	curl -s -X POST localhost:8003/load -H 'content-type: application/json' \
	  -d '{"users": $(USERS), "rate": $(RATE)}'
```

- [ ] **Step 6: Repo housekeeping**

`.gitignore`:

```
.env
node_modules/
dist/
.superpowers/
```

`package.json` at the root is a workspaces file so one `npm ci` installs the SDK, sample-app and tests:

```json
{
  "name": "usage-metering",
  "private": true,
  "workspaces": ["sdk", "sample-app", "tests"]
}
```

`tsconfig.json` at the root, extended by each workspace:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "declaration": true
  }
}
```

- [ ] **Step 7: Bring the infrastructure up and check it**

Run:

```bash
make up
docker compose ps
docker compose exec -T postgres psql -U metering -d metering -c '\d usage_per_minute'
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --topic usage.events --partitions 6 --replication-factor 1 --if-not-exists
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic usage.events | head -2
docker compose run --rm minio-init
```

Expected: `kafka`, `postgres` and `minio` healthy; `\d` shows the five columns and the primary key; the topic describes with `PartitionCount: 6`; `mc mb` prints `Bucket created successfully` or that it exists.

- [ ] **Step 8: README skeleton**

Replace `README.md` with the five-step structure the spec names; later tasks fill each step's commands:

```markdown
# usage-metering

The reference stack for usage metering on SQLFlow: exact, reproducible usage
counts at high event volume, self-hosted, and provable. It pairs with
turbolytics.io/use-cases/metering; the page makes the claims, this repo is
where an architect checks them.

Your usage events flow through Kafka. SQLFlow handles windowing and rollups,
the hard part, plus raw capture and the read path, with ingest as an optional
on-ramp. One binary, one YAML file per role.

## Run it

    make up      # the production shape
    make demo    # adds a second count worker (and the console)
    make test    # the guarantees, as tests

## The five steps

1. The app sends an event.
2. Every raw event is archived.
3. SQLFlow counts it.
4. The totals roll up.
5. Every reader gets the same number.

## Shipped and pilot

## Limits

## When not to use this

Below a few events per customer per minute, a Postgres table keyed by event id
is simpler: the primary key dedupes, the commit is the "200 means recorded",
and there are no windows to run. Streaming pre-aggregation earns its place when
raw events outgrow the database you read from: at 50,000 events a second,
about nine million raw rows a minute become about ninety thousand minute rows.
```

- [ ] **Step 9: Commit**

```bash
git add docker-compose.yml .env.example Makefile migrations/ .gitignore package.json tsconfig.json README.md
git commit -m "stack: the infrastructure services and the minute table

Kafka (one KRaft broker, six partitions per topic), Postgres 18 with the
usage_per_minute migration, and MinIO with the usage bucket. The Makefile
and .env.example carry the SQLFlow image pin, which stays at v2026.09.30.1
until the release after sql-flow #414 is cut; make sqlflow-image builds a
dev image from a checkout meanwhile."
git push origin reference-stack
```

---

### Task 2: `ingest`: webhook to Kafka, a 200 means durable

**Files:**
- Create: `config/ingest.yml`, `fixtures/ingest.jsonl`
- Modify: `docker-compose.yml` (add the `ingest` service)

**Interfaces:**
- Produces: topic `usage.events`, one record per `(id, meter)`, keyed by `customer`, value `{"id","customer","meter","quantity","ts_ms"}` with `ts_ms` in Unix milliseconds. Task 3's count config and Task 4's archive config read exactly this shape. The webhook listens on `ingest:8001`, `POST /events`, body `{"events":[...]}`.

- [ ] **Step 1: Write the fixture, with the Review Focus cases**

`fixtures/ingest.jsonl`, one request body per line. `evt_1` is the normal case; `evt_2` has two meters; the third event has no id and must vanish; `evt_badq` has a null meter that must be dropped alone; `evt_noz` has no offset and must read as UTC; `evt_future` is years ahead and must be clamped to now.

```jsonl
{"events":[{"id":"evt_1","customer":"user_1","status":200,"ts":"2026-09-24T12:07:13Z","quantities":{"requests":1,"input_tokens":812,"output_tokens":214}},{"id":"evt_2","customer":"user_2","status":200,"ts":"2026-09-24T12:07:14Z","quantities":{"requests":1,"input_tokens":50}},{"customer":"no_id","ts":"2026-09-24T12:07:14Z","quantities":{"requests":1}}]}
{"events":[{"id":"evt_badq","customer":"user_1","status":200,"ts":"2026-09-24T12:07:15Z","quantities":{"requests":1,"input_tokens":null}},{"id":"evt_noz","customer":"user_3","status":200,"ts":"2026-09-24T12:07:16","quantities":{"requests":1}},{"id":"evt_future","customer":"user_1","status":200,"ts":"2099-01-01T00:00:00Z","quantities":{"requests":1}}]}
```

- [ ] **Step 2: Write the config**

`config/ingest.yml`. The handler SQL was verified with `sqlflow dev invoke` during planning against this fixture: `unnest` over `events`, then `json_keys` over `quantities` gives one row per meter, so a new meter needs no config change.

```yaml
# Usage metering, step one: an HTTP endpoint that puts each metered event in
# Kafka and answers only once it is there. count.yml and archive.yml read the
# topic. Optional: an app that already produces to Kafka can skip it.
commands:
  # A `ts` with no offset reads as UTC, and epoch_ms renders in the session
  # zone; pinned, so the clamp and the millisecond are the same on every host.
  - name: pin the session timezone
    sql: SET TimeZone='UTC';

pipeline:
  name: metering-ingest
  # With ack after_flush a request waits for its batch to fill or for the
  # interval, so the interval is the ceiling on a lone sender's wait:
  # measured p50 1.00s, p99 1.04s at 1s.
  batch_size: 500
  flush_interval_seconds: 1

  source:
    type: webhook
    webhook:
      addr: "0.0.0.0:8001"
      # Answer after the batch holding the body has flushed to Kafka and its
      # position is committed: a 200 means the event is in the log, a failed
      # flush answers 503 and the sender retries. The default, on_receive,
      # answers as the body is queued, and a crash before the flush loses an
      # event the sender was told it had delivered.
      ack: after_flush

  handler:
    type: handlers.InferredMemBatch
    # One request body is a batch of events; one event is one row per meter.
    # An event with no id or customer is dropped: the count depends on both.
    # A meter whose value is null or absent is dropped for that meter only.
    # ts is clamped to at most 5s ahead of this clock, so one far-future
    # timestamp cannot drag the watermark and close every open minute.
    sql: |
      WITH e AS (SELECT unnest(events) AS e FROM batch),
      k AS (
        SELECT e, to_json(e.quantities) AS qj,
               unnest(json_keys(to_json(e.quantities))) AS meter
        FROM e
      )
      SELECT e.id AS id, e.customer AS customer, meter,
             CAST(json_extract(qj, '$.' || meter) AS BIGINT) AS quantity,
             epoch_ms(least(CAST(e.ts AS TIMESTAMPTZ), now() + INTERVAL 5 SECOND)) AS ts_ms
      FROM k
      WHERE e.id IS NOT NULL AND e.customer IS NOT NULL
        AND json_extract(qj, '$.' || meter) IS NOT NULL
        AND json_extract(qj, '$.' || meter) != 'null'

  sink:
    type: kafka
    kafka:
      brokers: ["{{ SQLFLOW_KAFKA_BROKERS }}"]
      topic: usage.events
      # Key every record by the customer, so a retried event lands on the
      # partition its original went to, where the count window sees both
      # and counts the id once. Unkeyed, a retry and its original spread
      # across partitions and each is counted.
      key: customer
```

- [ ] **Step 3: Validate and invoke the fixture (needs `sqlflow:dev`; see the pin note)**

Run:

```bash
make sqlflow-image SQLFLOW_SRC=../sql-flow
docker run --rm -v "$PWD/config:/config:ro" -v "$PWD/fixtures:/fixtures:ro" \
  -e SQLFLOW_KAFKA_BROKERS=kafka:9092 sqlflow:dev validate /config/ingest.yml
docker run --rm -v "$PWD/config:/config:ro" -v "$PWD/fixtures:/fixtures:ro" \
  -e SQLFLOW_KAFKA_BROKERS=kafka:9092 sqlflow:dev dev invoke /config/ingest.yml /fixtures/ingest.jsonl
```

Expected: `ingest.yml: valid`, then exactly these rows, in some order: `evt_1` ×3 meters with `ts_ms` 1790251633000; `evt_2` ×2 (`requests`, `input_tokens`); `evt_badq` ×1 (`requests` only; the null `input_tokens` is gone); `evt_noz` ×1 with `ts_ms` 1790251636000 (read as UTC); `evt_future` ×1 with a `ts_ms` within 6 seconds of now. No row for the id-less event. Verified during planning; a different `ts_ms` for `evt_1` means the time zone pin is missing.

- [ ] **Step 4: Add the service**

Append to `docker-compose.yml` `services:`:

```yaml
  ingest:
    image: ${SQLFLOW_IMAGE}
    command: ["run", "/config/ingest.yml", "--metrics", "prometheus"]
    env_file: .env
    volumes: ["./config:/config:ro"]
    ports: ["8001:8001"]
    depends_on:
      kafka: {condition: service_healthy}
    healthcheck:
      # The webhook's own /healthz: 200 while it admits deliveries.
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8001 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"ok\"' <&3"]
      interval: 3s
      timeout: 3s
      retries: 20
```

- [ ] **Step 5: Post an event and read it back from the topic**

Run:

```bash
make up
curl -s -X POST localhost:8001/events -H 'content-type: application/json' \
  -d '{"events":[{"id":"smoke_1","customer":"user_1","ts":"2026-09-24T12:07:13Z","quantities":{"requests":1,"input_tokens":5}}]}'
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic usage.events --from-beginning --max-messages 2 --property print.key=true --timeout-ms 10000
```

Expected: the curl answers `{"status":"flushed"}` after about a second (not `received`: that would be `on_receive`); the consumer prints two records keyed `user_1`, one per meter.

- [ ] **Step 6: Commit**

```bash
git add config/ingest.yml fixtures/ingest.jsonl docker-compose.yml
git commit -m "ingest: webhook to Kafka with ack after_flush, keyed by customer

One row per (id, meter) from json_keys over quantities, so a new meter needs
no config change; an event without id or customer is dropped, a null meter
is dropped alone, and ts is clamped to 5s ahead of the clock. The session
is pinned to UTC: without it epoch_ms rendered in the host zone. Verified
with dev invoke against fixtures/ingest.jsonl."
git push origin reference-stack
```

---

### Task 3: `count`: exact minutes in Postgres, two workers, `partition_owned`

**Files:**
- Create: `config/count.yml`, `docker-compose.demo.yml`
- Modify: `docker-compose.yml` (add the `count` service)

**Interfaces:**
- Consumes: topic `usage.events` from Task 2; table `usage_per_minute` from Task 1.
- Produces: rows in `usage_per_minute`, upserted on `(minute, customer, meter, kafka_partition)`; the consumer group `metering-count`; compose services `count` (core) and `count-2` (demo overlay), both on this config.

- [ ] **Step 1: Write the config**

`config/count.yml`. This is sql-flow's `dev/config/examples/metering/count.yml` with lateness raised to 120 seconds for the two-worker demo (sql-flow #417: lateness must exceed the time a rebalance takes, which after a crash is the ~45s session timeout), and the session pinned to UTC. Validated during planning.

```yaml
# Usage metering, step two: exact per-minute totals per customer and meter in
# Postgres, from the topic ingest.yml fills. Run as many copies as the topic
# has partitions; they share the consumer group, and partition_owned keeps
# each (minute, customer, meter, kafka_partition) row to one writer.
commands:
  - name: pin the session timezone
    sql: SET TimeZone='UTC';

tables:
  sql:
    - name: usage_window
      # No index, on purpose: DuckDB never frees rows deleted from an indexed
      # table, and every published bucket is deleted (sql-flow #268). Each
      # event is one row; emit_sql sums per key over distinct ids.
      #
      # id_hash is md5_number(id), a fixed 16 bytes per row whatever the id's
      # length, and deterministic, so a replay counts the same. Two ids
      # colliding in one minute is a chance of about 1e-25.
      sql: |
        CREATE TABLE IF NOT EXISTS usage_window (
          minute TIMESTAMPTZ,
          customer TEXT,
          meter TEXT,
          kafka_partition INTEGER,
          id_hash UHUGEINT,
          quantity BIGINT
        );
      window:
        time_column: minute
        size_seconds: 60
        # A minute closes 5s after the stream's clock passes its end, and
        # after 5s of quiet on every partition.
        grace_seconds: 5
        idle_close_seconds: 5
        # A closed minute's rows are kept 120s more: longer than the ~45s a
        # crashed worker's partitions wait before moving (sql-flow #417). An
        # event for it that arrives in that time is counted and the minute
        # republished whole, so a retry is deduplicated within size + grace
        # + lateness: 185s. A retry later than that counts twice; keep the
        # client's retry budget inside it.
        allowed_lateness_seconds: 120
        # Every row belongs to the Kafka partition in kafka_partition. When
        # the group moves a partition to another worker, this one deletes
        # the partition's rows before the rebalance completes rather than
        # publishing its partial count, and the new owner recounts them from
        # the committed position. Without it, both would write the same key.
        # A restart recounts the open minutes from Kafka the same way.
        partition_owned: true
        # Sum each meter over distinct events. A duplicate id in the minute
        # contributes its quantity once.
        emit_sql: |
          SELECT minute, customer, meter, kafka_partition, sum(quantity)::BIGINT AS quantity
          FROM (
            SELECT minute, customer, meter, kafka_partition, id_hash, any_value(quantity) AS quantity
            FROM closed
            GROUP BY ALL
          )
          GROUP BY ALL
        sink:
          # Replaces the row for the key, so a republished minute lands as its
          # whole value. The table needs PRIMARY KEY (minute, customer, meter,
          # kafka_partition). Readers sum over kafka_partition.
          type: postgres
          postgres:
            dsn: "{{ SQLFLOW_POSTGRES_URI }}"
            table: usage_per_minute
            mode: upsert
            key: [minute, customer, meter, kafka_partition]

pipeline:
  name: metering-count
  batch_size: 500
  flush_interval_seconds: 1
  # Kafka is the durable copy of the open minutes: the pipeline commits each
  # partition's position before the oldest row a retained minute holds, so
  # a worker that starts without this file rebuilds them from the log. The
  # file keeps the closed watermark and the progress row.
  state:
    path: /var/lib/sqlflow/count.duckdb

  source:
    type: kafka
    kafka:
      brokers: ["{{ SQLFLOW_KAFKA_BROKERS }}"]
      group_id: metering-count
      auto_offset_reset: earliest
      topics: ["usage.events"]
      # The event's own time, from the SDK's clock, is what buckets it.
      event_time:
        path: ts_ms
        format: unix_ms

  handler:
    type: handlers.InferredMemBatch
    sql: |
      INSERT INTO usage_window
      SELECT
        time_bucket(INTERVAL '60 seconds', event_time) AS minute,
        customer,
        meter,
        kafka_partition,
        md5_number(id) AS id_hash,
        quantity
      FROM batch

  sink:
    type: noop
```

- [ ] **Step 2: Validate**

Run: `docker run --rm -v "$PWD/config:/config:ro" --env-file .env sqlflow:dev validate /config/count.yml`
Expected: `count.yml: valid`, with no `partition_owned` diagnostic. If validate reports that `partition_owned` needs the time column or `kafka_partition` in the sink key, the key line is wrong; it must be exactly `[minute, customer, meter, kafka_partition]`.

- [ ] **Step 3: Add the core worker**

Append to `docker-compose.yml` `services:`:

```yaml
  count:
    image: ${SQLFLOW_IMAGE}
    command: ["run", "/config/count.yml", "--metrics", "prometheus"]
    env_file: .env
    volumes:
      - ./config:/config:ro
      - count-state:/var/lib/sqlflow
    depends_on:
      kafka: {condition: service_healthy}
      postgres: {condition: service_healthy}
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8000 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"ok\"' <&3"]
      interval: 3s
      timeout: 3s
      retries: 30
```

And at the bottom of the file:

```yaml
volumes:
  count-state:
  count-2-state:
```

- [ ] **Step 4: The demo overlay with the second worker**

`docker-compose.demo.yml`:

```yaml
# Demo scaffolding on top of docker-compose.yml: a second count worker, so a
# rebalance has somewhere to move partitions, and (in the console plan) the
# console. Run with: make demo
services:
  count-2:
    image: ${SQLFLOW_IMAGE}
    command: ["run", "/config/count.yml", "--metrics", "prometheus"]
    env_file: .env
    volumes:
      - ./config:/config:ro
      - count-2-state:/var/lib/sqlflow
    depends_on:
      kafka: {condition: service_healthy}
      postgres: {condition: service_healthy}
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8000 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"ok\"' <&3"]
      interval: 3s
      timeout: 3s
      retries: 30
```

- [ ] **Step 5: Count a minute, end to end**

Run (`make demo` so both workers run), then post events for a minute two minutes in the past so it closes at once, and wait for the rows:

```bash
make demo
M=$(date -u -v-2M +%Y-%m-%dT%H:%M:00Z 2>/dev/null || date -u -d '2 minutes ago' +%Y-%m-%dT%H:%M:00Z)
for i in 1 2 3; do curl -s -X POST localhost:8001/events -H 'content-type: application/json' \
  -d "{\"events\":[{\"id\":\"cnt_$i\",\"customer\":\"user_7\",\"ts\":\"$M\",\"quantities\":{\"requests\":1,\"input_tokens\":10}}]}"; done
sleep 70
docker compose exec -T postgres psql -U metering -d metering -c \
  "SELECT minute, meter, sum(quantity) FROM usage_per_minute WHERE customer='user_7' GROUP BY 1,2 ORDER BY 2"
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group metering-count | awk 'NR>1{print $7}' | sort | uniq -c
```

Expected: two rows for that minute, `input_tokens 30` and `requests 3`; the group describe shows two consumer ids, each on three partitions.

- [ ] **Step 6: Commit**

```bash
git add config/count.yml docker-compose.yml docker-compose.demo.yml
git commit -m "count: exact minutes in Postgres, two partition_owned workers

The core file runs one worker; the demo overlay adds count-2 on the same
config, so the stack scales out with no config change. Lateness is 120s,
longer than the session timeout a crashed worker's partitions wait out
(sql-flow #417), making the dedupe horizon 185s. Evidence: three events for
one past minute count as requests 3 and input_tokens 30, with the group split
three partitions each."
git push origin reference-stack
```

---

### Task 4: `archive`: every raw event to Parquet on MinIO

**Files:**
- Create: `config/archive.yml`
- Modify: `docker-compose.yml` (add the `archive` service)

**Interfaces:**
- Consumes: topic `usage.events` (Task 2), bucket `usage` (Task 1).
- Produces: `s3://usage/raw/day=YYYY-MM-DD/hour=HH/<uuid>.parquet`, columns `(id, customer, meter, quantity, ts TIMESTAMPTZ)`, `day` and `hour` as Hive partitions (UTC). The console plan's recompute reads this layout with `hive_partitioning=true`.

- [ ] **Step 1: Write the config**

`config/archive.yml`. Its own consumer group, so it replays, fails and scales independently of `count`; and a separate process rather than a second sink on `count`, because `count` commits at the low watermark and replays already-processed records on every restart, which would duplicate raw rows. The archive commits what it processed and writes each record once. The partitioned `COPY` form was verified against DuckDB during planning.

```yaml
# Usage metering, raw capture: every event, unmodified, as Parquet on object
# storage, partitioned by UTC day and hour. Full fidelity and cheap, so a
# day can be recomputed from it and matched against the rollups.
#
# Its own consumer group, and its own process rather than a second sink on
# count: count commits at the low watermark and replays already-processed
# records at every restart, which here would write them twice.
commands:
  - name: pin the session timezone
    # strftime renders in the session zone: unpinned, the day and hour
    # partitions came out in the host's zone during planning.
    sql: SET TimeZone='UTC';
  - name: install httpfs
    sql: |
      INSTALL httpfs;
      LOAD httpfs;
  - name: configure s3
    sql: |
      SET s3_region='us-east-1';
      SET s3_url_style='path';
      SET s3_endpoint='{{ SQLFLOW_S3_ENDPOINT }}';
      SET s3_access_key_id='{{ SQLFLOW_S3_ACCESS_KEY_ID }}';
      SET s3_secret_access_key='{{ SQLFLOW_S3_SECRET_ACCESS_KEY }}';
      SET s3_use_ssl=false;

pipeline:
  name: metering-archive
  # Larger batches than count: one Parquet object per batch, and an object is
  # worth reading back at a few thousand rows. The interval bounds the wait
  # at low volume so the demo's recompute step has a file within seconds.
  batch_size: 5000
  flush_interval_seconds: 5

  source:
    type: kafka
    kafka:
      brokers: ["{{ SQLFLOW_KAFKA_BROKERS }}"]
      group_id: metering-archive
      auto_offset_reset: earliest
      topics: ["usage.events"]

  handler:
    type: handlers.InferredMemBatch
    sql: |
      SELECT id, customer, meter, quantity,
             make_timestamptz(ts_ms * 1000) AS ts,
             strftime(make_timestamptz(ts_ms * 1000), '%Y-%m-%d') AS day,
             strftime(make_timestamptz(ts_ms * 1000), '%H') AS hour
      FROM batch

  sink:
    type: sqlcommand
    sqlcommand:
      sql: |
        COPY sqlflow_sink_batch
          TO 's3://usage/raw'
        (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (day, hour),
         FILENAME_PATTERN '{uuid}', APPEND);
```

- [ ] **Step 2: Validate**

Run: `docker run --rm -v "$PWD/config:/config:ro" --env-file .env sqlflow:dev validate /config/archive.yml`
Expected: `archive.yml: valid`.

- [ ] **Step 3: Add the service**

Append to `docker-compose.yml` `services:`:

```yaml
  archive:
    image: ${SQLFLOW_IMAGE}
    command: ["run", "/config/archive.yml", "--metrics", "prometheus"]
    env_file: .env
    volumes: ["./config:/config:ro"]
    depends_on:
      kafka: {condition: service_healthy}
      minio-init: {condition: service_completed_successfully}
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8000 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"ok\"' <&3"]
      interval: 3s
      timeout: 3s
      retries: 30
```

- [ ] **Step 4: Archive an event and read the file back**

Run:

```bash
make up
curl -s -X POST localhost:8001/events -H 'content-type: application/json' \
  -d '{"events":[{"id":"arc_1","customer":"user_9","ts":"2026-09-24T12:07:13Z","quantities":{"requests":1}}]}'
sleep 8
docker compose exec -T minio mc ls -r local/usage/raw/ 2>/dev/null || docker run --rm --network usage-metering_default --entrypoint sh minio/mc:RELEASE.2025-08-13T08-35-41Z -c "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc ls -r local/usage/raw/"
duckdb -c "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='localhost:9000'; SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; SET TimeZone='UTC';
  SELECT id, customer, meter, quantity, ts, day, hour FROM read_parquet('s3://usage/raw/*/*/*.parquet', hive_partitioning=true) WHERE id='arc_1';"
```

Expected: a file under `raw/day=2026-09-24/hour=12/`, and the row `arc_1 user_9 requests 1 2026-09-24 12:07:13+00 2026-09-24 12`. An `hour` other than `12` means the time-zone pin is missing.

- [ ] **Step 5: Commit**

```bash
git add config/archive.yml docker-compose.yml
git commit -m "archive: every raw event to Parquet on MinIO, by UTC day and hour

Its own consumer group and process, because count replays from the low
watermark at every restart and would write raw rows twice. One zstd Parquet
object per batch under raw/day=/hour=/, which the recompute step reads with
Hive partitioning. The session is pinned to UTC: unpinned, strftime put the
partitions in the host's zone. Evidence: arc_1 lands under hour=12."
git push origin reference-stack
```

---

### Task 5: `rollups` and `serve`: hour and day totals, and the read path

**Files:**
- Create: `config/rollups.yml`, `config/serve.yml`
- Modify: `docker-compose.yml` (add `rollups-install`, `rollups`, `serve`)

**Interfaces:**
- Consumes: `usage_per_minute` (Tasks 1, 3).
- Produces: Postgres tables `usage_by_customer_1h` and `usage_by_customer_1d`, columns `(minute, customer, meter, quantity)`; `serve` on `serve:8080` (host `8082`) with datasets `minute_totals`, `invoice`, `quota`, `usage`, each taking `?client_id=demo-console`. The sample-app (Task 7) and the e2e suite (Task 8) read these; the console plan reads all four.

- [ ] **Step 1: The rollup declaration**

`config/rollups.yml`. Verified with `sqlflow rollup ddl` during planning: it generates `usage_by_customer_1h` and `usage_by_customer_1d`, each `GROUP BY` of the minute table folding `kafka_partition` away, with the bucket column still named `minute`.

```yaml
# Usage metering, step four: hour and day totals per customer and meter,
# kept current by triggers as count writes each minute. The dimension set
# folds kafka_partition away, so the rollups are per customer; the minute
# table keeps it because the count workers need one writer per key.
#
#   sqlflow rollup install -c config/rollups.yml   (once, before serve)
#   sqlflow rollup run     -c config/rollups.yml   (its own process)
#   sqlflow rollup verify  -c config/rollups.yml   (recompute and compare)
rollups:
  - name: usage
    source:
      table: usage_per_minute
      time_column: minute
      grain: 1m
      dimensions: [customer, meter, kafka_partition]
    grains:
      1h: {from: 1m}
      1d: {from: 1h}
    dimension_sets:
      - name: usage_by_customer
        dimensions: [customer, meter]
        measures:
          quantity: {type: sum, column: quantity}

store:
  type: postgres
  postgres:
    dsn: "{{ SQLFLOW_POSTGRES_URI }}"
```

- [ ] **Step 2: The serve config**

`config/serve.yml`, validated during planning. All four datasets go through `serve`; nothing reads Postgres directly.

```yaml
# Usage metering, step five: the read path. Four datasets over Postgres, all
# through serve, so every reader gets one number. serve attaches Postgres
# read-only and binds each request's parameters into a fixed statement;
# nothing in a request becomes SQL.
commands:
  - name: pin the session timezone
    sql: SET TimeZone='UTC';
  - name: load postgres
    sql: |
      INSTALL postgres;
      LOAD postgres;
  - name: attach postgres read-only
    sql: ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES, READ_ONLY);

serve:
  http:
    addr: "0.0.0.0:8080"
  # A client id identifies a caller; it does not authenticate one. This
  # stack runs on a private network behind the app's own backend.
  clients:
    - name: demo-console
      id: "{{ SQLFLOW_SERVE_CLIENT_ID }}"
  limits:
    max_rows: 10000
    timeout_seconds: 10
  datasets:
    # The console's main graph: per minute, per meter, over every customer
    # or one. From the minute table, the finest grain.
    - name: minute_totals
      description: Per-minute totals per meter, over every customer or one.
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
        - {name: customer, type: string}
      sql: |
        SELECT minute, meter, sum(quantity)::BIGINT AS quantity
        FROM pg.usage_per_minute
        WHERE minute >= coalesce($since, now() - INTERVAL '1 hour')
          AND minute < coalesce($until, now() + INTERVAL '1 minute')
          AND customer = coalesce($customer, customer)
        GROUP BY 1, 2
        ORDER BY 1, 2
    # The three readers of the use-case page. The rollup tables keep the
    # bucket column's source name, minute, at every grain.
    - name: invoice
      description: A customer's total per meter over a period, from the hourly rollup.
      params:
        - {name: customer, type: string}
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
      sql: |
        SELECT meter, sum(quantity)::BIGINT AS quantity
        FROM pg.usage_by_customer_1h
        WHERE customer = $customer
          AND minute >= coalesce($since, date_trunc('month', now()))
          AND minute < coalesce($until, now() + INTERVAL '1 hour')
        GROUP BY 1
        ORDER BY 1
    - name: quota
      description: A customer's total per meter this calendar month, from the daily rollup, for a quota check.
      params:
        - {name: customer, type: string}
      sql: |
        SELECT meter, sum(quantity)::BIGINT AS quantity
        FROM pg.usage_by_customer_1d
        WHERE customer = $customer
          AND minute >= date_trunc('month', now())
        GROUP BY 1
        ORDER BY 1
    - name: usage
      description: A customer's totals per meter by day, from the daily rollup, for a usage page.
      params:
        - {name: customer, type: string}
        - {name: since, type: timestamp}
      sql: |
        SELECT minute AS day, meter, quantity
        FROM pg.usage_by_customer_1d
        WHERE customer = $customer
          AND minute >= coalesce($since, now() - INTERVAL '30 days')
        ORDER BY 1, 2
```

- [ ] **Step 3: Validate both and print the generated DDL**

Run:

```bash
docker run --rm -v "$PWD/config:/config:ro" --env-file .env sqlflow:dev validate /config/rollups.yml
docker run --rm -v "$PWD/config:/config:ro" --env-file .env sqlflow:dev validate /config/serve.yml
docker run --rm -v "$PWD/config:/config:ro" --env-file .env sqlflow:dev rollup ddl -c /config/rollups.yml | grep -E 'CREATE (TABLE|UNIQUE INDEX)'
```

Expected: both `valid`; the DDL names `usage_by_customer_1h` and `usage_by_customer_1d` and their unique indexes. Any other table name means a serve dataset's `FROM` is wrong.

- [ ] **Step 4: Add the services, ordered**

`serve` prepares its statements at start against tables `rollup install` creates, so `install` is a one-shot service that `serve` waits for. `rollups` (`run`) is its own long-running process, as the sql-flow README prescribes. Append to `docker-compose.yml` `services:`:

```yaml
  # Creates the hour and day tables and their triggers, once, then exits.
  # serve waits for it: its statements are prepared against these tables.
  rollups-install:
    image: ${SQLFLOW_IMAGE}
    command: ["rollup", "install", "-c", "/config/rollups.yml"]
    env_file: .env
    volumes: ["./config:/config:ro"]
    depends_on:
      postgres: {condition: service_healthy}

  # Leads the rollups: backfills what install marked, and checks each table's
  # newest buckets against its source every minute.
  rollups:
    image: ${SQLFLOW_IMAGE}
    command: ["rollup", "run", "-c", "/config/rollups.yml", "--metrics", "prometheus"]
    env_file: .env
    volumes: ["./config:/config:ro"]
    depends_on:
      rollups-install: {condition: service_completed_successfully}
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8000 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"ok\"' <&3"]
      interval: 3s
      timeout: 3s
      retries: 30

  serve:
    image: ${SQLFLOW_IMAGE}
    command: ["serve", "-c", "/config/serve.yml"]
    env_file: .env
    volumes: ["./config:/config:ro"]
    ports: ["8082:8080"]
    depends_on:
      rollups-install: {condition: service_completed_successfully}
    healthcheck:
      test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"ok\"' <&3"]
      interval: 3s
      timeout: 3s
      retries: 30
```

- [ ] **Step 5: Roll up a minute and read it through every dataset**

Run (after the Task 3 smoke events for `user_7` have counted; post them again if the stack was recreated):

```bash
make up
sleep 15
docker compose exec -T postgres psql -U metering -d metering -c \
  "SELECT 'minute' g, sum(quantity) FROM usage_per_minute WHERE customer='user_7' AND meter='requests'
   UNION ALL SELECT '1h', sum(quantity) FROM usage_by_customer_1h WHERE customer='user_7' AND meter='requests'
   UNION ALL SELECT '1d', sum(quantity) FROM usage_by_customer_1d WHERE customer='user_7' AND meter='requests'"
docker compose run --rm --no-deps -T rollups rollup verify -c /config/rollups.yml; echo "verify rc=$?"
for d in invoice quota usage; do echo "== $d"; curl -s "localhost:8082/v1/datasets/$d?client_id=demo-console&customer=user_7"; echo; done
curl -s "localhost:8082/v1/datasets/minute_totals?client_id=demo-console&customer=user_7"
```

Expected: `minute`, `1h` and `1d` all `3`; `verify rc=0`; `invoice` and `quota` each return `{"meter":"requests","quantity":3}` (and `input_tokens` 30); `usage` returns one day row per meter; `minute_totals` returns the minute's two rows. The same number from every reader is the claim; a difference here is a defect, not a timing issue, because `verify` just proved the rollups match their source.

- [ ] **Step 6: Commit**

```bash
git add config/rollups.yml config/serve.yml docker-compose.yml
git commit -m "rollups and serve: hour and day totals, and the read path

One rollup declaration folds kafka_partition away into usage_by_customer_1h
and _1d, kept current by triggers and verified by rollup verify. serve
answers four datasets over Postgres, read-only, with every reader of the
page reading the same tables. rollup install runs once before serve, which
prepares its statements against the tables it creates. Evidence: user_7's
requests read 3 at every grain and from every dataset, and verify exits 0."
git push origin reference-stack
```

---

### Task 6: The TypeScript SDK

**Files:**
- Create: `sdk/package.json`, `sdk/tsconfig.json`, `sdk/src/meter.ts`, `sdk/src/meter.test.ts`

**Interfaces:**
- Produces, in package `@turbolytics/usage-metering-sdk` (workspace-local, not published):
  ```ts
  type Quantities = Record<string, number>;
  type Event = { id: string; customer: string; ts: string; quantities: Quantities };
  type MeterOptions = {
    url: string;                 // ingest, e.g. http://ingest:8001
    flushMs?: number;            // default 1000
    maxBatch?: number;           // default 100
    maxBuffered?: number;        // default 10000
    retryBudgetMs?: number;      // default 60000
    retryBaseMs?: number;        // default 200; tests lower it
    serve?: { url: string; clientId: string };   // for usage()
  };
  class Meter {
    constructor(opts: MeterOptions);
    track(customer: string, quantities: Quantities): void;   // never blocks, never throws
    onAck(fn: (events: Event[]) => void): void;              // once per batch ingest answered 200
    flush(): Promise<void>;                                  // send what is buffered now
    stats(): { tracked: number; acked: number; dropped: number; buffered: number };
    usage(customer: string, meter: string, opts?: { since?: string; until?: string }): Promise<number>;
    close(): Promise<void>;
  }
  ```
  Task 7's sample-app builds its sent ledger on `onAck`, and the e2e suite (Task 8) asserts the SDK's retry keeps ids.

- [ ] **Step 1: Package files**

`sdk/package.json`:

```json
{
  "name": "@turbolytics/usage-metering-sdk",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "dist/meter.js",
  "types": "dist/meter.d.ts",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run"
  },
  "devDependencies": {
    "typescript": "^5.6.0",
    "vitest": "^2.1.0",
    "@types/node": "^22.0.0"
  }
}
```

`sdk/tsconfig.json`:

```json
{
  "extends": "../tsconfig.json",
  "compilerOptions": { "outDir": "dist", "rootDir": "src", "types": ["node"] },
  "include": ["src/**/*.ts"],
  "exclude": ["src/**/*.test.ts"]
}
```

- [ ] **Step 2: Write the failing tests**

`sdk/src/meter.test.ts`. A tiny `http` server stands in for ingest; each test scripts its answers.

```ts
import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { Meter, type Event } from "./meter.js";

type Received = { ids: string[]; body: { events: Event[] } };

// A fake ingest: answers from `codes` in order, repeating the last; records
// every request's ids so a test can see what a retry sent.
function fakeIngest(codes: number[]): Promise<{ server: Server; url: string; received: Received[] }> {
  const received: Received[] = [];
  let n = 0;
  const server = createServer((req, res) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      const body = JSON.parse(data) as { events: Event[] };
      received.push({ ids: body.events.map((e) => e.id), body });
      const code = codes[Math.min(n++, codes.length - 1)];
      res.writeHead(code, { "content-type": "application/json" });
      res.end(code === 200 ? '{"status":"flushed"}' : '{"detail":"Not flushed"}');
    });
  });
  return new Promise((resolve) =>
    server.listen(0, "127.0.0.1", () => {
      const a = server.address() as { port: number };
      resolve({ server, url: `http://127.0.0.1:${a.port}`, received });
    }),
  );
}

let servers: Server[] = [];
afterEach(() => {
  for (const s of servers) s.close();
  servers = [];
});

describe("Meter", () => {
  it("track never throws, even with nowhere to send", () => {
    const m = new Meter({ url: "http://127.0.0.1:1", flushMs: 60_000 });
    expect(() => m.track("user_1", { requests: 1 })).not.toThrow();
    expect(m.stats().tracked).toBe(1);
  });

  it("a batch that gets 200 is acked once, with its events", async () => {
    const f = await fakeIngest([200]);
    servers.push(f.server);
    const m = new Meter({ url: f.url, flushMs: 60_000 });
    const acked: Event[][] = [];
    m.onAck((evs) => acked.push(evs));
    m.track("user_1", { requests: 1, input_tokens: 5 });
    m.track("user_2", { requests: 1 });
    await m.flush();
    expect(acked).toHaveLength(1);
    expect(acked[0].map((e) => e.customer)).toEqual(["user_1", "user_2"]);
    expect(m.stats().acked).toBe(2);
  });

  it("a retry sends the same ids, and acks once", async () => {
    const f = await fakeIngest([503, 200]);
    servers.push(f.server);
    const m = new Meter({ url: f.url, flushMs: 60_000, retryBaseMs: 1 });
    const acked: Event[][] = [];
    m.onAck((evs) => acked.push(evs));
    m.track("user_1", { requests: 1 });
    await m.flush();
    expect(f.received).toHaveLength(2);
    expect(f.received[1].ids).toEqual(f.received[0].ids);
    expect(acked).toHaveLength(1);
  });

  it("a failed batch is never acked, and is dropped once the retry budget ends", async () => {
    const f = await fakeIngest([503]);
    servers.push(f.server);
    const m = new Meter({ url: f.url, flushMs: 60_000, retryBaseMs: 1, retryBudgetMs: 20 });
    let acks = 0;
    m.onAck(() => acks++);
    m.track("user_1", { requests: 1 });
    await m.flush();
    expect(acks).toBe(0);
    expect(m.stats().dropped).toBe(1);
    expect(m.stats().buffered).toBe(0);
  });

  it("the buffer is bounded: the oldest is dropped and counted", () => {
    const m = new Meter({ url: "http://127.0.0.1:1", flushMs: 60_000, maxBuffered: 3 });
    for (let i = 0; i < 5; i++) m.track(`user_${i}`, { requests: 1 });
    const s = m.stats();
    expect(s.buffered).toBe(3);
    expect(s.dropped).toBe(2);
  });

  it("each tracked event has one id, reused on retry, and a ts", async () => {
    const f = await fakeIngest([503, 200]);
    servers.push(f.server);
    const m = new Meter({ url: f.url, flushMs: 60_000, retryBaseMs: 1 });
    m.track("user_1", { requests: 1 });
    await m.flush();
    const ev = f.received[0].body.events[0];
    expect(ev.id).toMatch(/^evt_/);
    expect(new Date(ev.ts).toISOString()).toBe(ev.ts);
    expect(f.received[1].body.events[0].id).toBe(ev.id);
  });
});
```

- [ ] **Step 3: Run them to see them fail**

Run: `npm ci && cd sdk && npx vitest run`
Expected: fail to compile: `Cannot find module './meter.js'`.

- [ ] **Step 4: Implement the SDK**

`sdk/src/meter.ts`:

```ts
// The usage-metering SDK. track() never blocks the app and never throws: it
// appends to a bounded buffer and returns. A background loop sends the buffer
// as one request every flushMs or at maxBatch events. A non-2xx or a network
// error retries the same events, with the same ids, with backoff and jitter
// for at most retryBudgetMs; that budget sits inside the count window's
// dedupe horizon, so a retry is always deduplicated. Past the budget the
// batch is dropped and counted, never silently.
import { randomUUID } from "node:crypto";

export type Quantities = Record<string, number>;
export type Event = { id: string; customer: string; ts: string; quantities: Quantities };

export type MeterOptions = {
  url: string;
  flushMs?: number;
  maxBatch?: number;
  maxBuffered?: number;
  retryBudgetMs?: number;
  retryBaseMs?: number;
  serve?: { url: string; clientId: string };
};

export type Stats = { tracked: number; acked: number; dropped: number; buffered: number };

export class Meter {
  private readonly url: string;
  private readonly flushMs: number;
  private readonly maxBatch: number;
  private readonly maxBuffered: number;
  private readonly retryBudgetMs: number;
  private readonly retryBaseMs: number;
  private readonly serve?: { url: string; clientId: string };

  private buffer: Event[] = [];
  private acks: Array<(events: Event[]) => void> = [];
  private counts = { tracked: 0, acked: 0, dropped: 0 };
  private timer?: NodeJS.Timeout;
  private inflight: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(opts: MeterOptions) {
    this.url = opts.url.replace(/\/$/, "");
    this.flushMs = opts.flushMs ?? 1000;
    this.maxBatch = opts.maxBatch ?? 100;
    this.maxBuffered = opts.maxBuffered ?? 10_000;
    this.retryBudgetMs = opts.retryBudgetMs ?? 60_000;
    this.retryBaseMs = opts.retryBaseMs ?? 200;
    this.serve = opts.serve;
    this.schedule();
  }

  // Never blocks, never throws. The id is generated once here and travels
  // with every retry, which is what makes a retry safe to count.
  track(customer: string, quantities: Quantities): void {
    if (this.closed) return;
    this.counts.tracked++;
    if (this.buffer.length >= this.maxBuffered) {
      this.buffer.shift();
      this.counts.dropped++;
    }
    this.buffer.push({
      id: `evt_${randomUUID()}`,
      customer,
      ts: new Date().toISOString(),
      quantities,
    });
    if (this.buffer.length >= this.maxBatch) void this.flush();
  }

  // Called once for each batch ingest answered 200, with its events, after
  // the answer: a 200 from ack after_flush means the batch is in Kafka.
  onAck(fn: (events: Event[]) => void): void {
    this.acks.push(fn);
  }

  stats(): Stats {
    return { ...this.counts, buffered: this.buffer.length };
  }

  // Sends what is buffered now. Sends are serialized, so a flush that starts
  // while a retry is in progress waits for it: batches reach ingest in order.
  flush(): Promise<void> {
    this.inflight = this.inflight.then(() => this.send());
    return this.inflight;
  }

  // A customer's total for a meter over a period, read from serve's invoice
  // dataset: the same number a bill reads.
  async usage(customer: string, meter: string, opts: { since?: string; until?: string } = {}): Promise<number> {
    if (!this.serve) throw new Error("Meter: usage() needs the serve option");
    const q = new URLSearchParams({ client_id: this.serve.clientId, customer });
    if (opts.since) q.set("since", opts.since);
    if (opts.until) q.set("until", opts.until);
    const res = await fetch(`${this.serve.url.replace(/\/$/, "")}/v1/datasets/invoice?${q}`);
    if (!res.ok) throw new Error(`serve answered ${res.status}`);
    const body = (await res.json()) as { rows: Array<{ meter: string; quantity: number }> };
    return body.rows.find((r) => r.meter === meter)?.quantity ?? 0;
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    await this.flush();
  }

  private schedule(): void {
    if (this.closed) return;
    this.timer = setTimeout(() => {
      void this.flush().finally(() => this.schedule());
    }, this.flushMs);
    this.timer.unref?.();
  }

  private async send(): Promise<void> {
    if (this.buffer.length === 0) return;
    const batch = this.buffer.splice(0, this.maxBatch);
    const deadline = Date.now() + this.retryBudgetMs;
    for (let attempt = 0; ; attempt++) {
      if (await this.post(batch)) {
        this.counts.acked += batch.length;
        for (const fn of this.acks) fn(batch);
        if (this.buffer.length > 0) void this.flush();
        return;
      }
      const wait = Math.min(this.retryBaseMs * 2 ** attempt, 5000) * (0.5 + Math.random());
      if (Date.now() + wait > deadline) {
        this.counts.dropped += batch.length;
        return;
      }
      await new Promise((r) => setTimeout(r, wait));
    }
  }

  private async post(events: Event[]): Promise<boolean> {
    try {
      const res = await fetch(`${this.url}/events`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ events }),
      });
      return res.ok;
    } catch {
      return false;
    }
  }
}
```

- [ ] **Step 5: Run the tests**

Run: `cd sdk && npx vitest run && npm run build`
Expected: 6 passed; `tsc` writes `dist/meter.js` and `dist/meter.d.ts`.

- [ ] **Step 6: Commit**

```bash
git add sdk/
git commit -m "sdk: a Meter that never blocks the app and retries with the same ids

track appends to a bounded buffer and returns; a loop sends it every flushMs
or at maxBatch. A failed batch retries with the same ids, with backoff and
jitter, for at most retryBudgetMs, which sits inside the count window's
dedupe horizon, then is dropped and counted. onAck fires once per batch
ingest answered 200, which the sample app's sent ledger is built on. Six
unit tests against a scripted fake ingest."
git push origin reference-stack
```

---

### Task 7: `sample-app`: a metered fake LLM API, its load generator, and the sent ledger

**Files:**
- Create: `sample-app/package.json`, `sample-app/tsconfig.json`, `sample-app/Dockerfile`, `sample-app/src/ledger.ts`, `sample-app/src/ledger.test.ts`, `sample-app/src/server.ts`
- Modify: `docker-compose.demo.yml` (add `sample-app`), `README.md` (step 1)

**Interfaces:**
- Consumes: the SDK (Task 6), `ingest:8001` (Task 2), `serve:8080` (Task 5).
- Produces: `sample-app:8003`, in the demo overlay. Routes: `POST /v1/chat {user}` (the customer-facing fake API, metered); `POST /load {users, rate}`; `POST /load/stop`; `GET /status` → `{running, users, rate, stats, ledgerStartedAt}`; `GET /sent` → `{startedAt, totals: [{minute, customer, meter, quantity}]}`; `GET /healthz`. The console plan drives `/load` and reads `/sent` and `/status`.
- The ledger counts only events ingest acknowledged. "Sent" on the page means "answered 200", and with `ack: after_flush` that means "in Kafka".

- [ ] **Step 1: Write the failing ledger tests**

`sample-app/src/ledger.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { Ledger } from "./ledger.js";
import type { Event } from "@turbolytics/usage-metering-sdk";

const at = (iso: string, customer: string, q: Record<string, number>, id = "x"): Event =>
  ({ id, customer, ts: iso, quantities: q });

describe("Ledger", () => {
  it("sums acknowledged quantities per minute, customer and meter", () => {
    const l = new Ledger();
    l.record([
      at("2026-09-24T12:07:13Z", "user_1", { requests: 1, input_tokens: 5 }, "a"),
      at("2026-09-24T12:07:59Z", "user_1", { requests: 1, input_tokens: 7 }, "b"),
      at("2026-09-24T12:08:00Z", "user_1", { requests: 1 }, "c"),
    ]);
    const t = l.snapshot().totals;
    expect(t).toContainEqual({ minute: "2026-09-24T12:07:00.000Z", customer: "user_1", meter: "requests", quantity: 2 });
    expect(t).toContainEqual({ minute: "2026-09-24T12:07:00.000Z", customer: "user_1", meter: "input_tokens", quantity: 12 });
    expect(t).toContainEqual({ minute: "2026-09-24T12:08:00.000Z", customer: "user_1", meter: "requests", quantity: 1 });
  });

  it("counts an id once, however many times it is acknowledged", () => {
    const l = new Ledger();
    const e = at("2026-09-24T12:07:13Z", "user_1", { requests: 1 }, "same");
    l.record([e]);
    l.record([e]);
    expect(l.snapshot().totals).toEqual([
      { minute: "2026-09-24T12:07:00.000Z", customer: "user_1", meter: "requests", quantity: 1 },
    ]);
  });

  it("reports when it started, so minutes before it read as unknown", () => {
    const before = Date.now();
    const l = new Ledger();
    expect(new Date(l.snapshot().startedAt).getTime()).toBeGreaterThanOrEqual(before);
  });
});
```

- [ ] **Step 2: Run them to see them fail**

Run: `cd sample-app && npx vitest run`
Expected: `Cannot find module './ledger.js'`.

- [ ] **Step 3: Implement the ledger**

`sample-app/src/ledger.ts`:

```ts
// What the app knows it sent: quantities per minute, customer and meter,
// recorded only from events ingest acknowledged, so "sent" means "in Kafka".
// An id is counted once however many acks name it, which a retry whose
// first answer was lost can cause. In memory: a restart clears it and
// startedAt says so, so a console shows "sent unknown" rather than a false
// mismatch for earlier minutes.
import type { Event } from "@turbolytics/usage-metering-sdk";

export type Total = { minute: string; customer: string; meter: string; quantity: number };

export class Ledger {
  readonly startedAt = new Date().toISOString();
  private totals = new Map<string, Total>();
  private seen = new Set<string>();

  record(events: Event[]): void {
    for (const e of events) {
      if (this.seen.has(e.id)) continue;
      this.seen.add(e.id);
      const minute = new Date(e.ts);
      minute.setUTCSeconds(0, 0);
      const m = minute.toISOString();
      for (const [meter, quantity] of Object.entries(e.quantities)) {
        const key = `${m}\u0000${e.customer}\u0000${meter}`;
        const cur = this.totals.get(key);
        if (cur) cur.quantity += quantity;
        else this.totals.set(key, { minute: m, customer: e.customer, meter, quantity });
      }
    }
  }

  snapshot(): { startedAt: string; totals: Total[] } {
    return { startedAt: this.startedAt, totals: [...this.totals.values()] };
  }
}
```

- [ ] **Step 4: Run the ledger tests**

Run: `cd sample-app && npx vitest run`
Expected: 3 passed.

- [ ] **Step 5: The server, the fake API, and the load generator**

`sample-app/src/server.ts`:

```ts
// A fake LLM API with many users, metered per user by the SDK: what a
// customer's app looks like with metering added. Plus what the demo needs
// from it: a load generator that drives the fake API for N synthetic users
// at R events a second, and the sent ledger behind GET /sent.
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { Meter } from "@turbolytics/usage-metering-sdk";
import { Ledger } from "./ledger.js";

const INGEST = process.env.INGEST_URL ?? "http://ingest:8001";
const SERVE = process.env.SERVE_URL ?? "http://serve:8080";
const CLIENT_ID = process.env.SQLFLOW_SERVE_CLIENT_ID ?? "demo-console";
const MAX_USERS = 10_000;
const MAX_RATE = 2_000;

const meter = new Meter({ url: INGEST, flushMs: 1000, maxBatch: 200, serve: { url: SERVE, clientId: CLIENT_ID } });
const ledger = new Ledger();
meter.onAck((events) => ledger.record(events));

// The metered operation: one request, some input tokens, some output tokens.
function chat(user: string): { completion: string; usage: Record<string, number> } {
  const usage = {
    requests: 1,
    input_tokens: 100 + Math.floor(Math.random() * 900),
    output_tokens: 50 + Math.floor(Math.random() * 400),
  };
  meter.track(user, usage);
  return { completion: "ok", usage };
}

let load: { users: number; rate: number; timer: NodeJS.Timeout } | undefined;

function startLoad(users: number, rate: number): void {
  stopLoad();
  users = Math.max(1, Math.min(MAX_USERS, Math.floor(users)));
  rate = Math.max(1, Math.min(MAX_RATE, Math.floor(rate)));
  // A tick every 50ms, sending rate/20 events spread over random users.
  const perTick = rate / 20;
  let carry = 0;
  const timer = setInterval(() => {
    carry += perTick;
    for (; carry >= 1; carry--) chat(`user_${String(Math.floor(Math.random() * users)).padStart(5, "0")}`);
  }, 50);
  load = { users, rate, timer };
}

function stopLoad(): void {
  if (load) clearInterval(load.timer);
  load = undefined;
}

async function body(req: IncomingMessage): Promise<Record<string, unknown>> {
  let data = "";
  for await (const c of req) data += c;
  return data ? (JSON.parse(data) as Record<string, unknown>) : {};
}

function json(res: ServerResponse, code: number, v: unknown): void {
  res.writeHead(code, { "content-type": "application/json" });
  res.end(JSON.stringify(v));
}

createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", "http://x");
    if (req.method === "GET" && url.pathname === "/healthz") return json(res, 200, { status: "ok" });
    if (req.method === "POST" && url.pathname === "/v1/chat") {
      const b = await body(req);
      return json(res, 200, chat(String(b.user ?? "anonymous")));
    }
    if (req.method === "POST" && url.pathname === "/load") {
      const b = await body(req);
      startLoad(Number(b.users ?? 500), Number(b.rate ?? 200));
      return json(res, 200, { running: true, users: load!.users, rate: load!.rate });
    }
    if (req.method === "POST" && url.pathname === "/load/stop") {
      stopLoad();
      return json(res, 200, { running: false });
    }
    if (req.method === "GET" && url.pathname === "/status") {
      return json(res, 200, {
        running: !!load, users: load?.users ?? 0, rate: load?.rate ?? 0,
        stats: meter.stats(), ledgerStartedAt: ledger.startedAt,
      });
    }
    if (req.method === "GET" && url.pathname === "/sent") return json(res, 200, ledger.snapshot());
    json(res, 404, { detail: "not found" });
  } catch (err) {
    json(res, 500, { detail: String(err) });
  }
}).listen(8003, () => console.log("sample-app on :8003"));

process.on("SIGTERM", () => { stopLoad(); void meter.close().then(() => process.exit(0)); });
```

`sample-app/package.json`:

```json
{
  "name": "sample-app",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": { "build": "tsc -p tsconfig.json", "start": "node dist/server.js", "test": "vitest run" },
  "dependencies": { "@turbolytics/usage-metering-sdk": "*" },
  "devDependencies": { "typescript": "^5.6.0", "vitest": "^2.1.0", "@types/node": "^22.0.0" }
}
```

`sample-app/tsconfig.json` is identical to `sdk/tsconfig.json`.

- [ ] **Step 6: Dockerfile and the demo service**

`sample-app/Dockerfile` (built from the repo root, so it can see the SDK workspace):

```dockerfile
FROM node:22-alpine
WORKDIR /app
COPY package.json tsconfig.json ./
COPY sdk ./sdk
COPY sample-app ./sample-app
RUN npm ci --workspaces --include-workspace-root=false \
 && npm run build -w sdk && npm run build -w sample-app
EXPOSE 8003
CMD ["node", "sample-app/dist/server.js"]
```

Append to `docker-compose.demo.yml` `services:`:

```yaml
  # The customer's app, faked: an LLM API metered per user by the SDK, with
  # the load generator and sent ledger the demo drives and reads.
  sample-app:
    build: {context: ., dockerfile: sample-app/Dockerfile}
    environment:
      INGEST_URL: http://ingest:8001
      SERVE_URL: http://serve:8080
      SQLFLOW_SERVE_CLIENT_ID: ${SQLFLOW_SERVE_CLIENT_ID}
    ports: ["8003:8003"]
    depends_on:
      ingest: {condition: service_healthy}
      serve: {condition: service_healthy}
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8003/healthz | grep -q ok"]
      interval: 3s
      timeout: 3s
      retries: 20
```

- [ ] **Step 7: Generate load and reconcile one minute by hand**

Run:

```bash
make demo
make load USERS=50 RATE=20
sleep 90
curl -s -X POST localhost:8003/load/stop
curl -s localhost:8003/status
# One closed minute: what the app says it sent vs what Postgres counted.
M=$(curl -s localhost:8003/sent | python3 -c 'import json,sys; t=json.load(sys.stdin)["totals"]; print(sorted({x["minute"] for x in t})[0])')
echo "minute $M"
curl -s localhost:8003/sent | python3 -c "import json,sys; t=json.load(sys.stdin)['totals']; print('sent', sum(x['quantity'] for x in t if x['minute']=='$M' and x['meter']=='requests'))"
docker compose exec -T postgres psql -U metering -d metering -tAc "SELECT 'counted', sum(quantity) FROM usage_per_minute WHERE meter='requests' AND minute = '$M'"
```

Expected: `/status` shows `running: false`, `stats.acked` equal to `stats.tracked` and `dropped: 0`; the sent and counted totals for the first full minute are equal. (A later minute may still be open; the e2e suite in Task 8 waits for closes properly.)

- [ ] **Step 8: README step 1, and commit**

Under "1. The app sends an event" in `README.md`, add:

```markdown
The SDK buffers `track` calls and sends them in batches to `ingest`, which
answers 200 only once the batch is in Kafka. The same event id travels with
every retry, so a retry is safe.

    const meter = new Meter({ url: "http://ingest:8001" });
    meter.track("user_48213", { requests: 1, input_tokens: 812, output_tokens: 214 });

    curl -s -X POST localhost:8001/events -H 'content-type: application/json' \
      -d '{"events":[{"id":"evt_1","customer":"user_1","ts":"2026-09-24T12:07:13Z","quantities":{"requests":1}}]}'
    {"status":"flushed"}

`sample-app` is that integration, faked: `POST /v1/chat` meters one request;
`make load USERS=500 RATE=200` drives it for many users.
```

```bash
git add sample-app/ docker-compose.demo.yml README.md
git commit -m "sample-app: a metered fake LLM API, its load generator, and the sent ledger

What a customer's app looks like with the SDK, plus the two things the demo
needs from it: POST /load drives the fake API for N users at R events a
second, and GET /sent reports what ingest acknowledged, per minute,
customer and meter, counting an id once. Evidence: a 90-second run at 20
events a second reconciles its first closed minute against Postgres."
git push origin reference-stack
```

---

### Task 8: The end-to-end suite: the page's guarantees as tests

**Files:**
- Create: `tests/package.json`, `tests/tsconfig.json`, `tests/vitest.config.ts`, `tests/helpers.ts`, `tests/stack.test.ts`

**Interfaces:**
- Consumes: everything above, running under `make demo`. Reaches `ingest` on `localhost:8001`, `serve` on `localhost:8082`, Postgres on `localhost:5432`, Kafka on `localhost:9092`, MinIO on `localhost:9000`, and `docker compose` for failures.
- Produces: `make test`. Each scenario is one guarantee on the use-case page; the console plan's tour steps assert the same conditions.

Every scenario feeds a fixed workload whose exact answer is computed in the test, posts it through the real `ingest`, and waits for Postgres to hold exactly that answer and then to keep holding it (a partial republish landing after the right answer is the failure the rebalance scenarios exist for). Event times sit a few minutes in the past so minutes close at once; a burst arrives in time order, so nothing in it is late.

- [ ] **Step 1: Package files**

`tests/package.json`:

```json
{
  "name": "tests",
  "private": true,
  "type": "module",
  "scripts": { "test": "vitest run" },
  "dependencies": {
    "@turbolytics/usage-metering-sdk": "*",
    "kafkajs": "^2.2.4",
    "pg": "^8.13.0"
  },
  "devDependencies": { "typescript": "^5.6.0", "vitest": "^2.1.0", "@types/node": "^22.0.0", "@types/pg": "^8.11.0" }
}
```

`tests/vitest.config.ts`:

```ts
import { defineConfig } from "vitest/config";
// One scenario at a time: they share one stack and kill its workers.
export default defineConfig({ test: { testTimeout: 600_000, hookTimeout: 120_000, fileParallelism: false, sequence: { concurrent: false } } });
```

`tests/tsconfig.json` is identical to `sdk/tsconfig.json` with `"include": ["*.ts"]`.

- [ ] **Step 2: The helpers**

`tests/helpers.ts`:

```ts
import { execSync } from "node:child_process";
import { Kafka } from "kafkajs";
import pg from "pg";

export const INGEST = "http://localhost:8001";
export const SERVE = "http://localhost:8082";
export const CLIENT_ID = process.env.SQLFLOW_SERVE_CLIENT_ID ?? "demo-console";
const PG = "postgresql://metering:metering@localhost:5432/metering";
const KAFKA = ["localhost:9092"];
const COMPOSE = "docker compose -f docker-compose.yml -f docker-compose.demo.yml";

export type Ev = { id: string; customer: string; meter: string; quantity: number; ts: string };
export type Key = string; // `${minuteISO}|${customer}|${meter}`

// A fixed event set: customers x perCustomer events, each with three meters,
// spread evenly over span from start. The seed makes a failure reproducible.
export function workload(seed: number, customers: number, perCustomer: number, start: Date, spanMs: number): Ev[] {
  let s = seed;
  const rnd = () => ((s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const out: Ev[] = [];
  const n = customers * perCustomer;
  for (let i = 0; i < n; i++) {
    const customer = `user_${String(i % customers).padStart(5, "0")}`;
    const ts = new Date(start.getTime() + Math.floor((spanMs * i) / n)).toISOString();
    const id = `evt_${seed}_${i}`;
    out.push({ id, customer, meter: "requests", quantity: 1, ts });
    out.push({ id, customer, meter: "input_tokens", quantity: 100 + Math.floor(rnd() * 900), ts });
    out.push({ id, customer, meter: "output_tokens", quantity: 50 + Math.floor(rnd() * 400), ts });
  }
  return out;
}

export function minuteOf(ts: string): string {
  const d = new Date(ts);
  d.setUTCSeconds(0, 0);
  return d.toISOString();
}

// The exact answer: sum per (minute, customer, meter) over distinct (id, meter).
export function expected(events: Ev[]): Map<Key, number> {
  const seen = new Set<string>();
  const out = new Map<Key, number>();
  for (const e of events) {
    const k = `${e.id}|${e.meter}`;
    if (seen.has(k)) continue;
    seen.add(k);
    const key = `${minuteOf(e.ts)}|${e.customer}|${e.meter}`;
    out.set(key, (out.get(key) ?? 0) + e.quantity);
  }
  return out;
}

// Groups rows by event id into the bodies ingest takes, batch events per
// request, and posts them. Returns the ids answered 200. A code of 0 is a
// request that did not complete, which a sender under a killed ingest sees.
export async function postEvents(events: Ev[], batch = 25): Promise<{ acked: Set<string>; codes: number[] }> {
  const byId = new Map<string, { id: string; customer: string; ts: string; quantities: Record<string, number> }>();
  for (const e of events) {
    const b = byId.get(e.id) ?? { id: e.id, customer: e.customer, ts: e.ts, quantities: {} };
    b.quantities[e.meter] = e.quantity;
    byId.set(e.id, b);
  }
  const bodies = [...byId.values()];
  const acked = new Set<string>();
  const codes: number[] = [];
  for (let i = 0; i < bodies.length; i += batch) {
    const slice = bodies.slice(i, i + batch);
    const code = await postOnce(slice);
    codes.push(code);
    if (code === 200) for (const b of slice) acked.add(b.id);
  }
  return { acked, codes };
}

export async function postOnce(events: object[]): Promise<number> {
  try {
    const res = await fetch(`${INGEST}/events`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ events }),
      signal: AbortSignal.timeout(30_000),
    });
    await res.arrayBuffer();
    return res.status;
  } catch { return 0; }
}

// What a reader of the stack sees: Postgres summed over partitions.
export async function totals(): Promise<Map<Key, number>> {
  const c = new pg.Client(PG);
  await c.connect();
  try {
    const r = await c.query("SELECT minute, customer, meter, sum(quantity)::BIGINT AS q FROM usage_per_minute GROUP BY 1,2,3");
    const out = new Map<Key, number>();
    for (const row of r.rows) out.set(`${new Date(row.minute).toISOString()}|${row.customer}|${row.meter}`, Number(row.q));
    return out;
  } finally { await c.end(); }
}

export async function pgQuery<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T[]> {
  const c = new pg.Client(PG);
  await c.connect();
  try { return (await c.query(sql, params)).rows as T[]; } finally { await c.end(); }
}

export function diff(want: Map<Key, number>, got: Map<Key, number>): string[] {
  const out: string[] = [];
  for (const [k, w] of want) if ((got.get(k) ?? 0) !== w) out.push(`${k} want ${w} got ${got.get(k) ?? 0}`);
  for (const [k, g] of got) if (!want.has(k)) out.push(`${k} want nothing got ${g}`);
  return out.sort();
}

// Waits until Postgres holds exactly want, then holds it for settleMs.
export async function awaitExact(want: Map<Key, number>, timeoutMs: number, settleMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const d = diff(want, await totals());
    if (d.length === 0) {
      const hold = Date.now() + settleMs;
      while (Date.now() < hold) {
        await sleep(500);
        const d2 = diff(want, await totals());
        if (d2.length > 0) throw new Error(`exact, then not: ${d2.length} keys changed after the right answer: ${d2.slice(0, 5).join("; ")}`);
      }
      return;
    }
    if (Date.now() > deadline) throw new Error(`not exact after ${timeoutMs}ms: ${d.length} keys differ: ${d.slice(0, 5).join("; ")}`);
    await sleep(500);
  }
}

export async function serveRows<T>(dataset: string, params: Record<string, string>): Promise<T[]> {
  const q = new URLSearchParams({ client_id: CLIENT_ID, ...params });
  const res = await fetch(`${SERVE}/v1/datasets/${dataset}?${q}`);
  if (!res.ok) throw new Error(`serve ${dataset}: ${res.status} ${await res.text()}`);
  return ((await res.json()) as { rows: T[] }).rows;
}

// Every event id on the topic, read from the beginning with a fresh group.
export async function kafkaIds(): Promise<Set<string>> {
  const kafka = new Kafka({ brokers: KAFKA });
  const consumer = kafka.consumer({ groupId: `tests-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: "usage.events", fromBeginning: true });
  const ids = new Set<string>();
  let last = Date.now();
  await consumer.run({ eachMessage: async ({ message }) => { ids.add((JSON.parse(message.value!.toString()) as { id: string }).id); last = Date.now(); } });
  while (Date.now() - last < 5000) await sleep(250);
  await consumer.disconnect();
  return ids;
}

// The partitions each member of the count group owns.
export async function groupAssignment(): Promise<Map<string, number[]>> {
  const admin = new Kafka({ brokers: KAFKA }).admin();
  await admin.connect();
  try {
    const d = await admin.describeGroups(["metering-count"]);
    const out = new Map<string, number[]>();
    for (const m of d.groups[0]?.members ?? []) {
      const a = (await import("kafkajs")).AssignerProtocol.MemberAssignment.decode(m.memberAssignment);
      out.set(m.memberId, a?.assignment["usage.events"] ?? []);
    }
    return out;
  } finally { await admin.disconnect(); }
}

export async function awaitMembers(n: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const a = await groupAssignment();
    if (a.size === n && [...a.values()].every((p) => p.length > 0)) return;
    if (Date.now() > deadline) throw new Error(`group did not settle on ${n} members within ${timeoutMs}ms`);
    await sleep(500);
  }
}

// Total lag of the count group: head minus committed, summed over partitions.
export async function groupLag(): Promise<number> {
  const admin = new Kafka({ brokers: KAFKA }).admin();
  await admin.connect();
  try {
    const head = await admin.fetchTopicOffsets("usage.events");
    const committed = await admin.fetchOffsets({ groupId: "metering-count", topics: ["usage.events"] });
    const byPart = new Map(committed[0].partitions.map((p) => [p.partition, Number(p.offset)]));
    return head.reduce((lag, p) => lag + Number(p.high) - Math.max(0, byPart.get(p.partition) ?? 0), 0);
  } finally { await admin.disconnect(); }
}

// Runs the duckdb CLI on the host against MinIO, as JSON rows. A test
// prerequisite the README lists: brew install duckdb.
export async function duckdb<T>(sql: string): Promise<T[]> {
  const prelude = "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='localhost:9000'; SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; SET TimeZone='UTC';";
  const out = execSync("duckdb -json", { input: prelude + sql, encoding: "utf8" });
  return out.trim() ? (JSON.parse(out) as T[]) : [];
}

export const compose = {
  kill: (svc: string) => execSync(`${COMPOSE} kill -s SIGKILL ${svc}`, { stdio: "inherit" }),
  stop: (svc: string) => execSync(`${COMPOSE} stop ${svc}`, { stdio: "inherit" }),
  start: (svc: string) => execSync(`${COMPOSE} up -d --wait ${svc}`, { stdio: "inherit" }),
  // A lost disk: remove the container and its state volume, then start fresh.
  loseDisk: (svc: string, volume: string) =>
    execSync(`${COMPOSE} rm -sfv ${svc} && docker volume rm -f ${volume} && ${COMPOSE} up -d --wait ${svc}`, { stdio: "inherit" }),
  exec: (svc: string, cmd: string) => execSync(`${COMPOSE} exec -T ${svc} ${cmd}`, { encoding: "utf8" }),
  run: (svc: string, cmd: string) => execSync(`${COMPOSE} run --rm --no-deps -T ${svc} ${cmd}`, { encoding: "utf8" }),
};

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
export const minutesAgo = (n: number) => { const d = new Date(); d.setUTCSeconds(0, 0); return new Date(d.getTime() - n * 60_000); };
```

- [ ] **Step 3: Write the scenarios**

`tests/stack.test.ts`. Each `it` is one guarantee from the use-case page, named for it. The suite assumes `make demo` is up; `beforeAll` checks and fails fast with the command to run.

```ts
import { beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  awaitExact, awaitMembers, compose, duckdb, expected, groupLag, kafkaIds, minuteOf, minutesAgo,
  pgQuery, postEvents, postOnce, serveRows, sleep, totals, workload, type Ev,
} from "./helpers.js";

// The settle window: a minute closes about 65s after its end (window + grace)
// and the rows land within a second. 95s is that plus margin; a handoff adds
// the 45s session timeout plus margin. The console uses the same numbers.
const SETTLE_MS = 95_000;
const HANDOFF_MS = 75_000;
const HOLD_MS = 20_000;

let seed = Date.now() % 100_000;
let known: Ev[] = []; // every event posted so far: the suite's running answer

beforeAll(async () => {
  const code = await postOnce([]);
  if (code !== 200) throw new Error(`ingest not answering on :8001 (got ${code}); run: make demo`);
  await awaitMembers(2, 60_000);
  // The answer starts from what Postgres already holds.
  known = [];
});

beforeEach(() => { seed += 1; });

// Posts a workload, folds it into the suite's answer, and waits until Postgres
// holds exactly that answer and keeps holding it.
async function postAndSettle(events: Ev[], extraMs = 0): Promise<void> {
  const { acked, codes } = await postEvents(events);
  expect(codes.every((c) => c === 200), `codes: ${[...new Set(codes)]}`).toBe(true);
  expect(acked.size).toBe(new Set(events.map((e) => e.id)).size);
  known.push(...events);
  await awaitExact(mergeWithBaseline(), SETTLE_MS + extraMs, HOLD_MS);
}

let baseline = new Map<string, number>();
function mergeWithBaseline(): Map<string, number> {
  const want = new Map(baseline);
  for (const [k, v] of expected(known)) want.set(k, (want.get(k) ?? 0) + v);
  return want;
}

describe("the use-case page's guarantees", () => {
  beforeAll(async () => { baseline = await totals(); });

  it("exact: a burst of 200 customers x 10 events lands as its exact per-minute totals", async () => {
    const w = workload(seed, 200, 10, minutesAgo(5), 120_000);
    await postAndSettle(w);
  });

  it("retries dedupe: the same bodies posted twice count once", async () => {
    const w = workload(seed, 20, 5, minutesAgo(5), 60_000);
    const first = await postEvents(w);
    expect(first.codes.every((c) => c === 200)).toBe(true);
    // A lost 200: the client retries every batch, same ids.
    const second = await postEvents(w);
    expect(second.codes.every((c) => c === 200)).toBe(true);
    known.push(...w);
    await awaitExact(mergeWithBaseline(), SETTLE_MS, HOLD_MS);
    // Both copies reached Kafka: dedupe happened in the window, not at ingest.
    const ids = await kafkaIds();
    for (const id of new Set(w.map((e) => e.id))) expect(ids.has(id)).toBe(true);
  });

  it("one partition per customer: no customer has rows under two partitions", async () => {
    const rows = await pgQuery<{ customer: string; n: string }>(
      "SELECT customer, count(DISTINCT kafka_partition) AS n FROM usage_per_minute GROUP BY 1 HAVING count(DISTINCT kafka_partition) > 1");
    expect(rows).toEqual([]);
  });

  it("every reader agrees: minute table, hourly and daily rollups, and serve give one number", async () => {
    const w = workload(seed, 10, 20, minutesAgo(5), 60_000);
    await postAndSettle(w);
    await sleep(5000); // the rollup triggers are synchronous; this is for serve's view
    const customer = w[0].customer;
    const want = [...expected(w)].filter(([k]) => k.split("|")[1] === customer);
    const wantByMeter = new Map<string, number>();
    for (const [k, v] of want) wantByMeter.set(k.split("|")[2], (wantByMeter.get(k.split("|")[2]) ?? 0) + v);
    const since = minutesAgo(10).toISOString();
    const [pgMin, pg1h, pg1d] = await Promise.all([
      pgQuery<{ meter: string; q: string }>("SELECT meter, sum(quantity) q FROM usage_per_minute WHERE customer=$1 AND minute >= $2 GROUP BY 1", [customer, since]),
      pgQuery<{ meter: string; q: string }>("SELECT meter, sum(quantity) q FROM usage_by_customer_1h WHERE customer=$1 AND minute >= date_trunc('hour', $2::timestamptz) GROUP BY 1", [customer, since]),
      pgQuery<{ meter: string; q: string }>("SELECT meter, sum(quantity) q FROM usage_by_customer_1d WHERE customer=$1 AND minute >= date_trunc('day', $2::timestamptz) GROUP BY 1", [customer, since]),
    ]);
    const invoice = await serveRows<{ meter: string; quantity: number }>("invoice", { customer, since: minutesAgo(10).toISOString().slice(0, 19) });
    // Every reader's total for this customer includes everything the suite
    // sent for them, so compare each reader to the minute table, the source.
    const asMap = (rows: Array<{ meter: string; q?: string; quantity?: number }>) =>
      new Map(rows.map((r) => [r.meter, Number(r.q ?? r.quantity)]));
    expect(asMap(pg1h)).toEqual(asMap(pgMin));
    expect(asMap(pg1d)).toEqual(asMap(pgMin));
    expect(asMap(invoice)).toEqual(asMap(pgMin));
    for (const [meter, q] of wantByMeter) expect(asMap(pgMin).get(meter)).toBeGreaterThanOrEqual(q);
    // verify exits non-zero on drift (system.rollup.drift), and execSync throws on non-zero.
    expect(() => compose.run("rollups", "rollup verify -c /config/rollups.yml")).not.toThrow();
  });

  it("stop: a worker stopped mid-minute hands its partitions over and the minutes settle exact", async () => {
    const w = workload(seed, 100, 10, minutesAgo(4), 90_000);
    const half = Math.floor(w.length / 2);
    await postEvents(w.slice(0, half));
    compose.stop("count-2");
    await awaitMembers(1, 60_000);
    await postEvents(w.slice(half));
    known.push(...w);
    await awaitExact(mergeWithBaseline(), SETTLE_MS + HANDOFF_MS, HOLD_MS);
    compose.start("count-2");
    await awaitMembers(2, 60_000);
    // Restarting recounts its partitions from Kafka; the answer must not move.
    await awaitExact(mergeWithBaseline(), SETTLE_MS + HANDOFF_MS, HOLD_MS);
  });

  it("crash: a worker killed mid-minute loses nothing and double-counts nothing", async () => {
    const w = workload(seed, 100, 10, minutesAgo(4), 90_000);
    const half = Math.floor(w.length / 2);
    await postEvents(w.slice(0, half));
    compose.kill("count");
    await postEvents(w.slice(half));
    known.push(...w);
    // Kafka waits out the session timeout before moving the dead worker's
    // partitions; then count-2 recounts them from the committed position.
    await awaitMembers(1, 90_000);
    await awaitExact(mergeWithBaseline(), SETTLE_MS + HANDOFF_MS, HOLD_MS);
    compose.start("count");
    await awaitMembers(2, 60_000);
    await awaitExact(mergeWithBaseline(), SETTLE_MS + HANDOFF_MS, HOLD_MS);
  });

  it("lost disk: a worker that starts with no state file rebuilds its open minutes from Kafka", async () => {
    const w = workload(seed, 50, 10, minutesAgo(4), 60_000);
    await postEvents(w.slice(0, w.length / 2));
    compose.loseDisk("count-2", "usage-metering_count-2-state");
    await awaitMembers(2, 90_000);
    await postEvents(w.slice(w.length / 2));
    known.push(...w);
    await awaitExact(mergeWithBaseline(), SETTLE_MS + HANDOFF_MS, HOLD_MS);
  });

  it("ingest crash: a sender that got no 200 retries, and the count is still exact", async () => {
    const w = workload(seed, 20, 10, minutesAgo(4), 60_000);
    compose.kill("ingest");
    const dark = await postEvents(w.slice(0, 50));
    expect(dark.codes.every((c) => c !== 200)).toBe(true);
    compose.start("ingest");
    // The SDK's behavior, by hand: the same bodies, same ids, again.
    const { codes } = await postEvents(w);
    expect(codes.every((c) => c === 200)).toBe(true);
    known.push(...w);
    await awaitExact(mergeWithBaseline(), SETTLE_MS, HOLD_MS);
  });

  it("late events: an event inside allowed lateness is counted and its minute republished whole", async () => {
    const start = minutesAgo(3);
    const w = workload(seed, 10, 5, start, 30_000);
    await postAndSettle(w);
    // One more event for the same, now-closed, minute: 60s after its end,
    // well inside the 120s of lateness.
    const late: Ev = { id: `evt_${seed}_late`, customer: w[0].customer, meter: "requests", quantity: 1, ts: w[0].ts };
    await postAndSettle([late]);
    const key = `${minuteOf(late.ts)}|${late.customer}|requests`;
    expect((await totals()).get(key)).toBe(mergeWithBaseline().get(key));
  });

  it("lag reads as the retained span: committed offsets sit behind the head while minutes are retained", async () => {
    // Not a guarantee but a documented reading: the low-watermark commit
    // keeps each partition's position before its oldest retained row, so
    // consumer-group lag is nonzero for size + grace + lateness after the
    // last event, by design. The README explains it so nobody pages on it.
    const w = workload(seed, 10, 5, minutesAgo(1), 30_000);
    const { codes } = await postEvents(w);
    expect(codes.every((c) => c === 200)).toBe(true);
    known.push(...w);
    await sleep(5000);
    expect(await groupLag()).toBeGreaterThan(0);
  });

  it("archive: every acknowledged event is in Parquet, once", async () => {
    await sleep(6000); // the archive's flush interval
    const [row] = await duckdb<{ n: number; d: number }>(`
      SELECT count(*) AS n, count(DISTINCT id || '|' || meter) AS d
      FROM read_parquet('s3://usage/raw/*/*/*.parquet')`);
    expect(row.n).toBeGreaterThan(0);
    expect(row.n).toBe(row.d); // no (id, meter) written twice
    // Everything the suite sent is there.
    const ids = new Set((await duckdb<{ id: string }>("SELECT DISTINCT id FROM read_parquet('s3://usage/raw/*/*/*.parquet')")).map((r) => r.id));
    for (const id of new Set(known.map((e) => e.id))) expect(ids.has(id), id).toBe(true);
  });
});
```

- [ ] **Step 4: Run the suite against the demo stack**

Run (with `SQLFLOW_IMAGE=sqlflow:dev` in `.env`):

```bash
make down && make demo
make test 2>&1 | tee tests/last-run.log | tail -40
```

Expected: 11 passed in roughly 15 to 20 minutes (each failure scenario waits a settle window plus a handoff). On `v2026.09.30.1` the suite fails at `beforeAll` or `exact`, which is the pin note's point: the README says so.

A scenario that fails with "exact, then not" is the defect the stack exists to prevent (a partial republish landing after the right answer) and is reported upstream to sql-flow with the seed, not worked around in the test.

- [ ] **Step 5: Add `tests/last-run.log` to `.gitignore`, and commit**

```bash
echo 'tests/last-run.log' >> .gitignore
git add tests/ .gitignore
git commit -m "tests: the page's guarantees as an end-to-end suite

Eleven scenarios against the running demo stack, each the exact answer
for a fixed workload: a burst, retries, one partition per customer, every
reader agreeing with rollup verify, a stopped worker, a killed worker, a
lost state volume, a killed ingest, a late event, the lag reading, and
the archive holding each (id, meter) once. Each waits for Postgres to be
exact and then to stay exact, so a partial republish after the right
answer fails the test."
git push origin reference-stack
```

---

### Task 9: README: the five steps, the status table, and the limits

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: every command and name above. The turbolytics.io use-case page links here, and its pilot→shipped flips cite this README's status table.

- [ ] **Step 1: Fill the five steps**

Under each numbered step in `README.md`, add its commands and what to expect. Step 1 was written in Task 7; the rest:

```markdown
### 2. Every raw event is archived

`archive` reads the topic on its own consumer group and writes each batch as
one zstd Parquet object under `s3://usage/raw/day=YYYY-MM-DD/hour=HH/`.
Nothing is sampled or summarized. A day can be recomputed from it and
matched against the rollups, from any tool that reads Parquet.

    duckdb -c "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='localhost:9000'; SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';
      SELECT meter, sum(quantity) FROM read_parquet('s3://usage/raw/*/*/*.parquet', hive_partitioning=true)
      WHERE day='2026-09-24' AND hour='12' GROUP BY 1"

### 3. SQLFlow counts it

`count` windows the topic into one-minute buckets per customer and meter,
sums each meter over distinct event ids, and upserts the closed minute into
Postgres. The pipeline commits each partition's position before the oldest
row a retained minute holds, so a restart, a crash, or a lost state volume
recounts the open minutes from Kafka rather than losing them. Run as many
copies as the topic has partitions: `partition_owned` keeps each row to one
writer, and a worker that loses a partition drops its partial count before
the new owner recounts it.

    docker compose exec postgres psql -U metering -c \
      "SELECT minute, meter, sum(quantity) FROM usage_per_minute WHERE customer='user_00042' GROUP BY 1,2 ORDER BY 1,2"

A retry is counted once as long as it arrives within 185 seconds of the
original (window 60s + grace 5s + lateness 120s). The SDK's retry budget is
60 seconds. A retry later than that counts twice.

### 4. The totals roll up

`rollups` keeps `usage_by_customer_1h` and `usage_by_customer_1d` current
from the minute table with Postgres triggers, and checks the newest buckets
against their source every minute. `rollup verify` recomputes a table from
its source and reports any row that differs.

    docker compose run --rm --no-deps rollups rollup verify -c /config/rollups.yml

### 5. Every reader gets the same number

`serve` is the read path: four datasets over Postgres, read-only, with each
request's parameters bound into a fixed statement.

    curl "localhost:8082/v1/datasets/invoice?client_id=demo-console&customer=user_00042"
    curl "localhost:8082/v1/datasets/quota?client_id=demo-console&customer=user_00042"
    curl "localhost:8082/v1/datasets/usage?client_id=demo-console&customer=user_00042"

Invoice reads the hourly rollup, quota and usage the daily one, and all
three come from the one minute table `count` wrote, so they agree.
```

- [ ] **Step 2: The status table**

Replace the empty "Shipped and pilot" section:

```markdown
## Shipped and pilot

Each row is a guarantee on the use-case page and the scenario in `make test`
that proves it here.

| Guarantee | Status | Scenario |
|---|---|---|
| A 200 from ingest means the event is in Kafka | shipped | `ingest crash` |
| Exact per-minute totals, over distinct event ids | shipped | `exact` |
| A retry within 185s is counted once | shipped | `retries dedupe`, `late events` |
| A crashed, stopped or disk-less worker loses nothing and double-counts nothing | shipped | `stop`, `crash`, `lost disk` |
| Several count workers, one writer per row | shipped | `one partition per customer` |
| Hour and day rollups match their source | shipped | `every reader agrees` |
| Every reader serves the same number | shipped | `every reader agrees` |
| Every raw event archived once | shipped | `archive` |
| A reassigned partition whose open minutes are more than 120s behind the group's watermark | limit | see below |
| In-request quota check, period close and freeze | not built | |

"Shipped" means the scenario passes in this repo against the pinned image.
```

- [ ] **Step 3: The limits**

Replace the empty "Limits" section:

```markdown
## Limits

- **One Kafka broker, replication 1.** The stack shows SQLFlow's behavior,
  not Kafka's durability. In production Kafka has `min.insync.replicas` of
  2 or more; nothing in the configs changes.
- **Lag reads as the retained span.** The count group's committed offsets
  sit before the oldest retained minute on purpose, so consumer-group lag
  is nonzero for about 185 seconds after the last event. It is not falling
  behind. Alert on `window_close_lag_seconds` (how far closes trail the data) instead.
- **A partition reassigned after a long outage.** The watermark is per
  worker. A partition whose open minutes are more than 120 seconds behind
  the receiving worker's watermark has those minutes refused as late
  (sql-flow #417). The 45-second session timeout is well inside that; a
  worker paused for minutes is not. Raise `allowed_lateness_seconds` if your
  rebalances take longer.
- **The dedupe horizon is 185 seconds.** A retry later than that counts
  twice. Keep the client's retry budget inside it, as the SDK does.
- **The demo's sent ledger is in memory.** It is demo scaffolding; a restart
  clears it.
- **The Docker socket** is mounted only by the demo console, in
  `docker-compose.demo.yml`, for the failure buttons. It is root-equivalent
  on the host; the production shape in `docker-compose.yml` mounts nothing.
```

Also add, under "Run it":

```markdown
Prerequisites: Docker with compose v2, Node 22, and `duckdb` on the host
for `make test` (`brew install duckdb`). The first `make test` takes about
twenty minutes: each failure scenario waits for the minutes it broke to
settle.
```

- [ ] **Step 4: Check every command in the README runs**

Run each code block's command against `make demo` and compare with its text. Expected: each prints what its paragraph says; the `rollup verify` run exits 0.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "README: the five steps, the status table, and the limits

Each step has its command and what to expect; each guarantee on the
use-case page has its status and the make test scenario that proves it;
the limits say what the stack does not show (one broker), what reads
oddly on purpose (lag), and where the watermark is per worker
(sql-flow #417)."
git push origin reference-stack
```

---

## Self-review

**Spec coverage.** Reference-stack spec: ingest with `ack: after_flush` and `key: customer` (Task 2); count with `partition_owned`, lateness 120, two workers (Task 3); archive to Parquet (Task 4); rollups with `verify` and serve with the three readers (Task 5); the SDK with `onAck` (Task 6); sample-app with the control API and sent ledger (Task 7); the guarantees as tests and the shipped-vs-pilot table (Tasks 8, 9); the production/demo compose split (Tasks 1, 3). Demo spec's stack changes: `minute_totals` (Task 5), `onAck` (Task 6), `/load`, `/load/stop`, `/status`, `/sent` (Task 7). The console itself is the second plan, `2026-10-02-demo-console.md`, which consumes the interfaces this plan produces: `sample-app:8003`'s routes, `serve`'s four datasets, the compose service names `count`/`count-2`/`ingest`/`archive`/`rollups`/`serve`, the volume names `count-state`/`count-2-state`, and the `raw/day=/hour=/` layout.

**Placeholders.** None found: every config, test and command is written out. `rollup verify` is judged by exit code (it exits `system.rollup.drift` on a differing bucket), not by matching its output.

**Type consistency.** `Event` (SDK) is `{id, customer, ts, quantities}`; `Ledger.record(events: Event[])` consumes it; `Ev` in tests is the per-meter row and `postEvents` folds it back into bodies. `serveRows` returns `rows`; the SDK's `usage()` reads `rows[].meter/quantity`, matching serve's response shape. The compose volume `usage-metering_count-2-state` assumes the project name `usage-metering` (the directory name); `loseDisk` says so.

**Review Focus.** Items 1 and 2 are pinned by `fixtures/ingest.jsonl` in Task 2 Step 3; item 3 by `retries dedupe` and `ingest crash` in Task 8; item 4 by `one partition per customer`; item 5 by Task 5 Step 5's clean `make up`.

**Known gap, stated.** On `v2026.09.30.1` nothing past Task 1 verifies. The plan is written to be executed against `sqlflow:dev` from a sql-flow main checkout, and the pin bumped to the release that carries #414 as soon as it exists.
