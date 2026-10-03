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

`make up` copies `.env.example` to `.env` on first run. If something on your
machine already listens on 5432, 29092, 9000, 9001 or 8001, change the
`*_HOST_PORT` values there; nothing inside the stack uses them.

## The five steps

1. The app sends an event.
2. Every raw event is archived.
3. SQLFlow counts it.
4. The totals roll up.
5. Every reader gets the same number.

### 1. The app sends an event

`ingest` is an HTTP endpoint that writes each event to Kafka, keyed by
customer, and answers only once it is there: `{"status":"flushed"}` with a
200 means the event is in the log. Anything else means it may not be: the
sender retries with the same event id, which the count deduplicates. While
Kafka is unreachable a request waits rather than answering, so give it a
timeout; an event whose request timed out can still land once Kafka is back,
and its retry is then counted once. An app that already produces to Kafka
can skip `ingest` and write the same records to `usage.events` itself.

    curl -s -X POST localhost:8001/events -H 'content-type: application/json' \
      -d '{"events":[{"id":"evt_1","customer":"user_1","ts":"2026-09-24T12:07:13Z","quantities":{"requests":1,"input_tokens":812}}]}'
    {"status":"flushed"}

One event becomes one Kafka record per meter in `quantities`, so a new meter
needs no config change. An event without `id` or `customer` is dropped, as is
a meter whose value is null. `ts` is RFC 3339; one without an offset reads as
UTC, and one more than five seconds ahead of the server's clock is clamped to
it, so a single bad clock cannot close every open minute.

## Shipped and pilot

## Limits

## When not to use this

Below a few events per customer per minute, a Postgres table keyed by event id
is simpler: the primary key dedupes, the commit is the "200 means recorded",
and there are no windows to run. Streaming pre-aggregation earns its place when
raw events outgrow the database you read from: at 50,000 events a second,
about nine million raw rows a minute become about ninety thousand minute rows.
