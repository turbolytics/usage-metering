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
machine already listens on 5432, 29092, 9000 or 9001, change the `*_HOST_PORT`
values there; nothing inside the stack uses them.

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
