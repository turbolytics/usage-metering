# The stack: `make up` is the production shape, `make demo` adds the second
# count worker and (in the console plan) the console.
COMPOSE      := docker compose
COMPOSE_DEMO := docker compose -f docker-compose.yml -f docker-compose.demo.yml

.PHONY: env up demo down logs validate sqlflow-image test load

env:
	@test -f .env || cp .env.example .env

# --wait exits 1 when a one-shot container (the bucket and rollup installers)
# has exited, even with 0, unless the services to wait for are named. So
# the long-running services are named, and the one-shots run as their
# dependencies. One-shots end in -init or -install.
LONG_RUNNING      = $$($(COMPOSE) config --services | grep -vE -- '-(init|install)$$')
LONG_RUNNING_DEMO = $$($(COMPOSE_DEMO) config --services | grep -vE -- '-(init|install)$$')

up: env
	$(COMPOSE) up -d --wait $(LONG_RUNNING)

demo: env
	$(COMPOSE_DEMO) up -d --wait $(LONG_RUNNING_DEMO)

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
