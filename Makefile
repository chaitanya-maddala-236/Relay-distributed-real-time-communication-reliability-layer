.PHONY: help install dev-services db up down test test-unit test-integration test-distributed test-chaos lint seed

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-22s %s\n", $$1, $$2}'

install:  ## Create venv and install dependencies
	python3 -m venv .venv && ./.venv/bin/pip install -r requirements-dev.txt

db:  ## Create database schema
	PYTHONPATH=. ./.venv/bin/python scripts/init_db.py

up:  ## Start the full stack in Docker (2 nodes, nginx, redis, postgres, prometheus, grafana)
	docker compose up --build

down:  ## Stop the stack
	docker compose down -v

seed:  ## Create a demo tenant + API key against a running control plane
	PYTHONPATH=. ./.venv/bin/python scripts/seed.py

test-unit:  ## Unit tests (no services required)
	./.venv/bin/python -m pytest tests/unit -q

test-reliability:  ## Resume/recovery tests (requires Redis)
	./.venv/bin/python -m pytest tests/reliability -q

test-integration:  ## End-to-end protocol tests (requires running gateway + control plane)
	PYTHONPATH=. ./.venv/bin/python tests/integration/e2e_verify.py

test-distributed:  ## Cross-node fanout tests (requires two gateways on :8000 and :8010)
	PYTHONPATH=. ./.venv/bin/python tests/distributed/multinode_verify.py

test-slow-consumer:  ## Backpressure test (run gateway with small bounds, see file header)
	PYTHONPATH=. ./.venv/bin/python tests/reliability/slow_consumer_verify.py

test-presence:  ## Presence tests (requires running gateway + control plane)
	PYTHONPATH=. ./.venv/bin/python tests/integration/presence_verify.py

test-chaos:  ## Redis outage observation (stops/starts local Redis)
	PYTHONPATH=. ./.venv/bin/python tests/chaos/redis_outage_observe.py

test: test-unit test-reliability  ## Tests that need no running gateway

lint:  ## Ruff lint + format check
	./.venv/bin/ruff check apps packages tests
