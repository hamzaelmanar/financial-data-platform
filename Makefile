.PHONY: up down ingest run analysis all

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up -d postgres

down:
	docker compose down

# ── Pipeline ──────────────────────────────────────────────────────────────────

# Usage: make ingest CHAIN=celo ADDRESS=0x... URL=https://app.merkl.xyz/...
ingest:
	python -m ingestion.sources.hypersync_events --chain $(CHAIN) --address $(ADDRESS)
	python -m ingestion.utils.decode_events --chain $(CHAIN) --pool-address $(ADDRESS)
	python -m ingestion.sources.merkl_campaigns --url $(URL)

run:
	cd dbt && dbt run

test:
	cd dbt && dbt test

docs:
	cd dbt && dbt docs generate && dbt docs serve

analysis:
	python analysis/lp_survival.py

# ── Full pipeline (no Airflow required) ──────────────────────────────────────

all: ingest run analysis
