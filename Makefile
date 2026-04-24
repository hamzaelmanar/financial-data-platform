.PHONY: setup up down airflow ingest run test docs analysis dashboard all

# ── First-time setup ──────────────────────────────────────────────────────────

setup:
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example — fill in your credentials."; fi
	cd dbt && ../.venv/bin/dbt deps --profiles-dir .

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up -d postgres

down:
	docker compose down

airflow:
	docker compose --profile airflow up -d

# ── Pipeline ──────────────────────────────────────────────────────────────────

# Usage: make ingest CHAIN=celo ADDRESS=0x... URL=https://app.merkl.xyz/...
ingest:
	python -m ingestion.sources.hypersync_events --chain $(CHAIN) --address $(ADDRESS)
	python -m ingestion.utils.decode_events --chain $(CHAIN) --address $(ADDRESS)
	python -m ingestion.sources.merkl_campaigns --url $(URL)

run:
	cd dbt && dbt run --profiles-dir .

test:
	cd dbt && dbt test --profiles-dir .

docs:
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

analysis:
	python analysis/lp_survival.py

dashboard:
	python dashboard/app.py

# ── Full pipeline (no Airflow required) ──────────────────────────────────────

all: ingest run analysis
