# Build Plan — financial-data-platform

## Phases

### 1. Project Scaffold
- Folder structure: `ingestion/`, `dbt/`, `analysis/`, `dags/`, `terraform/`, `agents/`
- `Makefile`, `docker-compose.yml`, `.env.example`, `requirements.txt`
- PostgreSQL via Docker (replaces DuckDB — see Design Decisions)

### 2. Ingestion (Bronze)
Port from `uniswap-lp-analysis/src/`:

| Script | Target | Notes |
|---|---|---|
| `extract.py` | `ingestion/sources/hypersync_events.py` | write raw logs + blocks → Postgres `raw_hypersync_lp_events` |
| `merkl_campaigns.py` | `ingestion/sources/merkl_campaigns.py` | write API response → Postgres `raw_merkl_campaigns` |
| — | `ingestion/utils/db_loader.py` | shared Postgres write util (psycopg2 + SQLAlchemy) |

Fix: replace positional `topic0` mapping with explicit hash→name dict.

### 3. dbt — Staging (Silver)
- `sources.yml` — declare both raw tables
- `stg_hypersync__lp_events.sql` — rename cols, cast types, add `position_key`, signed `liquidity_delta`, explicit topic0 decode
- `stg_merkl__campaigns.sql` — rename, cast, add `campaign_duration_days`
- Tests: `not_null`, `unique`, `accepted_values` on every model

### 4. dbt — Marts (Gold)
- `fct_lp_positions.sql` — port `verify_lp_exit` + `entered_during_campaign` to SQL (one row per LP: entry, exit, duration, status, campaign flag)
- `fct_campaign_retention.sql` — aggregate KPIs: n_lps, retention_7d/30d, median_hold_days
- `fct_pool_tvl.sql` — port `tvl()` to SQL (cumulative token amounts + price from sqrtPriceX96)

### 5. Analysis
- `analysis/lp_survival.py` — KM estimator + logrank test reading from `fct_lp_positions` via SQLAlchemy/psycopg2
- Replaces notebook; single `python analysis/lp_survival.py` entry point

### 6. Orchestration (Optional)
- `dags/ingest_lp_events.py` — daily HyperSync DAG
- `dags/ingest_merkl_campaigns.py` — daily Merkl DAG
- Airflow local stack via `docker-compose.yml`

### 7. Agent Layer (Deferred)
- `agents/dbt_monitor_agent.py` — reads `dbt run_results.json`, outputs plain-English failure summary via OpenAI Agents SDK

### 8. Cloud Path (Terraform, Deferred)
- `terraform/` — BigQuery datasets: raw / staging / marts
- `dbt/profiles.yml` BigQuery target

---

## Key Design Decisions
- **No Airflow required** to run: `make ingest && dbt run && python analysis/lp_survival.py`
- **PostgreSQL** (Docker) as warehouse — enterprise-grade, handles uint256 via `NUMERIC(78)`, avoids DuckDB HUGEINT overflow
- **Type strategy**: bronze stores all hex columns as `TEXT` (exact preservation); staging casts to `NUMERIC(78)` / `BIGINT` / `TEXT` as appropriate
- **topic0 fix**: map by explicit keccak hash, not positional order
- **USD valuation** stays out of staging — analysis-time concern only
- **Cox model** stays commented out; KM + logrank is the deliverable
