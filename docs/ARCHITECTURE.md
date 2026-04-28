# Architecture & Setup

Full reference for the `financial-data-platform` pipeline.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Python 3.12+ | `python --version` to verify |
| Docker Desktop | For Airflow and dashboard containers |
| PostgreSQL (local install) | **Not Docker** — installed as a Windows service |
| HyperSync bearer token | Free at [Envio](https://docs.envio.dev/docs/HyperSync/overview) |

### PostgreSQL

The warehouse runs as a **local Windows service** (`postgresql-x64-*`), not a container.  
Containers reach it via `host.docker.internal` — Docker's built-in DNS for the host machine.  
This mirrors how production containers connect to a managed cloud DWH (Snowflake, BigQuery, Redshift).

Default credentials assumed by `.env.example`:
```
host:     localhost      (host-side) / host.docker.internal (container-side, handled in docker-compose.yml)
port:     5432
user:     postgres
password: postgres
database: postgres
```

If your install uses different credentials, update `.env` accordingly.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in each value:

| Variable | Description |
|----------|-------------|
| `POSTGRES_USER` | PostgreSQL user (e.g. `postgres`) |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | Target database name |
| `POSTGRES_HOST` | `localhost` for host-side scripts; containers use `host.docker.internal` automatically |
| `POSTGRES_PORT` | Default `5432` |
| `HYPERSYNC_BEARER_TOKEN` | Bearer token from Envio HyperSync — required for on-chain event ingestion |
| `MERKL_URL` | Full Merkl opportunity URL for your pool (no auth required) |
| `POOL_CHAIN` | Chain identifier, e.g. `celo` |
| `POOL_ADDRESS` | Pool contract address (checksum format) |
| `DBT_PROFILES_DIR` | Leave as `./dbt` — points dbt at the bundled `profiles.yml` |

---

## Data Flow

```
HyperSync API          Merkl REST API
     |                       |
     v                       v
ingestion/sources/     ingestion/sources/
  hypersync.py           merkl.py
     |                       |
     v                       v
raw.raw_hypersync_lp_events   raw.raw_merkl_campaigns
     |                       |
     +----------+------------+
                |
           dbt core
                |
     +----------+------------+
     |                       |
stg_hypersync__lp_events   stg_merkl__campaigns
     |                       |
     +----------+------------+
                |
         fct_lp_positions           <- one row per LP position
                |
      fct_campaign_retention        <- retention KPIs per cohort
                |
          analysis/survival.py      <- Kaplan-Meier, logrank
                |
         dashboard/app.py           <- Dash, localhost:8050
```

See `docs/lineage.png` for the dbt-generated lineage graph.

---

## Layer Details

### Ingestion (`ingestion/`)

- `sources/hypersync.py` — fetches `Mint`/`Burn`/`Collect` events via HyperSync streaming. Incremental: watermarks the last processed block in a local JSON file and resumes from there.
- `sources/merkl.py` — polls the Merkl REST API for campaign windows on the configured pool address.
- Both scripts write to `raw.*` tables in PostgreSQL, preserving source data as-is.

Entry point:
```bash
make ingest CHAIN=celo ADDRESS=0xYourPool URL=https://app.merkl.xyz/...
```

### Transformation (`dbt/`)

Three schemas: `raw` → `staging` → `marts`

| Model | Type | Description |
|-------|------|-------------|
| `stg_hypersync__lp_events` | view | Typed, renamed columns; signed `liquidity_delta` |
| `stg_merkl__campaigns` | view | Campaign window timestamps |
| `fct_lp_positions` | table | One row per LP: entry, exit, duration, cohort flag |
| `fct_campaign_retention` | table | Aggregated retention KPIs per cohort |

Notable dbt patterns used:
- `generate_schema_name` macro — custom schema routing so models land in the right schema
- `dbt test` — not-null, unique, accepted-values on key columns
- `sources.yml` — raw tables declared as dbt sources for lineage tracking

```bash
make run          # dbt run
make test         # dbt test
make docs         # dbt docs generate + serve → localhost:8080
                  # If Airflow is running: dbt docs serve --profiles-dir . --port 8081
```

### Analysis (`analysis/`)

`survival.py` loads `fct_lp_positions`, fits a Kaplan-Meier estimator per cohort using `lifelines`, and runs a logrank test. The p-value (0.0186) shows a statistically significant difference in LP retention between campaign and non-campaign cohorts.

Runs automatically as part of the dashboard on first load.

### Dashboard (`dashboard/`)

Dash + Plotly, three tabs:
1. **Survival curves** — KM curves segmented by cohort
2. **Retention KPIs** — bar charts from `fct_campaign_retention`
3. **Raw positions** — filterable table of `fct_lp_positions`

Runs via `python dashboard/app.py` locally (no Docker needed). A `fdp_dashboard` Docker container is also provided for deployment or sharing without a local venv — both connect to the same Postgres via `host.docker.internal`.

```bash
# Local (venv active)
python dashboard/app.py           # → localhost:8050

# Or via Docker (no venv required)
docker compose up -d dashboard    # → localhost:8050
```

### Orchestration (`dags/`)

Two Airflow DAGs:

| DAG | Schedule | Task |
|-----|----------|------|
| `ingest_lp_events` | daily | Incremental HyperSync fetch → raw table |
| `ingest_merkl_campaigns` | daily | Merkl API fetch → raw table |

DAGs are wiring only: they call `ingestion/sources/*.py` directly, keeping business logic out of Airflow. This means the full pipeline runs identically with or without Airflow.

Airflow connects to local Postgres via `host.docker.internal` — same pattern as the dashboard container.

---

## Full Setup from Clone

```bash
# 1. Clone
git clone https://github.com/hamzaelmanar/financial-data-platform
cd financial-data-platform

# 2. Python environment + deps
make setup

# 3. Configure
cp .env.example .env
# Edit .env — fill POSTGRES_*, HYPERSYNC_BEARER_TOKEN, MERKL_URL, POOL_CHAIN, POOL_ADDRESS

# 4. Ingest
make ingest CHAIN=celo ADDRESS=0xYourPool URL=https://app.merkl.xyz/...

# 5. Transform
make run

# 6. Explore
make dashboard
# open http://localhost:8050
```

**Windows PowerShell** — activate the venv and load `.env` first:
```powershell
. .\dev.ps1
```

---

## Optional: Airflow Stack

```bash
docker compose --profile airflow up -d
```

- Airflow UI: `http://localhost:8080` (admin / admin)
- Dashboard: `http://localhost:8050`
- Containers: `fdp_airflow_webserver`, `fdp_airflow_scheduler`, `fdp_dashboard`

> **Port conflict**: `dbt docs serve` defaults to `localhost:8080`. With Airflow running, use `--port 8081`.

Airflow is additive — the `make ingest && make run && make dashboard` pipeline works identically without it.

---

## Design Decisions

**Why local Postgres instead of a Docker container for the warehouse?**  
A containerised Postgres would be wiped each `docker compose down`. Persisting data requires named volumes and lifecycle management. Using the local service keeps the warehouse stable across container restarts — the same relationship as a cloud DWH (BigQuery, Snowflake) and compute containers in production.

**Why `host.docker.internal`?**  
Standard Docker networking: containers can't reach `localhost` on the host directly. `host.docker.internal` resolves to the host machine's internal IP, which is how containers connect to the local Postgres service. This is set in `docker-compose.yml` and transparent to application code.

**Why watermark-based ingestion?**  
HyperSync streams millions of events. Fetching from block 0 on every run would be slow and wasteful. The watermark (last processed block, persisted to a local JSON file) makes each run incremental and idempotent.
