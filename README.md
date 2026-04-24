# Financial Data Platform

> **The question**: did this Merkl incentive campaign actually change LP retention behaviour — or did it just attract mercenary liquidity that left when rewards ended?

**Stack**: Python · dbt · PostgreSQL · Dash · Docker · Apache Airflow (optional)

**Origin**: [`uniswap-lp-analysis`](https://github.com/hamzaelmanar/uniswap-lp-analysis) — a working LP retention analysis (Kaplan-Meier survival models, HyperSync + Merkl API). This project refactors that notebook-style script into a tested, orchestrated pipeline with a clean data model.

---

## Quick Start

**Prerequisites**: Python 3.12+, Docker Desktop (for Postgres), a [HyperSync bearer token](https://docs.envio.dev/docs/HyperSync/overview)

```bash
git clone https://github.com/hamzaelmanar/financial-data-platform
cd financial-data-platform

# 1. Create venv, install deps, run dbt deps
make setup

# 2. Fill in credentials (Postgres + HyperSync token)
cp .env.example .env && nano .env   # or edit in your IDE

# 3. Start Postgres
make up

# 4. Ingest data (Celo WETH/USDT pool)
make ingest \
  CHAIN=celo \
  ADDRESS=0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c \
  URL=https://app.merkl.xyz/opportunities/celo/CLAMM/0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c

# 5. Run dbt models
make run

# 6. Open the dashboard (http://localhost:8050)
make dashboard
```

**Windows**: dot-source `dev.ps1` first to activate the venv and load `.env` into your shell:
```powershell
. .\dev.ps1
```
Then use the same `make` commands above.

---

## What This Demonstrates

The full analytics engineering lifecycle, end to end:

- **Data engineer**: Python ingestion from two live APIs (HyperSync on-chain events + Merkl REST API), PostgreSQL warehouse setup, Airflow DAGs as a forward-looking extension
- **Analytics engineer**: dbt staging + mart models, tests, documentation, lineage graph — the 80-line `verify_lp_exit()` pandas function becomes a tested SQL model
- **Data analyst**: a clear output — *did the campaign change retention, and by how much?* — backed by a logrank test and segmented KM curves

Bridge pitch: *"The question 'did this intervention change user retention?' is the same question lesfurets asks about comparison funnel campaigns. I used DeFi data because it's public and the incentive conditions are explicit. The method — segment by treatment, measure retention, test for significance — is domain-agnostic."*

---

## Stack

| Layer | Tool | Notes |
|-------|------|-------|
| Ingestion | Python | HyperSync (on-chain events) + Merkl REST API |
| Warehouse | PostgreSQL | Docker-managed; schemas: `raw`, `staging`, `marts` |
| Transformation | dbt core (OSS) | Staging → mart models, full lineage |
| Data quality | dbt tests | `not_null`, `unique`, `accepted_values` on every model |
| Analysis | Python (`lifelines`) | KM estimator + logrank test, reads from mart |
| Dashboard | Dash + Plotly | 3-tab interactive app served on `localhost:8050` |
| Orchestration | Apache Airflow | Optional — DAGs in `dags/` wrap existing ingestion functions |

> **Running the pipeline does not require Airflow.** `make ingest && make run && make dashboard` is the full pipeline. Airflow DAGs are included as a demonstrated capability for multi-pool daily scheduling.

---

## Architecture

```
financial-data-platform/
│
├── dags/                             ← Airflow DAGs (optional scheduled ingestion)
│   ├── ingest_lp_events.py           ← DAG: HyperSync events + ABI decode
│   └── ingest_merkl_campaigns.py    ← DAG: Merkl campaign metadata
│
├── dashboard/
│   └── app.py                        ← Dash app: KM curves, cohort comparison, exit histogram
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                  ← PostgreSQL connection (env-var driven)
│   ├── macros/
│   │   └── generate_schema_name.sql  ← prevents target_schema prefix concatenation
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_models.yml
│       │   ├── stg_hypersync__lp_events.sql
│       │   ├── stg_hypersync__swap_events.sql
│       │   └── stg_merkl__campaigns.sql
│       └── marts/
│           └── retention/
│               ├── fct_models.yml
│               ├── fct_lp_positions.sql       ← one row per LP: entry, exit, duration, cohort
│               └── fct_campaign_retention.sql ← campaign-level KPIs per cohort
│
├── ingestion/
│   ├── sources/
│   │   ├── hypersync_events.py       ← incremental HyperSync event fetch (watermark-based)
│   │   └── merkl_campaigns.py       ← Merkl REST API campaign metadata
│   └── utils/
│       ├── db_loader.py              ← PostgreSQL writes via psycopg2 execute_values
│       └── decode_events.py          ← ABI decode raw hex logs → typed event rows
│
├── analysis/
│   └── lp_survival.py               ← KM estimator + logrank test (reads from mart)
│
├── Makefile                          ← setup, ingest, run, test, dashboard targets
├── dev.ps1                           ← Windows: activate venv + load .env into shell
├── docker-compose.yml                ← Postgres service; Airflow stack (commented)
├── Dockerfile.dashboard              ← Docker image for the Dash app
├── .env.example                      ← credentials template (never commit .env)
└── requirements.txt
```

---

## Data Model

### Bronze — Raw ingestion
- Exact API response stored as-is, never modified after write
- Tables:
  - `raw_hypersync_lp_events` — Mint, Burn, Swap, Collect events; `event_type`, `pool_address`, `owner`, `tick_lower`, `tick_upper`, `amount`, `sqrt_price_x96`, `block_number`, `transaction_index`, `log_index`, `block_timestamp`
  - `raw_merkl_campaigns` — campaign metadata; `campaign_id`, `pool_address`, `start_timestamp`, `end_timestamp`, `symbol_reward_token`, `daily_rewards_value`

### Silver — Staging (dbt `stg_` models)
- One staging model per raw source table; rename, type, deduplicate — no business logic
- `stg_hypersync__lp_events`: `event_type` (accepted_values: Mint/Burn/Swap/Collect), `position_key` (owner + tick_lower + tick_upper), `liquidity_delta` (signed: positive for Mint, negative for Burn), `block_timestamp`
- `stg_merkl__campaigns`: `campaign_id`, `pool_address`, `start_timestamp`, `end_timestamp`, `daily_rewards_value`

> Note: `sqrt_price_x96 → price_usd` conversion is valid only for stablecoin-paired pools (e.g. USDT/WETH). It is not applied at the staging layer — USD valuation is an analysis-time concern, not a pipeline concern.

### Gold — Marts (dbt `fct_` models)
- Domain: `retention/`
- Materialized as `table` in PostgreSQL
- All business logic lives here; minimum tests: `not_null` + `unique` on primary key

| Model | Description | Key columns |
|---|---|---|
| `fct_lp_positions` | One row per LP: port of `verify_lp_exit()` + `entered_during_campaign()` in SQL | `position_key`, `owner`, `first_mint_timestamp`, `exit_timestamp`, `duration_days`, `is_exited`, `entered_during_campaign` |
| `fct_campaign_retention` | Campaign-level aggregates; computed directly from `fct_lp_positions` | `campaign_id`, `n_lps_entered`, `n_still_active_7d`, `n_still_active_30d`, `retention_rate_7d`, `retention_rate_30d`, `median_hold_days` |

**What stays in Python** (`analysis/lp_survival.py`): the Kaplan-Meier estimator and logrank test. These are statistical models from `lifelines` — they read from `fct_lp_positions` and produce the segmented KM curves and p-value. SQL prepares the data; Python does the statistics.

---

## Conventions

### dbt
- Staging: `stg_<source>__<entity>.sql` (double underscore separates source from entity)
- Mart: `fct_<topic>.sql` or `dim_<topic>.sql`
- Sources declared in `staging/sources.yml` — no raw table names in mart models
- Every mart model column documented in a `.yml` file alongside the `.sql`

### Airflow (optional)
Airflow is not required to run the pipeline. It is included because the natural evolution of this project — tracking ten pools on a daily schedule, alerting on stale data — is exactly what Airflow solves. The DAG files demonstrate that the ingestion logic is already structured to support it: DAG = wiring only, logic lives in `ingestion/sources/`.

- DAG IDs: `ingest_<source>_<entity>`
- Default args: `retries=2`, `retry_delay=timedelta(minutes=5)`, `email_on_failure=False`
- Connections stored in Airflow Connections UI or `.env`, never hardcoded
