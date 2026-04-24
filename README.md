# Financial Data Platform

> **The question**: did this Merkl incentive campaign actually change LP retention behaviour — or did it just attract mercenary liquidity that left when rewards ended?

**Stack**: Python · dbt · DuckDB · Docker · Apache Airflow (optional — see below)

**Origin**: [`uniswap-lp-analysis`](https://github.com/hamzaelmanar/uniswap-lp-analysis) — a working LP retention analysis (Kaplan-Meier survival models, HyperSync + Merkl API). This project refactors that notebook-style script into a tested, orchestrated pipeline with a clean data model.

---

## What This Demonstrates

The full analytics engineering lifecycle, end to end:

- **Data engineer**: Python ingestion from two live APIs (HyperSync on-chain events + Merkl REST API), DuckDB warehouse setup, Airflow DAGs as a forward-looking extension
- **Analytics engineer**: dbt staging + mart models, tests, documentation, lineage graph — the 80-line `verify_lp_exit()` pandas function becomes a tested SQL model
- **Data analyst**: a clear output — *did the campaign change retention, and by how much?* — backed by a logrank test and segmented KM curves

Bridge pitch: *"The question 'did this intervention change user retention?' is the same question lesfurets asks about comparison funnel campaigns. I used DeFi data because it's public and the incentive conditions are explicit. The method — segment by treatment, measure retention, test for significance — is domain-agnostic."*

---

## Stack

| Layer | Tool | Notes |
|-------|------|-------|
| Ingestion | Python | HyperSync (on-chain events) + Merkl REST API |
| Warehouse | DuckDB | Persistent mode; handles large pools without OOM |
| Transformation | dbt core (OSS) | Staging → mart models, full lineage |
| Data quality | dbt tests | `not_null`, `unique`, `accepted_values` on every model |
| Analysis | Python (`lifelines`) | KM estimator + logrank test, reads from mart |
| Orchestration | Apache Airflow | Optional — see rationale below |
| Agent layer | OpenAI Agents SDK | dbt test failure summarizer (deferred) |

> **Running the pipeline does not require Airflow.** `make ingest && dbt run && python analysis/lp_survival.py` is the full pipeline. Airflow DAGs are included as a demonstrated capability and natural extension path for multi-pool daily scheduling — not as required infrastructure for a single-pool, on-demand analysis.

> BigQuery + Terraform are documented in `terraform/` as a cloud migration path but are not required locally.

---

## Architecture

```
financial-data-platform/
│
├── context/                          ← context nuggets (one concept per file)
│   ├── README.md                     ← index + fetch guide
│   ├── concepts/
│   │   ├── data-model.md             ← bronze/silver/gold layer definitions
│   │   └── pipeline-overview.md     ← end-to-end flow
│   ├── guidelines/
│   │   ├── dbt-conventions.md        ← naming, testing, staging/mart split
│   │   └── airflow-conventions.md   ← DAG structure, retries, SLAs
│   └── playbooks/
│       ├── add-new-source.md
│       └── add-new-dbt-model.md
│
├── dags/                             ← optional: Airflow DAGs for scheduled multi-pool ingestion
│   ├── ingest_lp_events.py           ← DAG: Mint/Burn/Swap/Collect events via HyperSync
│   └── ingest_merkl_campaigns.py    ← DAG: incentive campaign metadata via Merkl API
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                  ← DuckDB (local) + BigQuery (prod) targets
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml           ← every raw table declared as a dbt source
│   │   │   ├── stg_hypersync__lp_events.sql
│   │   │   └── stg_merkl__campaigns.sql
│   │   └── marts/
│   │       └── retention/
│   │           ├── fct_lp_positions.sql       ← one row per LP: entry, exit, duration, campaign_entry_flag
│   │           └── fct_campaign_retention.sql ← campaign-level KPIs: n_lps, retention_rate_7d/30d, median_hold_days
│   └── tests/
│       └── generic/
│
├── ingestion/
│   ├── sources/
│   │   ├── hypersync_events.py       ← fetch Mint/Burn/Swap/Collect from HyperSync (port of src/extract.py)
│   │   └── merkl_campaigns.py       ← fetch campaign metadata from Merkl API (port of src/merkl_campaigns.py)
│   └── utils/
│       └── bq_loader.py             ← write raw JSON → DuckDB (local) or BigQuery raw dataset
│
├── analysis/
│   └── lp_survival.py               ← KM estimator + logrank test reading from fct_lp_positions mart (port of src/survival_analysis.py)
│
├── terraform/
│   ├── main.tf                       ← BigQuery datasets: raw, staging, marts
│   ├── variables.tf
│   └── outputs.tf
│
├── agents/
│   └── dbt_monitor_agent.py         ← reads dbt run_results.json, outputs plain-English failure summary
│
├── Makefile                          ← `make ingest`, `make run`, `make analysis` — no Airflow required
├── docker-compose.yml                ← optional: Airflow local stack (Postgres + Scheduler + Webserver)
├── .env.example                      ← env vars template (never commit .env)
├── requirements.txt
├── CLAUDE.md                         ← glossary + links to context/ nuggets only
└── AGENTS.md
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
- Materialized as `table` in DuckDB (local) or BigQuery (prod)
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
- Partition `fct_lp_positions` by `first_mint_timestamp`; cluster on `owner`
- When migrating to BigQuery: same partition/cluster keys apply

### Airflow (optional)
Airflow is not required to run the pipeline. It is included because the natural evolution of this project — tracking ten pools on a daily schedule, alerting on stale data — is exactly what Airflow solves. The DAG files demonstrate that the ingestion logic is already structured to support it: DAG = wiring only, logic lives in `ingestion/sources/`.

- DAG IDs: `ingest_<source>_<entity>`
- Default args: `retries=2`, `retry_delay=timedelta(minutes=5)`, `email_on_failure=False`
- Connections stored in Airflow Connections UI or `.env`, never hardcoded

### Terraform
- One `google_bigquery_dataset` per layer (raw, staging, marts)
- Service account per environment; state stored in GCS, not committed
- `variables.tf` for project ID, region, environment

---

## Build Sequence

### Phase A — Infrastructure
1. `Makefile` with `ingest`, `run`, `analysis` targets
2. DuckDB + `profiles.yml` tested locally (`dbt debug --target local`)
3. One empty staging model runs clean (`dbt run`)

### Phase B — First Pipeline
5. `ingestion/sources/hypersync_events.py` — port `src/extract.py`; fetches Mint/Burn/Swap/Collect events via HyperSync for a given pool address
6. Raw events land in DuckDB bronze layer (`raw_hypersync_lp_events`)
8. `stg_hypersync__lp_events.sql` — staging model with `position_key` construction and signed `liquidity_delta`; `accepted_values` test on `event_type`
9. `fct_lp_positions.sql` — port `verify_lp_exit()` into SQL window functions: cumulative liquidity per position, exit detection, `duration_days`

### Phase C — Second Source + Analytical Output
10. `ingestion/sources/merkl_campaigns.py` — port `src/merkl_campaigns.py`
11. `stg_merkl__campaigns.sql` — typed campaign windows
13. `fct_lp_positions.sql` updated — add `entered_during_campaign` flag (join on `first_mint_timestamp BETWEEN campaign.start AND campaign.end`)
14. `fct_campaign_retention.sql` — campaign-level retention rates computed from `fct_lp_positions`
15. `analysis/lp_survival.py` — reads `fct_lp_positions`, runs KM estimator + logrank test; segmented curves match origin repo PNG
16. dbt docs generated (`dbt docs serve`) — lineage: `raw_hypersync_lp_events` → `stg_` → `fct_lp_positions` → `fct_campaign_retention`

### Phase D — Airflow (optional extension)
17. `docker-compose.yml` — local Airflow stack (Postgres + Scheduler + Webserver)
18. `dags/ingest_lp_events.py` + `dags/ingest_merkl_campaigns.py` — wrap ingestion scripts; verify DAGs parse and run clean in the local stack

### Phase E — Packaging
19. README final pass
20. dbt lineage graph screenshot
21. Loom walkthrough (5 min): `make ingest` → `dbt run` → `python analysis/lp_survival.py` → KM curve

---

## Files to Create

### Core pipeline (required for demo)
- [ ] `Makefile`
- [ ] `dbt/dbt_project.yml`
- [ ] `dbt/profiles.yml` — DuckDB local target
- [ ] `dbt/models/staging/sources.yml`
- [ ] `dbt/models/staging/stg_hypersync__lp_events.sql`
- [ ] `dbt/models/staging/stg_merkl__campaigns.sql`
- [ ] `dbt/models/marts/retention/fct_lp_positions.sql`
- [ ] `dbt/models/marts/retention/fct_campaign_retention.sql`
- [ ] `ingestion/sources/hypersync_events.py`
- [ ] `ingestion/sources/merkl_campaigns.py`
- [ ] `ingestion/utils/duckdb_loader.py`
- [ ] `analysis/lp_survival.py`

### Airflow extension (optional)
- [ ] `docker-compose.yml`
- [ ] `dags/ingest_lp_events.py`
- [ ] `dags/ingest_merkl_campaigns.py`

### Context and documentation
- [ ] `CLAUDE.md`
- [ ] `context/README.md`
- [ ] `context/concepts/data-model.md`
- [ ] `context/concepts/pipeline-overview.md`
- [ ] `context/guidelines/dbt-conventions.md`
- [ ] `context/guidelines/airflow-conventions.md`
- [ ] `context/playbooks/add-new-source.md`
- [ ] `context/playbooks/add-new-dbt-model.md`

### Cloud migration path (deferred)
- [ ] `dbt/profiles.yml` updated — add BigQuery prod target
- [ ] `ingestion/utils/bq_loader.py` — BigQuery variant of the loader
- [ ] `terraform/main.tf`
- [ ] `terraform/variables.tf`

### Agent layer (deferred)
- [ ] `agents/dbt_monitor_agent.py`
