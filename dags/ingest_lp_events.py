"""
DAG: ingest_lp_events
Schedule: daily at 01:00 UTC
Purpose: Fetch Mint/Burn/Swap/Collect events from HyperSync for a configured
         Uniswap V3 pool, decode them, and land raw tables in PostgreSQL.

Required env vars (set in Airflow Connections or .env mounted into the container):
    POOL_CHAIN        — e.g. "celo"
    POOL_ADDRESS      — e.g. "0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c"
    HYPERSYNC_BEARER_TOKEN
    POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB / POSTGRES_HOST / POSTGRES_PORT
"""

import asyncio
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
}

with DAG(
    dag_id="ingest_lp_events",
    description="Fetch and decode Uniswap V3 LP events via HyperSync",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "hypersync"],
) as dag:

    def _ingest_raw_events() -> None:
        from ingestion.sources.hypersync_events import ingest

        chain = os.environ["POOL_CHAIN"]
        address = os.environ["POOL_ADDRESS"]
        asyncio.run(ingest(chain_name=chain, contract_address=address))

    def _decode_events() -> None:
        from ingestion.utils.decode_events import decode_and_write

        chain = os.environ["POOL_CHAIN"]
        address = os.environ["POOL_ADDRESS"]
        decode_and_write(chain_name=chain, pool_address=address)

    fetch_raw = PythonOperator(
        task_id="fetch_raw_events",
        python_callable=_ingest_raw_events,
    )

    decode = PythonOperator(
        task_id="decode_events",
        python_callable=_decode_events,
    )

    fetch_raw >> decode
