"""
DAG: ingest_merkl_campaigns
Schedule: daily at 02:00 UTC  (after ingest_lp_events)
Purpose: Fetch incentive campaign metadata from the Merkl REST API and land
         raw campaign rows in PostgreSQL.

Required env vars:
    MERKL_URL         — e.g. "https://app.merkl.xyz/opportunities/celo/CLAMM/0x..."
    POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB / POSTGRES_HOST / POSTGRES_PORT
"""

import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
}

with DAG(
    dag_id="ingest_merkl_campaigns",
    description="Fetch Merkl incentive campaign metadata",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "merkl"],
) as dag:

    def _ingest_campaigns() -> None:
        from ingestion.sources.merkl_campaigns import ingest

        merkl_url = os.environ["MERKL_URL"]
        ingest(merkl_url=merkl_url)

    PythonOperator(
        task_id="fetch_campaigns",
        python_callable=_ingest_campaigns,
    )
