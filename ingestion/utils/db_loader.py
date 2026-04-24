import os

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(url, future=True)


def ensure_schema(engine, schema: str = "raw") -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def write_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "raw",
    if_exists: str = "append",
    dtype: dict | None = None,
) -> None:
    """
    Write a pandas DataFrame to PostgreSQL.

    uint256 / int256 values must already be Python int (arbitrary precision).
    Pass them as dtype={'col': NUMERIC} via sqlalchemy types if needed — the
    default TEXT storage for bronze is sufficient; staging casts via dbt.

    Parameters
    ----------
    df         : DataFrame to write
    table_name : target table (without schema prefix)
    schema     : target schema (default: 'raw' for bronze layer)
    if_exists  : 'append' (default) or 'replace'
    dtype      : optional SQLAlchemy column type overrides
    """
    engine = get_engine()
    ensure_schema(engine, schema)

    # DDL: create or replace table via SQLAlchemy
    with engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f'DROP TABLE IF EXISTS {schema}."{table_name}"'))
        col_defs = ", ".join(f'"{c}" TEXT' for c in df.columns)
        conn.execute(text(
            f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({col_defs})'
        ))

    if df.empty:
        return

    # Bulk insert via raw psycopg2 — avoids pandas/SQLAlchemy version issues
    cols = ", ".join(f'"{c}"' for c in df.columns)
    rows = [
        tuple(None if pd.isna(v) else str(v) for v in row)
        for row in df.itertuples(index=False)
    ]
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(
                cur,
                f'INSERT INTO {schema}."{table_name}" ({cols}) VALUES %s',
                rows,
                page_size=1000,
            )
        raw.commit()
    finally:
        raw.close()


# ── Watermark helpers ────────────────────────────────────────────────────────
# Each pipeline (e.g. 'hypersync', 'decode') stores its last processed
# block_number per (chain, pool) in raw.pipeline_watermarks.
# First run → returns -1, triggering a full backfill.
# Subsequent runs → only new blocks are processed.

_WATERMARK_DDL = """
    CREATE TABLE IF NOT EXISTS raw.pipeline_watermarks (
        pipeline     TEXT        NOT NULL,
        chain_name   TEXT        NOT NULL,
        pool_address TEXT        NOT NULL,
        last_block   BIGINT      NOT NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (pipeline, chain_name, pool_address)
    )
"""


def get_watermark(pipeline: str, chain_name: str, pool_address: str) -> int:
    """
    Return the last successfully processed block_number for this pipeline + pool.
    Returns -1 if no record exists yet (signals a full backfill on first run).
    """
    engine = get_engine()
    try:
        with engine.begin() as conn:
            ensure_schema(engine, "raw")
            conn.execute(text(_WATERMARK_DDL))
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT last_block FROM raw.pipeline_watermarks
                    WHERE pipeline = :pipeline
                      AND chain_name = :chain
                      AND pool_address = :pool
                    """
                ),
                {"pipeline": pipeline, "chain": chain_name, "pool": pool_address.lower()},
            ).fetchone()
            return row[0] if row else -1
    except Exception:
        return -1


def set_watermark(pipeline: str, chain_name: str, pool_address: str, block: int) -> None:
    """Upsert the watermark for this pipeline + pool."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw.pipeline_watermarks
                    (pipeline, chain_name, pool_address, last_block)
                VALUES (:pipeline, :chain, :pool, :block)
                ON CONFLICT (pipeline, chain_name, pool_address)
                DO UPDATE SET last_block = EXCLUDED.last_block,
                              updated_at = now()
                """
            ),
            {
                "pipeline": pipeline,
                "chain": chain_name,
                "pool": pool_address.lower(),
                "block": block,
            },
        )
