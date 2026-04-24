"""
Fetch Mint/Burn/Swap/Collect/Initialize events from HyperSync for a given
Uniswap V3 pool and write the raw logs + blocks to PostgreSQL (bronze layer).

Raw schema — all hex columns stored as TEXT, no numeric casting here.
Staging (dbt) owns all type coercions.

Usage (CLI):
    python -m ingestion.sources.hypersync_events \
        --chain celo \
        --address 0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c

Usage (programmatic):
    from ingestion.sources.hypersync_events import ingest
    await ingest(chain_name="celo", contract_address="0x...")
"""

import argparse
import asyncio
import os
import tempfile

import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

import hypersync
from hypersync import (
    BlockField,
    HexOutput,
    LogField,
    LogSelection,
)

from ingestion.utils.db_loader import get_watermark, set_watermark, write_dataframe

load_dotenv()

# Raw table names (bronze layer)
RAW_LOGS_TABLE = "raw_hypersync_lp_events"
RAW_BLOCKS_TABLE = "raw_hypersync_blocks"


async def ingest(chain_name: str, contract_address: str) -> None:
    """
    Fetch logs for `contract_address` on `chain_name`, starting from the block
    after the last watermark (or block 0 on first run), and write raw logs +
    blocks to PostgreSQL as TEXT columns.
    """
    client = hypersync.HypersyncClient(
        hypersync.ClientConfig(
            url=f"https://{chain_name}.hypersync.xyz",
            bearer_token=os.getenv("HYPERSYNC_BEARER_TOKEN"),
        )
    )

    field_selection = hypersync.FieldSelection(
        block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        log=[
            LogField.BLOCK_NUMBER,
            LogField.TRANSACTION_HASH,
            LogField.TRANSACTION_INDEX,
            LogField.LOG_INDEX,
            LogField.DATA,
            LogField.ADDRESS,
            LogField.TOPIC0,
            LogField.TOPIC1,
            LogField.TOPIC2,
            LogField.TOPIC3,
        ],
    )

    height = await client.get_height()
    from_block = get_watermark("hypersync", chain_name, contract_address) + 1
    if from_block > height:
        print(f"Already up to date at block {height}. Nothing to fetch.")
        return
    print(f"Fetching from block {from_block} (last watermark: {from_block - 1}).")
    query = hypersync.Query(
        from_block=from_block,
        to_block=height,
        field_selection=field_selection,
        logs=[LogSelection(address=[contract_address], topics=[])],
    )
    config = hypersync.StreamConfig(hex_output=HexOutput.PREFIXED)

    with tempfile.TemporaryDirectory() as tmp:
        print(f"Fetching logs for {contract_address} on {chain_name} …")
        await client.collect_parquet(tmp, query, config)
        print(f"Done. Blocks 0 → {height}. Loading to PostgreSQL …")

        logs_df = pq.read_table(f"{tmp}/logs.parquet").to_pandas()
        blocks_df = pq.read_table(f"{tmp}/blocks.parquet").to_pandas()

    # Normalise column names to lowercase
    logs_df.columns = [c.lower() for c in logs_df.columns]
    blocks_df.columns = [c.lower() for c in blocks_df.columns]

    # Tag every row with chain + pool so the table works for multiple pools
    logs_df["chain_name"] = chain_name
    logs_df["pool_address"] = contract_address.lower()
    blocks_df["chain_name"] = chain_name

    # Cast everything to str — bronze stores TEXT, no type pressure, no overflow
    logs_df = logs_df.astype(str)
    blocks_df = blocks_df.astype(str)

    write_dataframe(logs_df, RAW_LOGS_TABLE, schema="raw", if_exists="append")
    write_dataframe(blocks_df, RAW_BLOCKS_TABLE, schema="raw", if_exists="append")
    set_watermark("hypersync", chain_name, contract_address, height)
    print(
        f"Wrote {len(logs_df)} log rows and {len(blocks_df)} block rows to PostgreSQL."
        f" Watermark updated to block {height}."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest HyperSync LP events")
    parser.add_argument("--chain", required=True, help="Chain name (e.g. celo)")
    parser.add_argument("--address", required=True, help="Pool contract address")
    args = parser.parse_args()
    asyncio.run(ingest(chain_name=args.chain, contract_address=args.address))


if __name__ == "__main__":
    main()
