"""
Fetch incentive campaign metadata from the Merkl REST API for a given
opportunity URL and write raw campaign rows to PostgreSQL (bronze layer).

Raw schema — all columns stored with their natural Python types.
No business logic here; staging (dbt) owns renaming and derived columns.

Usage (CLI):
    python -m ingestion.sources.merkl_campaigns \
        --url "https://app.merkl.xyz/opportunities/celo/CLAMM/0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c"

Usage (programmatic):
    from ingestion.sources.merkl_campaigns import ingest
    ingest(merkl_url="https://...")
"""

import argparse
import http.client
import json
from collections import OrderedDict

import pandas as pd
from sqlalchemy import text

from ingestion.utils.db_loader import get_engine, write_dataframe

RAW_CAMPAIGNS_TABLE = "raw_merkl_campaigns"


def _parse_url(url: str) -> tuple[str, str, str]:
    parts = url.split("/")
    return parts[-3], parts[-2], parts[-1]  # chain_name, type, explorer_address


def _get_json(conn: http.client.HTTPSConnection, path: str) -> object:
    conn.request("GET", path, headers={"Accept": "*/*"})
    res = conn.getresponse()
    return json.loads(res.read().decode("utf-8"), object_pairs_hook=OrderedDict)


def _opportunity_id(url: str) -> str:
    chain_name, type_, explorer_address = _parse_url(url)
    conn = http.client.HTTPSConnection("api.merkl.xyz")
    data = _get_json(
        conn,
        f"/v4/opportunities?chainName={chain_name}&type={type_}&explorerAddress={explorer_address}",
    )
    return str(data[0]["id"])


def _fetch_campaigns(opportunity_id: str) -> pd.DataFrame:
    conn = http.client.HTTPSConnection("api.merkl.xyz")
    campaigns_raw = _get_json(conn, f"/v4/campaigns?opportunityId={opportunity_id}")

    rows = []
    for item in campaigns_raw:
        c = dict(item) if isinstance(item, OrderedDict) else item
        params = dict(c.get("params", {}))

        daily_rewards_value = None
        daily_rewards_token_symbol = None
        breakdown = c.get("dailyRewardsBreakdown") or []
        if breakdown:
            first = dict(breakdown[0]) if isinstance(breakdown[0], OrderedDict) else breakdown[0]
            daily_rewards_value = first.get("value")
            token_info = dict(first.get("token", {})) if first.get("token") else {}
            daily_rewards_token_symbol = token_info.get("symbol")

        rows.append(
            {
                "campaign_id": c.get("id"),
                "opportunity_id": opportunity_id,
                "start_timestamp": c.get("startTimestamp"),
                "end_timestamp": c.get("endTimestamp"),
                "amount": c.get("amount"),
                "symbol_reward_token": params.get("symbolRewardToken"),
                "decimals_reward_token": params.get("decimalsRewardToken"),
                "daily_rewards_value": daily_rewards_value,
                "daily_rewards_token_symbol": daily_rewards_token_symbol,
            }
        )

    return pd.DataFrame(rows)


def _existing_campaign_ids() -> set[str]:
    """Return campaign_ids already stored in the bronze table."""
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(
                text("SELECT campaign_id FROM raw.raw_merkl_campaigns")
            ).fetchall()
            return {r[0] for r in rows}
    except Exception:
        return set()


def ingest(merkl_url: str) -> None:
    opp_id = _opportunity_id(merkl_url)
    df = _fetch_campaigns(opp_id)
    print(f"Fetched {len(df)} campaigns for opportunity {opp_id}.")

    # Campaigns are immutable once created — only insert new ones.
    existing = _existing_campaign_ids()
    new_df = df[~df["campaign_id"].isin(existing)]
    if new_df.empty:
        print("No new campaigns to insert.")
        return

    write_dataframe(new_df, RAW_CAMPAIGNS_TABLE, schema="raw", if_exists="append")
    print(f"Wrote {len(new_df)} new campaigns to PostgreSQL ({len(df) - len(new_df)} already existed).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Merkl campaign metadata")
    parser.add_argument("--url", required=True, help="Merkl opportunity URL")
    args = parser.parse_args()
    ingest(merkl_url=args.url)


if __name__ == "__main__":
    main()
