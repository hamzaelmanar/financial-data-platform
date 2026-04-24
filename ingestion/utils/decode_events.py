"""
ABI decoding of raw Uniswap V3 log events.

Reads from raw.raw_hypersync_lp_events + raw.raw_hypersync_blocks in Postgres,
decodes the hex-encoded topics and data fields using Python's arbitrary-precision
integers, and writes one decoded table per event type back to the raw schema.

All decoded numeric columns (uint256, int256, uint128) are stored as TEXT
(decimal string representation of the Python int) to avoid any overflow.
dbt staging owns the CAST to NUMERIC(78) / BIGINT.

Decoded tables written:
    raw.lp_mint_events
    raw.lp_burn_events
    raw.lp_swap_events
    raw.lp_initialize_events

Usage (CLI):
    python -m ingestion.utils.decode_events \
        --chain celo --address 0xF55791...

Usage (programmatic):
    from ingestion.utils.decode_events import decode_and_write
    decode_and_write(chain_name="celo", pool_address="0x...")
"""

import argparse

import pandas as pd
from sqlalchemy import text

from ingestion.utils.db_loader import get_engine, get_watermark, set_watermark, write_dataframe

# ── Uniswap V3 Pool event topic0 hashes (keccak256 of ABI event signature) ──
# Verified against Uniswap V3 Pool contract ABI (Etherscan/Uniswap docs).
TOPIC0_EVENT_MAP = {
    "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": "Mint",
    "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": "Burn",
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": "Swap",
    "0x98636036cb66a21942a7841a974033d1c7cda7e036c4f67c88c2bdfc2240d609": "Initialize",
    "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0": "Collect",
    "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633": "Flash",
    "0xac49e518f90a358f652e4400164f05a5d8f7e35e7747279bc3a93dbf584e125a": "IncreaseObservationCardinalityNext",
}


# ── Low-level hex converters (arbitrary precision) ────────────────────────────

def _to_uint(hexstr: str) -> int:
    return int(hexstr, 16) if hexstr else 0


def _to_int256(hexstr: str) -> int:
    x = _to_uint(hexstr)
    return x - 2**256 if x >= 2**255 else x


def _to_int24(hexstr: str) -> int:
    """topic2/topic3 for tick values are padded to 32 bytes as sign-extended int256."""
    return _to_int256(hexstr)


def _hex_chunk(data_no_prefix: str, word_index: int) -> str:
    """Return the word_index-th 32-byte (64-hex-char) chunk from ABI-encoded data."""
    start = word_index * 64
    return data_no_prefix[start: start + 64]


def _address_from_topic(topic: str) -> str:
    """Strip 12-byte padding from a topic to recover a 20-byte address."""
    return "0x" + topic[-40:] if topic else None


# ── Per-event-type decoders ───────────────────────────────────────────────────

def _decode_mint(row: pd.Series) -> dict:
    """
    Mint(address sender, address indexed owner, int24 indexed tickLower,
         int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    topics: [topic0=sig, topic1=owner, topic2=tickLower, topic3=tickUpper]
    data:   [sender(32), amount(32), amount0(32), amount1(32)]
    """
    d = row["data"][2:]  # strip 0x
    return {
        "sender": _address_from_topic(_hex_chunk(d, 0)),
        "owner": _address_from_topic(row["topic1"]),
        "tick_lower": str(_to_int24(row["topic2"])),
        "tick_upper": str(_to_int24(row["topic3"])),
        "amount": str(_to_uint(_hex_chunk(d, 1))),
        "amount0": str(_to_uint(_hex_chunk(d, 2))),
        "amount1": str(_to_uint(_hex_chunk(d, 3))),
    }


def _decode_burn(row: pd.Series) -> dict:
    """
    Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper,
         uint128 amount, uint256 amount0, uint256 amount1)
    topics: [topic0=sig, topic1=owner, topic2=tickLower, topic3=tickUpper]
    data:   [amount(32), amount0(32), amount1(32)]
    """
    d = row["data"][2:]
    return {
        "owner": _address_from_topic(row["topic1"]),
        "tick_lower": str(_to_int24(row["topic2"])),
        "tick_upper": str(_to_int24(row["topic3"])),
        "amount": str(_to_uint(_hex_chunk(d, 0))),
        "amount0": str(_to_uint(_hex_chunk(d, 1))),
        "amount1": str(_to_uint(_hex_chunk(d, 2))),
    }


def _decode_swap(row: pd.Series) -> dict:
    """
    Swap(address indexed sender, address indexed recipient,
         int256 amount0, int256 amount1, uint160 sqrtPriceX96,
         uint128 liquidity, int24 tick)
    topics: [topic0=sig, topic1=sender, topic2=recipient]
    data:   [amount0(32), amount1(32), sqrtPriceX96(32), liquidity(32), tick(32)]
    """
    d = row["data"][2:]
    return {
        "sender": _address_from_topic(row["topic1"]),
        "recipient": _address_from_topic(row["topic2"]),
        "amount0": str(_to_int256(_hex_chunk(d, 0))),
        "amount1": str(_to_int256(_hex_chunk(d, 1))),
        "sqrt_price_x96": str(_to_uint(_hex_chunk(d, 2))),
        "liquidity": str(_to_uint(_hex_chunk(d, 3))),
        "tick": str(_to_int256(_hex_chunk(d, 4))),
    }


def _decode_initialize(row: pd.Series) -> dict:
    """
    Initialize(uint160 sqrtPriceX96, int24 tick)
    data: [sqrtPriceX96(32), tick(32)]
    """
    d = row["data"][2:]
    return {
        "sqrt_price_x96": str(_to_uint(_hex_chunk(d, 0))),
        "tick": str(_to_int256(_hex_chunk(d, 1))),
    }


_EVENT_DECODERS = {
    "Mint": _decode_mint,
    "Burn": _decode_burn,
    "Swap": _decode_swap,
    "Initialize": _decode_initialize,
}

_EVENT_TABLES = {
    "Mint": "lp_mint_events",
    "Burn": "lp_burn_events",
    "Swap": "lp_swap_events",
    "Initialize": "lp_initialize_events",
}

_BASE_COLS = [
    "block_number",
    "transaction_hash",
    "transaction_index",
    "log_index",
    "timestamp",
    "chain_name",
    "pool_address",
]


# ── Main decode + write ───────────────────────────────────────────────────────

def decode_and_write(chain_name: str, pool_address: str) -> None:
    engine = get_engine()

    last_decoded = get_watermark("decode", chain_name, pool_address)

    # Load only raw logs that haven't been decoded yet (block_number > watermark).
    # Both block_number columns are TEXT from bronze; cast to BIGINT for comparison.
    query = text(
        """
        SELECT
            l.block_number,
            l.transaction_hash,
            l.transaction_index,
            l.log_index,
            b.timestamp,
            l.chain_name,
            l.pool_address,
            l.topic0,
            l.topic1,
            l.topic2,
            l.topic3,
            l.data
        FROM raw.raw_hypersync_lp_events l
        JOIN raw.raw_hypersync_blocks b
            ON l.block_number = b.number
            AND l.chain_name = b.chain_name
        WHERE l.chain_name = :chain
          AND l.pool_address = :pool
          AND l.block_number::BIGINT > :last_decoded
        ORDER BY
            l.block_number::BIGINT,
            l.transaction_index::BIGINT,
            l.log_index::BIGINT
        """
    )
    with engine.connect() as conn:
        raw = pd.read_sql(
            query,
            conn,
            params={"chain": chain_name, "pool": pool_address.lower(), "last_decoded": last_decoded},
        )

    if raw.empty:
        print(f"No new rows to decode for {pool_address} on {chain_name} (watermark: {last_decoded}).")
        return

    print(f"Decoding {len(raw)} new rows for {pool_address} on {chain_name} (blocks > {last_decoded}).")

    # Map topic0 → event name
    raw["event_name"] = raw["topic0"].map(TOPIC0_EVENT_MAP)

    for event_name, decoder in _EVENT_DECODERS.items():
        subset = raw[raw["event_name"] == event_name].copy()
        if subset.empty:
            print(f"  No {event_name} events found, skipping.")
            continue

        decoded_fields = subset.apply(decoder, axis=1, result_type="expand")
        out = pd.concat([subset[_BASE_COLS].reset_index(drop=True), decoded_fields.reset_index(drop=True)], axis=1)
        # Everything as TEXT — dbt staging owns all casts
        out = out.astype(str).replace("None", None)

        table = _EVENT_TABLES[event_name]
        write_dataframe(out, table, schema="raw", if_exists="append")
        print(f"  Wrote {len(out)} {event_name} rows → raw.{table}")

    max_decoded_block = int(raw["block_number"].astype("int64").max())
    set_watermark("decode", chain_name, pool_address, max_decoded_block)
    print(f"  Watermark updated to block {max_decoded_block}.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Decode raw HyperSync LP events")
    parser.add_argument("--chain", required=True)
    parser.add_argument("--address", required=True)
    args = parser.parse_args()
    decode_and_write(chain_name=args.chain, pool_address=args.address)


if __name__ == "__main__":
    main()
