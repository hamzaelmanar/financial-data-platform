/*
  stg_hypersync__swap_events
  ───────────────────────────
  Typed swap events. Used exclusively for price derivation (sqrtPriceX96 → spot
  price) in fct_pool_tvl. Not included in stg_hypersync__lp_events because swaps
  have no position_key and carry different semantics.
*/

select
    block_number::bigint                    as block_number,
    transaction_hash,
    transaction_index::bigint               as transaction_index,
    log_index::bigint                       as log_index,
    timestamp::bigint                       as block_timestamp,
    chain_name,
    pool_address,
    lower(sender)                           as sender,
    lower(recipient)                        as recipient,
    cast(amount0 as numeric(78))            as amount0,
    cast(amount1 as numeric(78))            as amount1,
    cast(sqrt_price_x96 as numeric(78))     as sqrt_price_x96,
    cast(liquidity as numeric(78))          as liquidity,
    tick::bigint                            as tick
from {{ source('raw', 'lp_swap_events') }}
