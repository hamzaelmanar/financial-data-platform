/*
  stg_hypersync__lp_events
  ─────────────────────────
  Combines decoded Mint + Burn events into a single, typed event stream.
  One row per liquidity event. Swap events are excluded — they belong in
  a separate model (used only for price derivation in fct_pool_tvl).

  Key additions vs. bronze:
    - position_key    : dbt_utils surrogate key (MD5) over chain/pool/owner/ticks
    - liquidity_delta : signed NUMERIC(78) — positive for Mint, negative for Burn
    - event_type      : 'Mint' | 'Burn'

  All NUMERIC(78) casts are exact — the bronze TEXT columns hold decimal
  string representations of Python arbitrary-precision ints (no overflow risk).
*/

with

mint_events as (
    select
        block_number::bigint                    as block_number,
        transaction_hash,
        transaction_index::bigint               as transaction_index,
        log_index::bigint                       as log_index,
        timestamp::bigint                       as block_timestamp,
        chain_name,
        pool_address,
        'Mint'                                  as event_type,
        lower(owner)                            as owner,
        lower(sender)                           as sender,
        tick_lower::bigint                      as tick_lower,
        tick_upper::bigint                      as tick_upper,
        cast(amount as numeric(78))             as liquidity_raw,
        cast(amount as numeric(78))             as liquidity_delta,   -- positive
        cast(amount0 as numeric(78))            as amount0_raw,
        cast(amount1 as numeric(78))            as amount1_raw
    from {{ source('raw', 'lp_mint_events') }}
),

burn_events as (
    select
        block_number::bigint                    as block_number,
        transaction_hash,
        transaction_index::bigint               as transaction_index,
        log_index::bigint                       as log_index,
        timestamp::bigint                       as block_timestamp,
        chain_name,
        pool_address,
        'Burn'                                  as event_type,
        lower(owner)                            as owner,
        null::text                              as sender,
        tick_lower::bigint                      as tick_lower,
        tick_upper::bigint                      as tick_upper,
        cast(amount as numeric(78))             as liquidity_raw,
        -cast(amount as numeric(78))            as liquidity_delta,   -- negative
        cast(amount0 as numeric(78))            as amount0_raw,
        cast(amount1 as numeric(78))            as amount1_raw
    from {{ source('raw', 'lp_burn_events') }}
    where cast(amount as numeric(78)) > 0  -- exclude zero-amount burns (dust)
),

unioned as (
    select * from mint_events
    union all
    select * from burn_events
)

select
    block_number,
    transaction_hash,
    transaction_index,
    log_index,
    block_timestamp,
    chain_name,
    pool_address,
    event_type,
    owner,
    sender,
    tick_lower,
    tick_upper,
    liquidity_raw,
    liquidity_delta,
    amount0_raw,
    amount1_raw,
    -- position_key: stable 32-char surrogate key for one LP range position
    -- MD5(chain_name, pool_address, owner, tick_lower, tick_upper)
    {{ dbt_utils.generate_surrogate_key(['chain_name', 'pool_address', 'owner', 'tick_lower', 'tick_upper']) }} as position_key
from unioned
