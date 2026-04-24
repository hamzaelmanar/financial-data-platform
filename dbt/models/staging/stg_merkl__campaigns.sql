/*
  stg_merkl__campaigns
  ─────────────────────
  Typed and renamed campaign metadata from the Merkl REST API.
  Adds campaign_duration_days as a convenience column.
*/

select
    campaign_id,
    opportunity_id,
    cast(start_timestamp as bigint)     as start_timestamp,
    cast(end_timestamp as bigint)       as end_timestamp,
    cast(amount as numeric(78))         as total_reward_amount,
    symbol_reward_token,
    cast(decimals_reward_token as int)  as decimals_reward_token,
    cast(daily_rewards_value as numeric(20, 6)) as daily_rewards_usd,
    daily_rewards_token_symbol,
    -- derived
    round(
        (cast(end_timestamp as bigint) - cast(start_timestamp as bigint))::numeric / 86400,
        0
    )::int                              as campaign_duration_days
from {{ source('raw', 'raw_merkl_campaigns') }}
where campaign_id is not null
