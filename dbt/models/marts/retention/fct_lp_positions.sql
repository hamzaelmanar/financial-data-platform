/*
  fct_lp_positions
  ─────────────────
  One row per LP (wallet address). This is the SQL port of:
    - metrics.verify_lp_exit()
    - merkl_campaigns.entered_during_campaign()

  Logic:
    1. Compute cumulative liquidity per position_key over time (running sum of
       signed liquidity_delta, ordered by block/tx/log).
    2. A position is "active" if its latest cumulative liquidity > 0.
    3. An LP has "fully exited" if ALL their positions have cumulative_liquidity = 0.
    4. Join with campaigns to flag LPs who entered during an active campaign window.

  Output columns:
    owner                     LP wallet address
    total_positions           distinct tick ranges ever opened
    active_positions          positions still open (cumulative_liq > 0)
    closed_positions          total - active
    status                    1 = fully exited, 0 = still providing liquidity
    first_mint_timestamp      LP entry time (Unix seconds)
    exit_timestamp            last event time if exited, else pool max timestamp
    duration_seconds          exit_timestamp - first_mint_timestamp
    lp_cohort                 'pre_campaign' | 'during_campaign' | 'post_campaign'
                              Pre  = entered before any campaign window opened.
                              During = entered while at least one campaign was active.
                              Post = entered after all campaigns ended.
                              The post cohort captures indirect stickiness: LPs
                              attracted by improved pool depth after rewards ended,
                              not by the rewards themselves.
*/

with

-- 1. Running cumulative liquidity per position, in event order
events_ordered as (
    select
        *,
        row_number() over (
            partition by pool_address, chain_name, position_key
            order by block_number, transaction_index, log_index
        ) as event_seq
    from {{ ref('stg_hypersync__lp_events') }}
),

cumulative_liquidity as (
    select
        *,
        sum(liquidity_delta) over (
            partition by pool_address, chain_name, position_key
            order by block_number, transaction_index, log_index
            rows between unbounded preceding and current row
        ) as cumulative_liquidity
    from events_ordered
),

-- 2. Latest state of each position (last event per position_key)
latest_per_position as (
    select distinct on (chain_name, pool_address, position_key)
        chain_name,
        pool_address,
        position_key,
        owner,
        tick_lower,
        tick_upper,
        cumulative_liquidity,
        block_timestamp
    from cumulative_liquidity
    order by chain_name, pool_address, position_key,
             block_number desc, transaction_index desc, log_index desc
),

-- 3. LP-level aggregation
lp_agg as (
    select
        chain_name,
        pool_address,
        owner,
        count(*)                                            as total_positions,
        sum(case when cumulative_liquidity > 0 then 1 else 0 end) as active_positions
    from latest_per_position
    group by chain_name, pool_address, owner
),

-- 4. First mint timestamp per LP
first_mint as (
    select
        chain_name,
        pool_address,
        owner,
        min(block_timestamp) as first_mint_timestamp
    from {{ ref('stg_hypersync__lp_events') }}
    where event_type = 'Mint'
    group by chain_name, pool_address, owner
),

-- 5. Last event timestamp per LP (used as exit_timestamp for fully exited LPs)
last_event as (
    select
        chain_name,
        pool_address,
        owner,
        max(block_timestamp) as last_event_timestamp
    from {{ ref('stg_hypersync__lp_events') }}
    group by chain_name, pool_address, owner
),

-- Pool-wide max timestamp (censoring time for still-active LPs)
pool_max_ts as (
    select
        chain_name,
        pool_address,
        max(block_timestamp) as max_timestamp
    from {{ ref('stg_hypersync__lp_events') }}
    group by chain_name, pool_address
),

-- 6. Campaign window (global min start → global max end across all campaigns)
campaign_window as (
    select
        min(start_timestamp) as global_start,
        max(end_timestamp)   as global_end
    from {{ ref('stg_merkl__campaigns') }}
),

-- 7. Assemble final LP summary
lp_base as (
    select
        a.chain_name,
        a.pool_address,
        a.owner,
        a.total_positions,
        a.active_positions,
        a.total_positions - a.active_positions              as closed_positions,
        case when a.active_positions = 0 then 1 else 0 end as status,
        f.first_mint_timestamp,
        case
            when a.active_positions = 0 then l.last_event_timestamp
            else p.max_timestamp
        end                                                 as exit_timestamp
    from lp_agg a
    join first_mint  f on a.chain_name = f.chain_name
                       and a.pool_address = f.pool_address
                       and a.owner = f.owner
    join last_event  l on a.chain_name = l.chain_name
                       and a.pool_address = l.pool_address
                       and a.owner = l.owner
    join pool_max_ts p on a.chain_name = p.chain_name
                       and a.pool_address = p.pool_address
)

select
    b.chain_name,
    b.pool_address,
    b.owner,
    b.total_positions,
    b.active_positions,
    b.closed_positions,
    b.status,
    b.first_mint_timestamp,
    b.exit_timestamp,
    b.exit_timestamp - b.first_mint_timestamp           as duration_seconds,
    case
        when b.first_mint_timestamp < c.global_start                         then 'pre_campaign'
        when b.first_mint_timestamp between c.global_start and c.global_end  then 'during_campaign'
        else                                                                       'post_campaign'
    end                                                 as lp_cohort
from lp_base b
cross join campaign_window c
