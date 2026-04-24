/*
  fct_campaign_retention
  ───────────────────────
  Campaign-level LP retention KPIs. One row per pool.

  Three cohorts from fct_lp_positions.lp_cohort:
    pre_campaign    LPs who entered before any incentive window — baseline behaviour.
    during_campaign LPs who entered while rewards were live — direct campaign effect.
    post_campaign   LPs who entered after all rewards ended — indirect stickiness signal.
                    If this cohort is large and sticky, the campaign durably improved
                    pool depth beyond the incentive period.

  Retention is measured from each LP's own entry date (survival-analysis style),
  NOT from the campaign end date. A "7-day retained" LP stayed at least 7 days
  from when they first entered — or is still active at observation time.
*/

with

lp as (
    select * from {{ ref('fct_lp_positions') }}
),

retention as (
    select
        chain_name,
        pool_address,
        lp_cohort,
        count(*)                                                    as n_lps,
        -- 7-day retention from LP entry (604800 seconds)
        round(
            100.0 * sum(
                case when duration_seconds >= 604800 or status = 0 then 1 else 0 end
            )::numeric / nullif(count(*), 0),
            2
        )                                                           as retention_7d_pct,
        -- 30-day retention from LP entry (2592000 seconds)
        round(
            100.0 * sum(
                case when duration_seconds >= 2592000 or status = 0 then 1 else 0 end
            )::numeric / nullif(count(*), 0),
            2
        )                                                           as retention_30d_pct,
        -- Median hold time in seconds (exited LPs only — status = 1)
        percentile_cont(0.5) within group (
            order by case when status = 1 then duration_seconds end
        )                                                           as median_hold_seconds
    from lp
    group by chain_name, pool_address, lp_cohort
)

select
    chain_name,
    pool_address,
    sum(n_lps)                                                              as n_lps_total,
    max(case when lp_cohort = 'pre_campaign'    then n_lps end)             as n_lps_pre_campaign,
    max(case when lp_cohort = 'during_campaign' then n_lps end)             as n_lps_during_campaign,
    max(case when lp_cohort = 'post_campaign'   then n_lps end)             as n_lps_post_campaign,
    -- Direct campaign effect
    max(case when lp_cohort = 'during_campaign' then retention_7d_pct end)  as retention_7d_during,
    max(case when lp_cohort = 'during_campaign' then retention_30d_pct end) as retention_30d_during,
    max(case when lp_cohort = 'during_campaign' then median_hold_seconds end) as median_hold_s_during,
    -- Baseline (pre-campaign)
    max(case when lp_cohort = 'pre_campaign'    then retention_7d_pct end)  as retention_7d_pre,
    max(case when lp_cohort = 'pre_campaign'    then retention_30d_pct end) as retention_30d_pre,
    max(case when lp_cohort = 'pre_campaign'    then median_hold_seconds end) as median_hold_s_pre,
    -- Indirect stickiness signal (post-campaign)
    max(case when lp_cohort = 'post_campaign'   then retention_7d_pct end)  as retention_7d_post,
    max(case when lp_cohort = 'post_campaign'   then retention_30d_pct end) as retention_30d_post,
    max(case when lp_cohort = 'post_campaign'   then median_hold_seconds end) as median_hold_s_post
from retention
group by chain_name, pool_address
