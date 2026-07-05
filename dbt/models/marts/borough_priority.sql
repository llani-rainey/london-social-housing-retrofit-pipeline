{{
  config(materialized='table')
}}

/*
  Borough priority ranking for the Warm Homes: Social Housing Fund.
  Composite rank = average of three component ranks:
    - EPC rank   (% of stock below EPC C — higher is worse)
    - Income rank (income deprivation rate — higher is worse)
    - Age rank   (% pre-1950 stock — higher is worse)

  CORE microdata has no borough geography (anonymised to GOR level),
  so IMD 2019 income deprivation rate is used as the borough-level
  proxy for tenant financial vulnerability.
*/

with epc as (
    select * from {{ ref('stg_epc') }}
),

imd as (
    select * from {{ ref('stg_imd') }}
),

joined as (
    select
        e.borough,
        e.social_properties,
        e.avg_epc_score,
        e.pct_below_epc_c,
        e.pct_pre_1950,
        e.avg_co2_per_m2,
        i.imd_score,
        i.income_deprivation_rate,
        i.employment_deprivation_rate,
        i.health_deprivation_score,
        i.living_env_score,
        i.lsoa_count
    from epc e
    inner join imd i
        on upper(trim(e.borough)) = i.borough_key
),

ranked as (
    select
        *,
        dense_rank() over (order by pct_below_epc_c desc)      as rank_epc,
        dense_rank() over (order by income_deprivation_rate desc) as rank_income,
        dense_rank() over (order by pct_pre_1950 desc)          as rank_age
    from joined
),

scored as (
    select
        *,
        round((rank_epc + rank_income + rank_age) / 3.0, 1) as priority_score
    from ranked
),

final as (
    select
        dense_rank() over (order by priority_score)  as priority_rank,
        borough,
        priority_score,
        pct_below_epc_c,
        income_deprivation_rate,
        pct_pre_1950,
        avg_epc_score,
        imd_score,
        avg_co2_per_m2,
        social_properties,
        rank_epc,
        rank_income,
        rank_age,
        employment_deprivation_rate,
        health_deprivation_score,
        living_env_score
    from scored
)

select * from final
order by priority_rank
