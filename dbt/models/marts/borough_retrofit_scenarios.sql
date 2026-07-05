{{
  config(materialized='table')
}}

/*
  Three cost scenarios (optimistic / central / pessimistic) per borough.
  Reflects RdSAP band uncertainty; real London costs are 15–30% higher
  than these national indicative figures.
*/

with recs_raw as (
    select
        certificate_number,
        indicative_cost
    from read_csv_auto(
        '{{ env_var("REC_GLOB", "../data/bronze/epc_raw/recommendations-*.csv") }}',
        header = true
    )
),

epc_silver as (
    select
        certificate_id  as certificate_number,
        borough
    from read_parquet('{{ env_var("SILVER_EPC_PATH", "../data/silver/epc/**/*.parquet") }}')
    where borough is not null
),

joined as (
    select
        r.certificate_number,
        e.borough,
        try_cast(
            replace(regexp_extract_all(r.indicative_cost, '[\d,]+')[1], ',', '')
        as double)                                                as cost_low,
        try_cast(
            replace(regexp_extract_all(r.indicative_cost, '[\d,]+')[2], ',', '')
        as double)                                                as cost_high
    from recs_raw r
    inner join epc_silver e
        on r.certificate_number = e.certificate_number
),

with_midpoint as (
    select
        *,
        case
            when cost_high is not null then (cost_low + cost_high) / 2.0
            else cost_low
        end as cost_midpoint
    from joined
)

select
    borough,
    count(certificate_number)                                          as total_recommendations,
    count(distinct certificate_number)                                 as properties_with_recs,
    round(sum(cost_low)      / 1e6, 2)                                 as total_optimistic_m,
    round(sum(cost_midpoint) / 1e6, 2)                                 as total_central_m,
    round(sum(cost_high)     / 1e6, 2)                                 as total_pessimistic_m,
    round(sum(cost_low)      / count(distinct certificate_number), 0)  as per_home_optimistic,
    round(sum(cost_midpoint) / count(distinct certificate_number), 0)  as per_home_central,
    round(sum(cost_high)     / count(distinct certificate_number), 0)  as per_home_pessimistic,
    round(
        (sum(cost_high) - sum(cost_low)) / nullif(sum(cost_midpoint), 0) * 100, 1
    )                                                                  as uncertainty_pct
from with_midpoint
group by borough
order by per_home_central desc
