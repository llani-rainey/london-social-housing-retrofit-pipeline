{{
  config(materialized='table')
}}

/*
  Borough-level retrofit cost summary.
  Reads the silver EPC Parquet joined to the bronze recommendations CSV.
  Indicative costs are string ranges ("£800 - £1,200") — DuckDB's
  regexp_extract pulls the numbers; midpoint = (low + high) / 2.
*/

with recs_raw as (
    select
        certificate_number,
        improvement_id,
        improvement_summary_text  as improvement_type,
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
        r.improvement_id,
        r.improvement_type,
        r.indicative_cost,
        e.borough,
        -- parse "£800 - £1,200" → low=800, high=1200
        -- regexp_extract_all returns ['800','1,200']; [1]/[2] are 1-based in DuckDB
        try_cast(
            replace(regexp_extract_all(r.indicative_cost, '[\d,]+')[1], ',', '')
        as double)                                                              as cost_low,
        try_cast(
            replace(regexp_extract_all(r.indicative_cost, '[\d,]+')[2], ',', '')
        as double)                                                              as cost_high
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
    count(certificate_number)                                       as total_recommendations,
    round(avg(cost_midpoint), 0)                                    as avg_cost_per_recommendation,
    round(sum(cost_midpoint) / 1e6, 2)                              as total_indicative_cost_m
from with_midpoint
group by borough
order by total_indicative_cost_m desc
