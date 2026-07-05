{{
  config(materialized='table')
}}

/*
  Cavity vs solid wall insulation split by borough.
  improvement_id = 6 → cavity wall (~£1,500)
  improvement_id = 7 → solid wall (£8,000–£25,000)
  Used as the retrofit difficulty metric in 07_clustering_analysis.ipynb.
*/

with recs_raw as (
    select
        certificate_number,
        try_cast(improvement_id as integer)  as improvement_id
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
        e.borough
    from recs_raw r
    inner join epc_silver e
        on r.certificate_number = e.certificate_number
),

aggregated as (
    select
        borough,
        count(certificate_number)                                   as total_recs,
        sum(case when improvement_id = 6 then 1 else 0 end)        as cavity_wall_recs,
        sum(case when improvement_id = 7 then 1 else 0 end)        as solid_wall_recs
    from joined
    group by borough
),

final as (
    select
        borough,
        total_recs,
        cavity_wall_recs,
        solid_wall_recs,
        (cavity_wall_recs + solid_wall_recs)                        as total_wall_recs,
        round(solid_wall_recs::double  / nullif(cavity_wall_recs + solid_wall_recs, 0) * 100, 1) as pct_solid_wall,
        round(cavity_wall_recs::double / nullif(cavity_wall_recs + solid_wall_recs, 0) * 100, 1) as pct_cavity_wall
    from aggregated
    where (cavity_wall_recs + solid_wall_recs) > 0
)

select * from final
order by pct_solid_wall desc
