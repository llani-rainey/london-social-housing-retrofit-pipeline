{{
  config(materialized='view')
}}

/*
  Staging for silver EPC Parquet.
  Aggregates certificate-level records to borough level — the grain needed
  by all mart models. Pre-1950 is proxied by three construction age bands
  available in the EPC schema.
*/

with raw as (
    select *
    from read_parquet('{{ env_var("SILVER_EPC_PATH", "../data/silver/epc/**/*.parquet") }}')
    where borough is not null
),

borough_agg as (
    select
        borough,
        count(*)                                                                as social_properties,
        round(avg(epc_score), 1)                                               as avg_epc_score,
        round(
            sum(case when below_epc_c = true then 1 else 0 end)::double
            / count(*) * 100, 1
        )                                                                       as pct_below_epc_c,
        round(
            sum(case
                when construction_age_band in (
                    'England and Wales: before 1900',
                    'England and Wales: 1900-1929',
                    'England and Wales: 1930-1949'
                ) then 1 else 0
            end)::double / count(*) * 100, 1
        )                                                                       as pct_pre_1950,
        round(avg(co2_emissions), 2)                                            as avg_co2_per_m2
    from raw
    group by borough
)

select * from borough_agg
