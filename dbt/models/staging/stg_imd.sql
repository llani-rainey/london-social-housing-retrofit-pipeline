{{
  config(materialized='view')
}}

/*
  Staging for IMD 2019 CSV.
  Filters to London boroughs (LA code prefix E09), averages LSOA scores
  up to borough level, and normalises the borough name to uppercase for
  joining with EPC data.
*/

with raw as (
    select *
    from read_csv_auto(
        '{{ env_var("IMD_PATH", "../data/bronze/imd/imd2019_lsoa.csv") }}',
        header = true
    )
),

london_only as (
    select *
    from raw
    where "Local Authority District code (2019)" like 'E09%'
),

borough_agg as (
    select
        upper(trim("Local Authority District name (2019)"))  as borough_key,
        "Local Authority District name (2019)"              as borough_name,
        round(avg("Index of Multiple Deprivation (IMD) Score"), 3)   as imd_score,
        round(avg("Income Score (rate)"), 4)                         as income_deprivation_rate,
        round(avg("Employment Score (rate)"), 4)                     as employment_deprivation_rate,
        round(avg("Health Deprivation and Disability Score"), 3)     as health_deprivation_score,
        round(avg("Living Environment Score"), 3)                    as living_env_score,
        count(*)                                                     as lsoa_count
    from london_only
    group by
        "Local Authority District code (2019)",
        "Local Authority District name (2019)"
)

select * from borough_agg
