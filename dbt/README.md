# dbt — London Housing Pipeline

This dbt project sits between the PySpark silver layer and the gold analysis tables.
It uses [dbt-duckdb](https://github.com/duckdb/dbt-duckdb), which reads Parquet natively — no warehouse or cloud credentials needed.

## Architecture position

```
PySpark ingestion (src/ingest_*.py)
        ↓
Silver Parquet (data/silver/)
        ↓
dbt transformations ← you are here
        ↓
Gold tables (data/gold/london_housing.duckdb)
        ↓
Notebooks 07 + analysis
```

## Setup

```bash
pip install dbt-duckdb
cd dbt
dbt deps          # install packages (none required currently)
dbt run           # build all models
dbt test          # run schema tests
dbt docs generate # build docs catalog
dbt docs serve    # open lineage graph in browser
```

Run `dbt run` from the `dbt/` directory. Profiles are in `profiles.yml` — no separate `~/.dbt/profiles.yml` needed because the file is in the project directory.

## Models

### Staging

| Model | Source | Description |
|-------|--------|-------------|
| `stg_epc` | `data/silver/epc/` | Borough-level EPC aggregates (612k certificates → 33 boroughs) |
| `stg_imd` | `data/bronze/imd/imd2019_lsoa.csv` | IMD 2019 LSOA scores averaged to borough level |

### Marts

| Model | Description |
|-------|-------------|
| `borough_priority` | Composite retrofit priority rank for all 33 London boroughs |
| `borough_retrofit_costs` | Average and total indicative retrofit cost per borough |
| `borough_retrofit_scenarios` | Optimistic / central / pessimistic cost scenarios |
| `borough_wall_type` | Cavity vs solid wall insulation split (retrofit difficulty proxy) |

## Why dbt here?

- **Staging models** clean and normalise the PySpark output — borough name uppercase normalisation, pre-1950 band grouping, null-safe cost parsing.
- **Mart models** implement the business logic — composite ranking, scenario arithmetic — in plain SQL with a full audit trail.
- **dbt tests** catch data quality issues before they reach the analysis layer: 33 boroughs, no nulls, ranks 1–33.
- **dbt docs** generate a navigable data catalog with column-level lineage that's easy to demo in interviews.

## Environment variables

The models use `env_var()` with sensible defaults so they run without configuration:

| Variable | Default |
|----------|---------|
| `SILVER_EPC_PATH` | `../data/silver/epc/**/*.parquet` |
| `IMD_PATH` | `../data/bronze/imd/imd2019_lsoa.csv` |
| `REC_GLOB` | `../data/bronze/epc_raw/recommendations-*.csv` |
| `DBT_DB_PATH` | `../data/gold/london_housing.duckdb` |
