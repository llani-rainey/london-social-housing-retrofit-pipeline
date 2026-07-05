# London Social Housing Pipeline

[![CI](https://github.com/llani-rainey/london-social-housing-retrofit-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/llani-rainey/london-social-housing-retrofit-pipeline/actions/workflows/ci.yml)

The UK government's **Warm Homes: Social Housing Fund (£1.29bn, 2025–2028)** requires housing associations to identify and prioritise their worst-performing stock to bid for retrofit funding. This pipeline produces exactly that prioritisation.

It ingests three public datasets — EPC certificates, CORE social lettings microdata, and the Index of Multiple Deprivation — cleans and joins them using a **Bronze → Silver → Gold medallion architecture** in PySpark, transforms them with **dbt + DuckDB**, and answers a single research question:

**Which London boroughs have the worst social housing stock AND the most financially vulnerable tenants?**

The output is a borough-level priority ranking that a housing association could use directly to sequence retrofit investment, size a Warm Homes Fund bid, and report stock condition to the Regulator of Social Housing against the April 2030 EPC C target.

The project was built to demonstrate a production-style data engineering workflow: multi-source ingestion with schema handling, medallion layering, joined analysis, and ML-based clustering — all grounded in live government policy rather than a synthetic dataset.

---

## Results

| Priority Rank | Borough | % Social Stock Below EPC C | Income Deprivation Rate | % Pre-1950 Stock |
|---|---|---|---|---|
| 1 | Barking and Dagenham | 56.0% | 19.4% | 44.0% |
| 2 | Haringey | 48.5% | 16.8% | 47.4% |
| 3 | Lambeth | 46.9% | 15.6% | 46.0% |
| 4 | Hammersmith and Fulham | 45.6% | 14.2% | 57.0% |
| 5 | Enfield | 52.4% | 16.8% | 29.1% |
| 6 | Kensington and Chelsea | 46.1% | 12.1% | 52.4% |
| 6 | Camden | 46.8% | 14.0% | 40.4% |

Boroughs are ranked by a composite score averaging three equal-weight dimensions: % of social stock below EPC C (housing quality), IMD 2019 income deprivation rate (financial vulnerability), and % of stock built before 1950 (retrofit difficulty). Rank 1 = highest need for retrofit investment.

**London-wide affordability context (CORE data, 2020–2022):** 60%+ of London social housing tenants spend more than half their income on rent.

**Notable finding:** Kensington & Chelsea appears in the top 10 despite being an affluent borough. It has the highest proportion of pre-1950 social stock in the dataset (57%), and its deprived wards — including the area around Grenfell Tower — have income deprivation rates comparable to East London. Borough-level wealth does not protect social tenants from poor housing conditions.

---

## Policy Context

**Warm Homes: Social Housing Fund (£1.29bn, 2025–2028)**

The fund provides competitive grants to housing associations and councils to retrofit social housing — insulation, heat pumps, solar panels, window upgrades. To bid successfully, landlords must demonstrate which properties are worst performing and make a quantified case for investment. This pipeline addresses that directly:

- The borough priority ranking identifies where to focus bids (worst EPC stock + most deprived tenants)
- The retrofit cost scenarios (optimistic / central / pessimistic per home and total) provide the cost evidence to size a bid
- The improvement-type breakdown shows what works are needed and at what scale
- The wall insulation analysis flags the most expensive category — solid wall properties — by borough

**EPC C target (April 2030)**

All social housing in England must reach EPC C or equivalent. Currently 42.4% of London's social rented stock falls below this threshold. Associations need borough-level tracking to plan retrofit programmes, sequence investment decisions, and report progress to the Regulator of Social Housing. The gold layer `epc_trend_top_boroughs` table shows whether priority boroughs are improving year-on-year.

**Academic validation**

A 2025 study (*Inequitable efficiency*, ScienceDirect) combined 2 million London EPCs with deprivation data across 2011–2021 and reached the same conclusion as this pipeline: income deprivation has overtaken housing age as the primary driver of poor EPC outcomes. This validates combining both signals rather than relying on EPC data alone.

---

## Data Sources

| Dataset | Source | Volume | Geography |
|---|---|---|---|
| Domestic EPC certificates | [MHCLG Open Data](https://epc.opendatacommunities.org/) | 612,357 rows (London social rented) | Borough |
| CORE social lettings microdata | [UK Data Service, SN 9237](https://ukdataservice.ac.uk/) | 501,244 rows (London, 2007–2022) | Region only |
| Index of Multiple Deprivation 2019 | [GOV.UK](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019) | 4,835 LSOAs aggregated to borough | Borough |

**Note on CORE geography:** CORE microdata is anonymised to Government Office Region level (E12000007 = London). No borough breakdown is available in this dataset. Borough-level financial vulnerability is therefore proxied using the IMD 2019 income deprivation domain, which measures the proportion of residents experiencing income deprivation at LSOA level, averaged to borough.

### About EPC Improvement Recommendations

Each EPC certificate includes a set of assessor recommendations — measures that would improve the property's energy rating, with an indicative cost range. There are approximately 40 distinct improvement types, grouped here into six categories:

| Category | Examples | Typical cost range |
|---|---|---|
| **Insulation** | Cavity wall, solid wall, loft, floor, party wall | £300 – £25,000 |
| **Heating system** | Condensing boiler replacement, storage heaters, heat pump | £1,500 – £10,000 |
| **Heating controls** | Thermostats, zone controls, programmers | £200 – £800 |
| **Glazing & draughts** | Double glazing, secondary glazing, draught proofing, doors | £300 – £8,000 |
| **Hot water cylinder** | Cylinder jacket, cylinder thermostat, immersion upgrade | £15 – £400 |
| **Renewables** | Solar PV, solar thermal, heat recovery | £3,000 – £8,000 |

**The critical distinction within insulation: cavity wall vs solid wall.**
- **Cavity wall insulation** (improvement ID 6): ~£1,500. Applies to properties built after approximately 1920, which have a gap between their inner and outer brick layers that can be filled with insulating material. Quick and cost-effective.
- **Solid wall insulation** (improvement ID 7): £8,000–£25,000. Applies to pre-1920 solid-brick Victorian and Edwardian properties, plus some interwar stock. Insulation must be fixed to the inside or outside of the wall, which is disruptive and expensive. This is the single biggest cost driver for London's oldest social housing stock.

The pipeline uses the % of wall insulation recommendations that are solid wall (vs cavity) as a direct retrofit difficulty metric in the clustering analysis — more accurate than using property age as a proxy.

---

## Notebook Structure

Notebooks are numbered so the workflow is easy to follow end to end.

### `01_explore_epc.ipynb` — EPC data exploration
Explores the raw EPC data before any pipeline work. Documents column availability, null rates, EPC score and rating distributions, borough breakdown, construction age distribution, and inspection date range. Key findings: 42.4% below EPC C London-wide, `CO2_EMISS_CURR_PER_FLOOR_AREA` preferred over raw CO2 for cross-property comparability, some certificates are 10+ years old.

### `02_explore_core.ipynb` — CORE data exploration
Documents the most significant data quality challenge in the project: 42 tab files spanning 2007–2022 with column counts ranging from 93 to 213. Explores the two geography coding schemes (numeric `GOVREG=7` vs string `GOVREG=E12000007`), the string band format of income and rent columns, blank string issues in integer columns, and which key columns are absent from earlier file versions.

### `03_ingest_epc.ipynb` — EPC bronze → silver
Reads all `certificates-*.csv` files, filters to London social rented, selects and renames 14 key columns, derives `below_epc_c` flag, and writes partitioned Parquet to `data/silver/epc/`. Output: 612,357 rows partitioned by borough.

### `04_ingest_core.ipynb` — CORE bronze → silver
Reads each of the 42 tab files individually to preserve correct headers, filters to London using both geography coding schemes, handles column name differences across schema versions using conditional selection, applies a band midpoint UDF to convert string ranges to numeric estimates, and unions all years into a single silver table. Output: 501,244 rows partitioned by year, covering 2007–2022.

### `05_build_gold.ipynb` — Gold layer analysis
Aggregates EPC to borough level. Aggregates IMD 2019 from LSOA to borough by averaging income/employment deprivation scores. Joins EPC and IMD on borough name. Computes a composite priority score from three ranked dimensions. Also produces a London-wide affordability trend from CORE (2007–2022) and an EPC improvement trend for the top 5 priority boroughs.

### `06_recommendations_analysis.ipynb` — Retrofit cost analysis
Joins EPC recommendations data to London social rented certificates. Parses indicative cost strings into three numeric bounds (low / midpoint / high) to produce optimistic, central, and pessimistic cost scenarios. Key analyses: most common and expensive improvement types London-wide; top 5 improvements per priority borough; **cavity vs solid wall split by borough** (saved to gold, used in notebook 07 as a retrofit difficulty signal); cost breakdown by the six improvement categories (insulation, heating, controls, glazing, hot water, renewables); **quick wins vs major works split** (measures under/over £1,000) to show whether boroughs still have cheap work outstanding or are into expensive structural territory; and **priority order analysis** showing what assessors most commonly rank as the single biggest lever per borough. Outputs both cost per home (retrofit difficulty comparison) and total borough cost (Warm Homes Fund bid sizing).

### `07_clustering_analysis.ipynb` — ML clustering (alternative prioritisation)
Addresses a double-counting problem in the composite ranking: pre-1950 stock and EPC quality are correlated, so the original ranking penalises old stock twice. Fixes this by replacing % pre-1950 with **% solid wall recommendations** (from notebook 06 gold output) — a direct retrofit cost signal from actual surveyor assessments rather than a proxy. Then tries three ML approaches — PCA, K-means (with elbow and silhouette methods to find optimal k), and hierarchical clustering (with dendrogram) — and compares all outputs side by side. Conclusion: the top priority boroughs are robust across every method, validating the core findings regardless of methodology.

---

## Architecture

```
Raw data (EPC CSVs, CORE .tab files, IMD CSV)
        ↓
PySpark ingestion — src/ingest_epc.py, src/ingest_core.py
(handles 42 heterogeneous schemas, UDFs, geography mismatches)
        ↓
Silver Parquet (data/silver/) — cleaned, typed, filtered
        ↓
dbt transformations — dbt/models/
(borough aggregations, EPC+IMD join, cost models, priority ranking)
        ↓
Gold (data/gold/) — DuckDB via dbt + Parquet via PySpark analysis scripts
        ↓
ML validation — notebooks/07_clustering_analysis.ipynb
(PCA, K-means, hierarchical clustering — reads PySpark gold Parquet)
```

**Why this tool split?**
- **PySpark**: handles the raw ingestion challenges — 42 heterogeneous schema variants, multi-GB CSV, Python UDFs for string-to-numeric conversion. The right tool when you're wrangling raw files.
- **dbt + DuckDB**: implements the business logic transformations — joins, rankings, cost aggregations — in plain, tested, documented SQL. DuckDB reads Parquet natively; no warehouse needed. Makes the transformation layer auditable, testable, and easy to inspect.
- **Jupyter notebooks**: the design and validation layer. Notebooks 05/06 document the methodology behind the dbt models; notebook 07 applies ML to validate the priority ranking.

**Data layout:**

```
data/
├── bronze/          # Raw data, untransformed
│   ├── epc_raw/     # certificates-*.csv + recommendations-*.csv
│   ├── core_raw/    # 42 tab-delimited files from UK Data Service
│   └── imd/         # IMD 2019 LSOA CSV (File 7 from GOV.UK)
│
├── silver/          # Cleaned, typed, filtered — Parquet format
│   ├── epc/         # 612k rows, partitioned by borough
│   └── core/        # 501k rows, partitioned by year
│
└── gold/            # Aggregated, analysis-ready tables
    ├── london_housing.duckdb        # dbt output — all mart models
    ├── borough_priority/            # (PySpark fallback) 33 boroughs, composite score
    ├── london_affordability_trend/  # Rent-to-income ratio 2007–2022
    ├── epc_trend_top_boroughs/      # EPC improvement over time, top 5 boroughs
    ├── borough_retrofit_costs/      # Avg cost per recommendation per borough
    ├── borough_retrofit_scenarios/  # Optimistic / central / pessimistic cost per home + total
    └── borough_wall_type/           # Cavity vs solid wall insulation split per borough
```

---

## Key Engineering Decisions

**CORE schema split**

The 42 CORE files cannot be read with a single glob — doing so causes Spark to apply one file's header to all files, putting data into the wrong columns for every file with a different schema. The fix is to read each file individually in a Python loop, select the required columns by name (not position), handle missing columns with `lit(None)` fallbacks, then union all results. This adds some processing time but is the only correct approach when source files share a naming convention but not a schema.

**Dual geography filter**

Old CORE files (2007–2018) encode London as `GOVREG = '7'`. New files (2018–2022) use `GOVREG = 'E12000007'`. A single filter handles both: `.filter((trim(col('GOVREG')) == '7') | (trim(col('GOVREG')) == 'E12000007'))`.

**Band midpoint UDF**

CORE income and rent fields are stored as string ranges — e.g. `"151 to 190"`, `"More than 500"`, `"Less than 50"`. A Python UDF converts these to numeric midpoints for aggregation: `"151 to 190"` → `170.5`, `"More than 500"` → `500.0`, `"Less than 50"` → `25.0`. This introduces measurement error at the individual row level but is reliable for aggregated trends.

**Safe integer casting**

Several CORE columns (`HHMEMBT`, `BEDST`, `BED_MINUS_BEDSTANDARD`) contain blank strings `' '` rather than nulls. A direct `.cast('int')` raises a `NumberFormatException`. All integer casts are wrapped: `when(trim(col(c)) == '', None).otherwise(trim(col(c))).cast('int')`.

**IMD aggregation**

IMD 2019 is published at LSOA level (~32,000 areas nationally). London boroughs are identified by Local Authority District codes starting `E09`. LSOA scores are averaged within each borough to produce a single income deprivation rate and IMD score per borough, then joined to EPC borough stats on normalised (uppercased, trimmed) borough name.

**Composite priority score**

Each borough receives a rank 1–33 on three dimensions: % below EPC C, income deprivation rate, and % pre-1950 stock. The composite score is the average of these three ranks. Equal weighting reflects the assumption that housing condition and tenant deprivation contribute equally to retrofit urgency. Alternative weighting schemes (e.g. doubling the income deprivation weight to reflect a tenant-focused mission) would not change the top 3 boroughs substantially.

---

## Exploratory Findings

**EPC data**
- London social rented stock averages EPC score 65 (low band C). The national social housing average is higher — London's older stock pulls it down.
- Rating distribution is bimodal: a cluster around band D and another around band C, suggesting many properties are close to but not reaching the 2030 target.
- Pre-1950 stock accounts for roughly 30% of London social lettings inspected. These properties typically have solid walls, original windows, and no cavity insulation — the most expensive properties to retrofit.
- Some EPC certificates are 10+ years old. The dataset reflects stock condition at inspection, not necessarily today.

**CORE data**
- The dataset covers 2007–2022 with 501k London lettings after filtering. Year 2022 has only ~5,400 rows because CORE runs April–March financial years; most 2021-22 lettings have `YEAR=2021`.
- Income and rent band formats are consistent within each file year but vary slightly across years. The midpoint UDF handles all observed variants.
- `econstat_imputed_R` (employment status) and `TENANCYLENGTH_Bands` are absent from files before 2012-13 — these are null for earlier years in the silver table.
- CORE does not provide borough-level geography. This is a structural limitation of the public end-user licence version of the dataset.

---

## Limitations

- **CORE recency**: CORE data runs to 2021–22 only. No more recent social lettings microdata is publicly available without secure access to the UK Data Service.
- **Band midpoint noise**: Individual rent-to-income ratios are approximations. Tenants in the lowest income bands (e.g. under £50/week) produce extreme ratios that skew averages. The `pct_spending_over_50pct_on_rent` metric is more robust than the mean ratio.
- **IMD vintage**: IMD 2019 is used. A 2025 update exists but borough-level summaries were not yet published at time of build.
- **EPC inspection lag**: Certificates reflect the property at time of inspection. High inspection volumes in 2022–2024 (driven by retrofit programme requirements) may mean recent certificates skew towards properties that have already been improved.
- **Equal weighting**: The composite priority score weights all three dimensions equally. This is a modelling assumption, not a fact.
- **Indicative retrofit costs are national, not London-specific**: The cost estimates in `recommendations-*.csv` (e.g. "£800 – £1,200") are fixed bands generated by RdSAP (Reduced Data Standard Assessment Procedure) software. They do not vary by region — the same band is output regardless of whether the property is in London or rural Norfolk. London construction labour costs are typically [15–30% higher than the national average](https://rapidqs.co.uk/construction-cost-per-m-uk-2026-by-building-type-and-region/), so actual retrofit costs for the priority boroughs will exceed the indicative figures. These should be treated as order-of-magnitude estimates only, not as London-specific quotes. Sources: [GOV.UK EPC Reform Consultation (2024)](https://www.gov.uk/government/consultations/reforms-to-the-energy-performance-of-buildings-framework); [RdSAP 10 Changes — EPCGuide](https://www.epcguide.co.uk/rdsap-10).

---

## Tech Stack

- **PySpark** (local mode) — raw ingestion and silver build (bronze → silver)
- **dbt-duckdb** — SQL transformation layer between silver and gold; 6 models, 24 schema tests
- **DuckDB** — reads silver Parquet directly; no warehouse or cloud credentials needed
- **Parquet with Snappy compression** — silver storage format
- **scikit-learn** — PCA, K-means, hierarchical clustering in notebook 07
- **Python 3.13**, Java 17 (OpenJDK), Jupyter notebooks
- Designed to port directly to **Azure Databricks** — medallion pattern, partitioned Parquet, and PySpark UDFs are all Databricks-native. On Databricks, dbt-databricks replaces dbt-duckdb, and `OPTIMIZE`/`ZORDER BY borough` would be added to the gold layer.

---

## Setup

```bash
# Requirements: Java 17, Python 3.10+
pip install -r requirements.txt
```

**Data files are not included in this repository.** Download links:
- **EPC**: [epc.opendatacommunities.org](https://epc.opendatacommunities.org/) — domestic certificates + recommendations for all London local authorities → `data/bronze/epc_raw/`
- **CORE**: [UK Data Service SN 9237](https://ukdataservice.ac.uk/) — free registration required, download TAB format → `data/bronze/core_raw/tab/`
- **IMD**: [GOV.UK File 7](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019) — direct CSV download → `data/bronze/imd/imd2019_lsoa.csv`

### Notebook path (interactive / exploratory)

```bash
jupyter lab
# Run notebooks in order: 01 → 02 → 03 → 04 → 05 → 06 → 07
```

### Production pipeline path (script + dbt)

```bash
# 1. Ingest bronze → silver, build PySpark gold
python src/pipeline.py

# 2. Run dbt transformations (silver → gold via DuckDB)
cd dbt
dbt run --profiles-dir .   # builds all 6 models
dbt test --profiles-dir .  # runs 24 schema tests
dbt docs generate --profiles-dir .
dbt docs serve             # browse lineage graph at http://localhost:8080
```

### Tests (no Spark required)

```bash
pytest tests/test_helpers.py -v   # 18 pure Python tests, runs in CI
pytest tests/test_gold.py -v      # validates CSV exports — skips unless notebooks 05–07 have been run
```

---

## Files

### Notebooks

| File | Purpose |
|---|---|
| `notebooks/01_explore_epc.ipynb` | EPC raw data exploration and quality assessment |
| `notebooks/02_explore_core.ipynb` | CORE raw data exploration, schema discovery, quality issues |
| `notebooks/03_ingest_epc.ipynb` | EPC bronze → silver ingestion pipeline |
| `notebooks/04_ingest_core.ipynb` | CORE bronze → silver ingestion pipeline (multi-schema handling) |
| `notebooks/05_build_gold.ipynb` | Design notebook — EPC + IMD join, composite priority ranking (→ implemented in dbt) |
| `notebooks/06_recommendations_analysis.ipynb` | Design notebook — retrofit cost analysis, wall type split (→ implemented in dbt) |
| `notebooks/07_clustering_analysis.ipynb` | ML clustering: PCA, K-means, hierarchical clustering vs composite rank |

### src/ — PySpark pipeline scripts

| File | Purpose |
|---|---|
| `src/config.py` | Path resolution (`ROOT / data / bronze/silver/gold`) |
| `src/helpers.py` | Pure Python helpers: `band_midpoint`, `cost_midpoint/low/high` |
| `src/ingest_epc.py` | EPC bronze → silver (filters, column selection, fuel_category) |
| `src/ingest_core.py` | CORE bronze → silver (42-file loop, schema variants, UDFs) |
| `src/build_gold.py` | Gold: EPC+IMD join, priority ranking, affordability trend |
| `src/analyse_recommendations.py` | Gold: retrofit costs, scenarios, wall type |
| `src/pipeline.py` | Orchestrates all 4 stages in sequence |

### dbt/ — SQL transformation layer

| File | Purpose |
|---|---|
| `dbt/models/staging/stg_epc.sql` | Borough-level EPC aggregates from silver Parquet |
| `dbt/models/staging/stg_imd.sql` | IMD 2019 LSOA → borough aggregation |
| `dbt/models/marts/borough_priority.sql` | Composite priority ranking for 33 boroughs |
| `dbt/models/marts/borough_retrofit_costs.sql` | Avg and total retrofit cost per borough |
| `dbt/models/marts/borough_retrofit_scenarios.sql` | Optimistic / central / pessimistic cost scenarios |
| `dbt/models/marts/borough_wall_type.sql` | Cavity vs solid wall split (retrofit difficulty) |
| `dbt/models/schema.yml` | Column descriptions and 24 data quality tests |

### tests/

| File | Purpose |
|---|---|
| `tests/test_helpers.py` | 18 pure Python tests for UDF functions — runs in CI, no Spark |
| `tests/test_gold.py` | Gold layer validation against CSV exports — local only (requires running notebooks 05–07 first) |
| `tests/test_silver_epc.py` | Spark-based EPC silver validation (local only) |
| `tests/test_silver_core.py` | Spark-based CORE silver validation (local only) |
