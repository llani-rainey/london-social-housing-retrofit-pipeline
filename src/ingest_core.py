"""
ingest_core.py — Bronze → Silver for CORE lettings microdata

Source: UK Data Service, SN 9237
Download: Continuous Recording of Social Housing Lettings (general needs), 2007–2022
Files:  data/bronze/core_raw/tab/*.tab  (42 files, heterogeneous schemas)

Key data engineering challenge: the 42 files span 7 schema variants (93–213 columns each).
They cannot be read with a single glob — columns land in different positions and some
columns only exist in certain year ranges. We read each file individually, select by
column NAME (not position), and union the results.

CORE geography: the SN 9237 microdata is anonymised to Government Office Region level.
No borough breakdown in the individual records (unlike the published CORE dashboard).
Old files use GOVREG='7'; new files use GOVREG='E12000007'. Both are London.

Silver output: data/silver/core/  (Parquet, partitioned by year)
  ~501,000 rows — London social rented lettings, 2007–2022

Run: python src/ingest_core.py
Requires: JAVA_HOME pointing to JDK 17+
"""

import glob
import logging
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, trim, udf, when
from pyspark.sql.types import FloatType

from config import BRONZE, SILVER, setup_logging
from helpers import band_midpoint

logger = logging.getLogger(__name__)

def safe_int(c):
    """Cast column to int, treating blank strings as null."""
    return when(trim(col(c)) == "", None).otherwise(trim(col(c))).cast("int")


def process_file(spark: SparkSession, path: str) -> DataFrame | None:
    """Read one .tab file, filter to London, return standardised DataFrame or None."""
    df = spark.read.csv(path, header=True, inferSchema=False, sep="\t")
    cols = set(df.columns)

    df = df.filter((trim(col("GOVREG")) == "7") | (trim(col("GOVREG")) == "E12000007"))
    if not df.head(1):
        return None

    def sc(name):
        return trim(col(name)) if name in cols else lit(None).cast("string")

    def si(name):
        return safe_int(name) if name in cols else lit(None).cast("int")

    if "BED_MINUS_BEDSTANDARD" in cols:
        bed_diff = safe_int("BED_MINUS_BEDSTANDARD")
    elif "BED_MINUS_BEDSTANDARD2" in cols:
        bed_diff = safe_int("BED_MINUS_BEDSTANDARD2")
    else:
        bed_diff = lit(None).cast("int")

    if "WTSHORTFALLHB_Bands" in cols:
        shortfall = trim(col("WTSHORTFALLHB_Bands"))
    elif "WTSHORTFALL_Bands" in cols:
        shortfall = trim(col("WTSHORTFALL_Bands"))
    else:
        shortfall = lit(None).cast("string")

    year_col = "YEAR" if "YEAR" in cols else None

    return df.select(
        (safe_int(year_col) if year_col else lit(None).cast("int")).alias("year"),
        trim(col("GOVREG")).alias("region_code_raw"),
        sc("LETTYPE").alias("let_type"),
        sc("TENANCY").alias("tenancy_type"),
        si("HHMEMBT").alias("household_size"),
        si("BEDST").alias("bedroom_standard"),
        bed_diff.alias("bedrooms_vs_need"),
        sc("PREVTEN_R").alias("previous_tenure"),
        sc("REASON_R").alias("reason_for_letting"),
        sc("WEEKINC_T_Bands").alias("weekly_income_band"),
        sc("WRENT_Bands").alias("weekly_rent_band"),
        shortfall.alias("rent_shortfall_band"),
        sc("ETHNIC_Bands").alias("ethnicity"),
        sc("TENANCYLENGTH_Bands").alias("tenancy_length_band"),
        sc("econstat_imputed_R").alias("employment_status"),
    )


if __name__ == "__main__":
    setup_logging()
    TAB_GLOB   = str(BRONZE / "core_raw" / "tab" / "*.tab")
    SILVER_OUT = str(SILVER / "core")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("core_ingest")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    midpoint_udf = udf(band_midpoint, FloatType())

    all_files = sorted(glob.glob(TAB_GLOB))
    logger.info("CORE tab files found: %d", len(all_files))

    dfs = []
    for path in all_files:
        name = os.path.basename(path)
        result = process_file(spark, path)
        if result is not None:
            n = result.count()
            logger.info("  %s: %s London rows", name, f"{n:,}")
            dfs.append(result)
        else:
            logger.info("  %s: 0 London rows (skipped)", name)

    logger.info("Files with London data: %d", len(dfs))

    if not dfs:
        raise ValueError("No London CORE data found — check data/bronze/core_raw/tab/")
    combined = reduce(DataFrame.union, dfs)

    silver = (
        combined
        .withColumn("region_code", lit("E12000007"))
        .drop("region_code_raw")
        .withColumn("weekly_income_est",  midpoint_udf(col("weekly_income_band")))
        .withColumn("weekly_rent_est",    midpoint_udf(col("weekly_rent_band")))
        .withColumn("rent_shortfall_est", midpoint_udf(col("rent_shortfall_band")))
        .withColumn(
            "rent_to_income_pct",
            when(
                (col("weekly_income_est") > 0) & col("weekly_rent_est").isNotNull(),
                (col("weekly_rent_est") / col("weekly_income_est")) * 100,
            ).otherwise(None),
        )
        .withColumn("overcrowded", when(col("bedrooms_vs_need") < 0, True).otherwise(False))
    )

    silver.write.mode("overwrite").partitionBy("year").parquet(SILVER_OUT)
    total = spark.read.parquet(SILVER_OUT).count()
    logger.info("Silver CORE written: %s rows → %s", f"{total:,}", SILVER_OUT)
