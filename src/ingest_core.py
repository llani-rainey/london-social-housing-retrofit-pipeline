"""
ingest_core.py — Bronze → Silver for CORE lettings data

Source: https://www.gov.uk/government/collections/continuous-recording-of-social-housing-lettings-and-sales-core-data
Download: Lettings data (general needs), latest available years
File: CORE_Lettings_*.csv

Bronze: raw CSV landed to Parquet as-is
Silver: cleaned, typed, filtered to London, key columns selected
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, round as spark_round
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder.master("local[*]").appName("core_ingest").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BRONZE_PATH = "../data/bronze/core"
SILVER_PATH = "../data/silver/core"
RAW_CSV = "../data/bronze/core_raw/"  # update path after download — can glob multiple years

# ── Bronze: land raw CSV to Parquet ──────────────────────────────────────────
raw = spark.read.csv(RAW_CSV, header=True, inferSchema=False)
raw.write.mode("overwrite").parquet(BRONZE_PATH)
print(f"Bronze: {raw.count():,} rows written to {BRONZE_PATH}")

# ── Silver: clean + filter ────────────────────────────────────────────────────
bronze = spark.read.parquet(BRONZE_PATH)

silver = (
    bronze
    .select(
        trim(col("POSTCODE")).alias("postcode"),
        trim(col("LANDLORD")).alias("landlord"),           # housing association name
        trim(col("LA")).alias("borough_code"),             # local authority code
        trim(col("LANAME")).alias("borough"),
        col("YEAR").cast(IntegerType()).alias("year"),
        trim(col("NEEDSTYPE")).alias("needs_type"),        # general needs vs supported
        trim(col("LETTYPE")).alias("let_type"),            # new let vs transfer
        col("HHMEMB").cast(IntegerType()).alias("household_size"),
        col("HHAGE").cast(IntegerType()).alias("lead_tenant_age"),
        trim(col("ETHNIC")).alias("ethnicity"),
        trim(col("ECSTAT1")).alias("employment_status"),
        col("INCOME1").cast(FloatType()).alias("income_weekly"),
        col("TSHORTFALL").cast(FloatType()).alias("rent_shortfall"),  # affordability gap
        col("RENT").cast(FloatType()).alias("rent_weekly"),
        col("HBELIGAMT").cast(FloatType()).alias("housing_benefit"),
        col("WAITLIST").cast(IntegerType()).alias("months_on_waitlist"),
        trim(col("PREVTEN")).alias("previous_tenure"),     # where they came from
        trim(col("REASONFOR")).alias("reason_for_letting"),
    )
    # filter to London boroughs only
    .filter(col("borough").isNotNull())
    # add affordability flag: rent > 30% of weekly income
    .withColumn(
        "rent_unaffordable",
        when(
            (col("income_weekly") > 0) &
            ((col("rent_weekly") / col("income_weekly")) > 0.3),
            True
        ).otherwise(False)
    )
    # rent-to-income ratio
    .withColumn(
        "rent_to_income_pct",
        spark_round((col("rent_weekly") / col("income_weekly")) * 100, 1)
    )
)

silver.write.mode("overwrite").partitionBy("year").parquet(SILVER_PATH)
print(f"Silver: {silver.count():,} lettings rows written to {SILVER_PATH}")
