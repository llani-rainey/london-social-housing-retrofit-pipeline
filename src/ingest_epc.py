"""
ingest_epc.py — Bronze → Silver for EPC data

Source: https://epc.opendatacommunities.org/
Download: Domestic EPCs for London (all boroughs)
File: certificates.csv (inside each borough zip)

Bronze: raw CSV landed to Parquet as-is
Silver: cleaned, typed, filtered to social rented properties
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, upper, trim, when
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master("local[*]").appName("epc_ingest").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BRONZE_PATH = "../data/bronze/epc"
SILVER_PATH = "../data/silver/epc"
RAW_CSV = "../data/bronze/epc_raw/certificates.csv"  # update path after download

# ── Bronze: land raw CSV to Parquet ──────────────────────────────────────────
raw = spark.read.csv(RAW_CSV, header=True, inferSchema=False)
raw.write.mode("overwrite").parquet(BRONZE_PATH)
print(f"Bronze: {raw.count():,} rows written to {BRONZE_PATH}")

# ── Silver: clean + filter ────────────────────────────────────────────────────
bronze = spark.read.parquet(BRONZE_PATH)

silver = (
    bronze
    .select(
        trim(col("LMK_KEY")).alias("property_id"),
        trim(col("POSTCODE")).alias("postcode"),
        trim(col("LOCAL_AUTHORITY_LABEL")).alias("borough"),
        trim(col("TENURE")).alias("tenure"),
        trim(col("PROPERTY_TYPE")).alias("property_type"),
        trim(col("BUILT_FORM")).alias("built_form"),
        trim(col("CURRENT_ENERGY_RATING")).alias("epc_rating"),
        col("CURRENT_ENERGY_EFFICIENCY").cast(FloatType()).alias("epc_score"),
        col("TOTAL_FLOOR_AREA").cast(FloatType()).alias("floor_area_m2"),
        trim(col("MAIN_FUEL")).alias("main_fuel"),
        col("CO2_EMISSIONS_CURRENT").cast(FloatType()).alias("co2_emissions"),
        to_date(col("INSPECTION_DATE"), "yyyy-MM-dd").alias("inspection_date"),
        trim(col("LODGEMENT_DATETIME")).alias("lodgement_date"),
    )
    # filter to social rented only
    .filter(
        upper(col("tenure")).isin("RENTAL (SOCIAL)", "SOCIAL RENTED")
    )
    # drop rows with no EPC rating or postcode
    .filter(col("epc_rating").isNotNull() & col("postcode").isNotNull())
    # add below_c flag — 2030 government retrofit target is EPC C
    .withColumn("below_epc_c", when(col("epc_rating").isin("D", "E", "F", "G"), True).otherwise(False))
)

silver.write.mode("overwrite").partitionBy("borough").parquet(SILVER_PATH)
print(f"Silver: {silver.count():,} social rented rows written to {SILVER_PATH}")
