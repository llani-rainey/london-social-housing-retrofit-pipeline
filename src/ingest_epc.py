"""
ingest_epc.py — Bronze → Silver for EPC certificate data

Source: https://epc.opendatacommunities.org/
Download: Domestic EPCs → filter to London → all boroughs
Files:  data/bronze/epc_raw/certificates-*.csv  (one file per year, 2012–2026)

Silver output: data/silver/epc/  (Parquet, partitioned by borough)
  612,357 rows — London social rented properties only

Run: python src/ingest_epc.py
Requires: JAVA_HOME pointing to JDK 17+
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, trim, upper, when
from pyspark.sql.functions import year as spark_year
from pyspark.sql.types import FloatType

from config import BRONZE, SILVER, setup_logging
from helpers import validate_schema

logger = logging.getLogger(__name__)

_REQUIRED_COLS = {
    "certificate_number", "local_authority_label", "tenure",
    "region", "current_energy_rating", "current_energy_efficiency",
}


def transform_epc(raw: DataFrame) -> DataFrame:
    return (
        raw
        .select(
            trim(col("certificate_number")).alias("certificate_id"),
            trim(col("postcode")).alias("postcode"),
            trim(col("local_authority")).alias("local_authority_code"),
            trim(col("local_authority_label")).alias("borough"),
            trim(col("tenure")).alias("tenure"),
            trim(col("property_type")).alias("property_type"),
            trim(col("built_form")).alias("built_form"),
            trim(col("construction_age_band")).alias("construction_age_band"),
            trim(col("current_energy_rating")).alias("epc_rating"),
            col("current_energy_efficiency").cast(FloatType()).alias("epc_score"),
            col("total_floor_area").cast(FloatType()).alias("floor_area_m2"),
            trim(col("main_fuel")).alias("main_fuel"),
            col("co2_emissions_current").cast(FloatType()).alias("co2_emissions"),
            col("energy_consumption_current").cast(FloatType()).alias("energy_consumption"),
            col("heating_cost_current").cast(FloatType()).alias("heating_cost"),
            trim(col("walls_energy_eff")).alias("walls_energy_eff"),
            trim(col("roof_energy_eff")).alias("roof_energy_eff"),
            trim(col("windows_energy_eff")).alias("windows_energy_eff"),
            trim(col("mains_gas_flag")).alias("mains_gas_flag"),
            to_date(col("inspection_date"), "yyyy-MM-dd").alias("inspection_date"),
            trim(col("region")).alias("region_code"),
        )
        .filter(upper(col("tenure")).isin("RENTAL (SOCIAL)", "SOCIAL RENTED", "RENTED (SOCIAL)"))
        .filter(col("region_code") == "E12000007")
        .filter(col("epc_rating").isNotNull())
        .withColumn("below_epc_c", when(col("epc_rating").isin("D", "E", "F", "G"), True).otherwise(False))
        .withColumn("inspection_year", spark_year(col("inspection_date")))
        .withColumn(
            "fuel_category",
            when(col("main_fuel").isin("mains gas (not community)", "Gas: mains gas"), "gas_individual")
            .when(col("main_fuel") == "mains gas (community)", "gas_community")
            .when(col("main_fuel").isin("electricity (not community)", "Electricity: electricity, unspecified tariff"), "electric_individual")
            .when(col("main_fuel") == "electricity (community)", "electric_community")
            .when(col("main_fuel").isNotNull(), "other")
            .otherwise(None)
        )
    )


if __name__ == "__main__":
    setup_logging()
    RAW_GLOB   = str(BRONZE / "epc_raw" / "certificates-*.csv")
    SILVER_OUT = str(SILVER / "epc")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("epc_ingest")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.csv(RAW_GLOB, header=True, inferSchema=False)
    logger.info("Raw rows (all England): %s", f"{raw.count():,}")
    validate_schema(raw.columns, _REQUIRED_COLS, "EPC bronze")

    silver = transform_epc(raw)
    silver.write.mode("overwrite").partitionBy("borough").parquet(SILVER_OUT)
    logger.info("Silver EPC written: %s rows → %s", f"{spark.read.parquet(SILVER_OUT).count():,}", SILVER_OUT)
