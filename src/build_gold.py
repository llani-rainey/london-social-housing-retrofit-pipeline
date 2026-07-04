"""
build_gold.py — Silver → Gold analysis tables

Joins EPC (buildings) + CORE (tenants) on postcode + borough
Produces aggregated tables answering key questions for a London social housing association:

Gold tables:
  1. borough_retrofit_priority  — which boroughs have most sub-EPC-C social stock
  2. borough_affordability       — rent vs income by borough and year
  3. retrofit_vs_vulnerability   — where worst stock meets most vulnerable tenants
  4. epc_trend                   — EPC rating distribution over time (lodgement year)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, round as spark_round,
    year as spark_year, when, desc
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").appName("build_gold").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SILVER_EPC  = "../data/silver/epc"
SILVER_CORE = "../data/silver/core"
GOLD_PATH   = "../data/gold"

epc  = spark.read.parquet(SILVER_EPC)
core = spark.read.parquet(SILVER_CORE)

# ── Gold 1: Borough retrofit priority ────────────────────────────────────────
# Which boroughs have the most sub-EPC-C social housing stock?
borough_retrofit = (
    epc
    .groupBy("borough")
    .agg(
        count("*").alias("total_properties"),
        spark_sum(when(col("below_epc_c"), 1).otherwise(0)).alias("below_c_count"),
        avg("epc_score").alias("avg_epc_score"),
        avg("co2_emissions").alias("avg_co2"),
        avg("floor_area_m2").alias("avg_floor_area"),
    )
    .withColumn(
        "pct_below_c",
        spark_round((col("below_c_count") / col("total_properties")) * 100, 1)
    )
    .orderBy(desc("pct_below_c"))
)
borough_retrofit.write.mode("overwrite").parquet(f"{GOLD_PATH}/borough_retrofit_priority")
borough_retrofit.show(10)

# ── Gold 2: Borough affordability ────────────────────────────────────────────
# Rent vs income by borough and year
borough_affordability = (
    core
    .filter(col("income_weekly") > 0)
    .groupBy("borough", "year")
    .agg(
        count("*").alias("new_lettings"),
        avg("rent_weekly").alias("avg_rent_weekly"),
        avg("income_weekly").alias("avg_income_weekly"),
        avg("rent_to_income_pct").alias("avg_rent_to_income_pct"),
        spark_sum(when(col("rent_unaffordable"), 1).otherwise(0)).alias("unaffordable_count"),
        avg("months_on_waitlist").alias("avg_months_on_waitlist"),
    )
    .withColumn(
        "pct_unaffordable",
        spark_round((col("unaffordable_count") / col("new_lettings")) * 100, 1)
    )
    .orderBy("borough", "year")
)
borough_affordability.write.mode("overwrite").parquet(f"{GOLD_PATH}/borough_affordability")
borough_affordability.show(10)

# ── Gold 3: Retrofit vs vulnerability ────────────────────────────────────────
# Join EPC + CORE on borough — where does worst stock meet most vulnerable tenants?
epc_borough_summary = (
    epc
    .groupBy("borough")
    .agg(
        avg("epc_score").alias("avg_epc_score"),
        spark_round(
            (spark_sum(when(col("below_epc_c"), 1).otherwise(0)) / count("*")) * 100, 1
        ).alias("pct_below_c")
    )
)

core_borough_summary = (
    core
    .filter(col("income_weekly") > 0)
    .groupBy("borough")
    .agg(
        avg("rent_to_income_pct").alias("avg_rent_to_income_pct"),
        avg("months_on_waitlist").alias("avg_months_on_waitlist"),
        count("*").alias("total_lettings"),
    )
)

retrofit_vs_vulnerability = (
    epc_borough_summary
    .join(core_borough_summary, on="borough", how="inner")
    .withColumn(
        # high priority = lots of bad stock AND unaffordable rents
        "priority_score",
        spark_round(col("pct_below_c") * col("avg_rent_to_income_pct") / 100, 2)
    )
    .orderBy(desc("priority_score"))
)
retrofit_vs_vulnerability.write.mode("overwrite").parquet(f"{GOLD_PATH}/retrofit_vs_vulnerability")
retrofit_vs_vulnerability.show(10)

# ── Gold 4: EPC trend over time ───────────────────────────────────────────────
# Has social housing EPC improved since 2012?
epc_trend = (
    epc
    .withColumn("lodgement_year", spark_year(col("inspection_date")))
    .filter(col("lodgement_year").between(2012, 2024))
    .groupBy("lodgement_year", "epc_rating")
    .agg(count("*").alias("property_count"))
    .orderBy("lodgement_year", "epc_rating")
)
epc_trend.write.mode("overwrite").parquet(f"{GOLD_PATH}/epc_trend")
epc_trend.show(20)

print("Gold layer complete.")
