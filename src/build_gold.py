"""
build_gold.py — Silver → Gold analysis tables

Joins EPC (building stock quality) with IMD 2019 (area-level deprivation) to produce
a borough priority ranking for the Warm Homes: Social Housing Fund retrofit programme.

Important: CORE microdata has no borough breakdown (anonymised to Government Office
Region level). Borough-level tenant vulnerability is proxied using IMD 2019 income
deprivation scores instead. See notebooks/05_build_gold.ipynb for full design rationale.

Gold outputs (data/gold/):
  borough_priority          — 33 London boroughs, composite priority rank
  london_affordability_trend — London-wide rent-to-income trend, 2007–2022
  epc_trend_top_boroughs    — EPC improvement trajectory for the 5 worst boroughs

Run: python src/build_gold.py
Requires: JAVA_HOME pointing to JDK 17+
"""

import logging

from pyspark.sql.functions import (
    avg,
    col,
    count,
    dense_rank,
    desc,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import year as spark_year
from pyspark.sql.window import Window

# Ensure `src/` is on sys.path so `from config import ...` works when this
# script is run from anywhere (e.g. `python src/ingest_epc.py`, `python -m src.ingest_epc`).
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import GOLD, IMD_CSV, SILVER, setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("build_gold")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    EPC_SILVER  = str(SILVER / "epc")
    CORE_SILVER = str(SILVER / "core")
    GOLD_PATH   = str(GOLD)
    IMD_PATH    = str(IMD_CSV)

    epc  = spark.read.parquet(EPC_SILVER)
    core = spark.read.parquet(CORE_SILVER)
    logger.info("EPC silver: %s | CORE silver: %s", f"{epc.count():,}", f"{core.count():,}")

    # ── IMD 2019: aggregate LSOA → borough ───────────────────────────────────────
    imd_raw = spark.read.csv(IMD_PATH, header=True, inferSchema=True)

    imd_borough = (
        imd_raw
        .filter(col("`Local Authority District code (2019)`").startswith("E09"))
        .groupBy(col("`Local Authority District name (2019)`").alias("la_name"))
        .agg(
            spark_round(avg("`Index of Multiple Deprivation (IMD) Score`"), 3).alias("imd_score"),
            spark_round(avg("`Income Score (rate)`"), 4).alias("income_deprivation_rate"),
            spark_round(avg("`Employment Score (rate)`"), 4).alias("employment_deprivation_rate"),
            spark_round(avg("`Health Deprivation and Disability Score`"), 3).alias("health_deprivation_score"),
            spark_round(avg("`Living Environment Score`"), 3).alias("living_env_score"),
            count("*").alias("lsoa_count"),
        )
    )
    logger.info("London boroughs in IMD: %d", imd_borough.count())

    # ── EPC: aggregate to borough ─────────────────────────────────────────────────
    epc_borough = (
        epc
        .groupBy("borough")
        .agg(
            count("*").alias("social_properties"),
            spark_round(avg("epc_score"), 1).alias("avg_epc_score"),
            spark_round(
                spark_sum(when(col("below_epc_c"), 1).otherwise(0)) / count("*") * 100, 1
            ).alias("pct_below_epc_c"),
            spark_round(
                spark_sum(when(col("construction_age_band").isin(
                    "England and Wales: before 1900",
                    "England and Wales: 1900-1929",
                    "England and Wales: 1930-1949",
                ), 1).otherwise(0)) / count("*") * 100, 1
            ).alias("pct_pre_1950"),
            spark_round(avg("co2_emissions"), 2).alias("avg_co2_per_m2"),
        )
    )
    logger.info("Boroughs in EPC: %d", epc_borough.count())

    # ── Join EPC + IMD on normalised borough name ─────────────────────────────────
    epc_norm = epc_borough.withColumn("borough_key", upper(trim(col("borough"))))
    imd_norm = imd_borough.withColumn("borough_key", upper(trim(col("la_name"))))

    joined = epc_norm.join(imd_norm, on="borough_key", how="inner").drop("borough_key", "la_name")
    logger.info("Boroughs matched EPC ∩ IMD: %d / 33", joined.count())

    unmatched = epc_norm.join(imd_norm, on="borough_key", how="left_anti")
    if unmatched.count() > 0:
        logger.warning("Unmatched EPC boroughs: %s", [r["borough"] for r in unmatched.collect()])

    # ── Borough priority ranking ──────────────────────────────────────────────────
    w_epc    = Window.orderBy(desc("pct_below_epc_c"))
    w_income = Window.orderBy(desc("income_deprivation_rate"))
    w_age    = Window.orderBy(desc("pct_pre_1950"))
    w_final  = Window.orderBy("priority_score")

    priority = (
        joined
        .withColumn("rank_epc",    dense_rank().over(w_epc))
        .withColumn("rank_income", dense_rank().over(w_income))
        .withColumn("rank_age",    dense_rank().over(w_age))
        .withColumn("priority_score",
            spark_round((col("rank_epc") + col("rank_income") + col("rank_age")) / 3.0, 1)
        )
        .withColumn("priority_rank", dense_rank().over(w_final))
        .select(
            "priority_rank", "borough", "priority_score",
            "pct_below_epc_c", "income_deprivation_rate", "pct_pre_1950",
            "avg_epc_score", "imd_score", "avg_co2_per_m2", "social_properties",
            "rank_epc", "rank_income", "rank_age",
        )
        .orderBy("priority_rank")
    )

    priority.write.mode("overwrite").parquet(f"{GOLD_PATH}/borough_priority")
    logger.info("Saved borough_priority")

    # ── London-wide affordability trend (CORE) ────────────────────────────────────
    affordability = (
        core
        .filter(col("weekly_rent_est").isNotNull() & col("weekly_income_est").isNotNull())
        .groupBy("year")
        .agg(
            count("*").alias("lettings"),
            spark_round(avg("weekly_rent_est"), 2).alias("avg_weekly_rent_£"),
            spark_round(avg("weekly_income_est"), 2).alias("avg_weekly_income_£"),
            spark_round(avg("rent_to_income_pct"), 1).alias("avg_rent_to_income_%"),
            spark_round(
                spark_sum(when(col("rent_to_income_pct") > 50, 1).otherwise(0)) / count("*") * 100, 1
            ).alias("pct_spending_over_50pct_on_rent"),
            spark_round(
                spark_sum(when(col("overcrowded"), 1).otherwise(0)) / count("*") * 100, 1
            ).alias("pct_overcrowded"),
        )
        .orderBy("year")
    )

    affordability.write.mode("overwrite").parquet(f"{GOLD_PATH}/london_affordability_trend")
    logger.info("Saved london_affordability_trend")

    # ── EPC trend for top 5 priority boroughs ─────────────────────────────────────
    top5 = [row["borough"] for row in priority.limit(5).select("borough").collect()]
    logger.info("Top 5 priority boroughs: %s", top5)

    epc_trend = (
        epc
        .withColumn("inspection_year", spark_year(to_date(col("inspection_date"), "yyyy-MM-dd")))
        .filter(col("inspection_year").between(2012, 2024))
        .filter(col("borough").isin(top5))
        .groupBy("borough", "inspection_year")
        .agg(
            count("*").alias("inspections"),
            spark_round(avg("epc_score"), 1).alias("avg_epc_score"),
            spark_round(
                spark_sum(when(col("below_epc_c"), 1).otherwise(0)) / count("*") * 100, 1
            ).alias("pct_below_epc_c"),
        )
        .orderBy("borough", "inspection_year")
    )

    epc_trend.write.mode("overwrite").parquet(f"{GOLD_PATH}/epc_trend_top_boroughs")
    logger.info("Saved epc_trend_top_boroughs")

    logger.info("Gold layer complete.")


if __name__ == "__main__":
    setup_logging()
    main()
