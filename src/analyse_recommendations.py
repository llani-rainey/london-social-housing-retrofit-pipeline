"""
analyse_recommendations.py — EPC recommendations → Gold cost tables

Joins the EPC improvement recommendations dataset to London social rented certificates
to produce borough-level retrofit cost estimates.

Key engineering decisions:
  - Indicative costs are string ranges ("£800 - £1,200") parsed to low/mid/high bounds
  - Solid wall (improvement ID 7, £8–25k) vs cavity wall (ID 6, ~£1.5k) split is used
    as the retrofit difficulty metric in 07_clustering_analysis.ipynb
  - Three cost scenarios (optimistic/central/pessimistic) reflect RdSAP band uncertainty;
    real London costs are 15–30% higher than these national figures

Gold outputs (data/gold/):
  borough_retrofit_costs      — avg and total indicative cost per borough
  borough_retrofit_scenarios  — low/central/high cost per home and total
  borough_wall_type           — cavity vs solid wall insulation split by borough

Run: python src/analyse_recommendations.py
Requires: JAVA_HOME pointing to JDK 17+, silver EPC already written
"""

import logging

from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    desc,
    udf,
    when,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import FloatType

from config import BRONZE, GOLD, SILVER, setup_logging
from helpers import cost_high, cost_low, cost_midpoint

logger = logging.getLogger(__name__)


def main() -> None:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("recommendations")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    REC_GLOB   = str(BRONZE / "epc_raw" / "recommendations-*.csv")
    EPC_SILVER = str(SILVER / "epc")
    GOLD_PATH  = str(GOLD)

    cost_mid_udf  = udf(cost_midpoint, FloatType())
    cost_low_udf  = udf(cost_low,      FloatType())
    cost_high_udf = udf(cost_high,     FloatType())

    # ── Load and join ─────────────────────────────────────────────────────────────
    certs = (
        spark.read.parquet(EPC_SILVER)
        .select(
            col("certificate_id").alias("certificate_number"),
            col("borough"),
            col("epc_rating"),
            col("epc_score"),
            col("construction_age_band"),
            col("property_type"),
        )
    )

    recs = (
        spark.read.csv(REC_GLOB, header=True, inferSchema=False)
        .select(
            col("certificate_number"),
            col("improvement_item").cast("int").alias("priority"),
            col("improvement_id"),
            col("improvement_summary_text").alias("improvement_type"),
            col("indicative_cost"),
        )
    )

    joined = recs.join(certs, on="certificate_number", how="inner")
    logger.info("Joined rows (recs for London social stock): %s", f"{joined.count():,}")

    joined = (
        joined
        .withColumn("cost_midpoint", cost_mid_udf(col("indicative_cost")))
        .withColumn("cost_low",      cost_low_udf(col("indicative_cost")))
        .withColumn("cost_high",     cost_high_udf(col("indicative_cost")))
    )

    # ── Gold: borough retrofit costs ──────────────────────────────────────────────
    borough_costs = (
        joined
        .groupBy("borough")
        .agg(
            count("certificate_number").alias("total_recommendations"),
            spark_round(avg("cost_midpoint"), 0).alias("avg_cost_per_recommendation_£"),
            spark_round(spark_sum("cost_midpoint") / 1_000_000, 2).alias("total_indicative_cost_£m"),
        )
        .orderBy(desc("total_indicative_cost_£m"))
    )

    borough_costs.write.mode("overwrite").parquet(f"{GOLD_PATH}/borough_retrofit_costs")
    logger.info("Saved borough_retrofit_costs")

    # ── Gold: cost scenarios (optimistic / central / pessimistic) ─────────────────
    borough_scenarios = (
        joined
        .groupBy("borough")
        .agg(
            count("certificate_number").alias("total_recommendations"),
            countDistinct("certificate_number").alias("properties_with_recs"),
            spark_round(spark_sum("cost_low")      / 1_000_000, 2).alias("total_optimistic_£m"),
            spark_round(spark_sum("cost_midpoint") / 1_000_000, 2).alias("total_central_£m"),
            spark_round(spark_sum("cost_high")     / 1_000_000, 2).alias("total_pessimistic_£m"),
            spark_round(spark_sum("cost_low")      / countDistinct("certificate_number"), 0).alias("per_home_optimistic_£"),
            spark_round(spark_sum("cost_midpoint") / countDistinct("certificate_number"), 0).alias("per_home_central_£"),
            spark_round(spark_sum("cost_high")     / countDistinct("certificate_number"), 0).alias("per_home_pessimistic_£"),
            spark_round(
                (spark_sum("cost_high") - spark_sum("cost_low")) / spark_sum("cost_midpoint") * 100, 1
            ).alias("uncertainty_pct"),
        )
        .orderBy(desc("per_home_central_£"))
    )

    borough_scenarios.write.mode("overwrite").parquet(f"{GOLD_PATH}/borough_retrofit_scenarios")
    logger.info("Saved borough_retrofit_scenarios")

    # ── Gold: cavity vs solid wall split ─────────────────────────────────────────
    # Solid wall (ID 7) costs £8–25k vs cavity wall (ID 6) ~£1.5k.
    # Used as retrofit difficulty metric in 07_clustering_analysis.ipynb.
    wall_by_borough = (
        joined
        .groupBy("borough")
        .agg(
            count("certificate_number").alias("total_recs"),
            spark_sum(when(col("improvement_id").cast("int") == 6, 1).otherwise(0)).alias("cavity_wall_recs"),
            spark_sum(when(col("improvement_id").cast("int") == 7, 1).otherwise(0)).alias("solid_wall_recs"),
        )
        .withColumn("total_wall_recs", col("cavity_wall_recs") + col("solid_wall_recs"))
        .withColumn("pct_solid_wall",
            spark_round(col("solid_wall_recs") / col("total_wall_recs") * 100, 1)
        )
        .withColumn("pct_cavity_wall",
            spark_round(col("cavity_wall_recs") / col("total_wall_recs") * 100, 1)
        )
        .filter(col("total_wall_recs") > 0)
        .orderBy(desc("pct_solid_wall"))
    )

    wall_by_borough.write.mode("overwrite").parquet(f"{GOLD_PATH}/borough_wall_type")
    logger.info("Saved borough_wall_type")

    logger.info("Recommendations analysis complete.")


if __name__ == "__main__":
    setup_logging()
    main()
