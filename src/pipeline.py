"""
pipeline.py — Run the full ingestion and gold build pipeline

Usage:
    python src/pipeline.py        (from repo root)

Stages:
    1. ingest_epc              — EPC certificates CSV → Silver Parquet
    2. ingest_core             — CORE lettings .tab files → Silver Parquet
    3. build_gold              — Silver EPC + IMD CSV → Gold borough tables
    4. analyse_recommendations — EPC recommendations CSV → Gold cost tables

Note: clustering analysis (07_clustering_analysis.ipynb) is notebook-only —
it uses scikit-learn and generates visualisations not suited to a batch script.
Run it after this pipeline completes.

Requires: JAVA_HOME pointing to JDK 17+, raw data in data/bronze/
"""

import logging
import subprocess
import sys
import time
from pathlib import Path

# Ensure `src/` is on sys.path so `from config import ...` works when this
# script is run from anywhere (e.g. `python src/ingest_epc.py`, `python -m src.ingest_epc`).
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

SRC = Path(__file__).resolve().parent

STAGES = [
    ("EPC Ingest",         SRC / "ingest_epc.py"),
    ("CORE Ingest",        SRC / "ingest_core.py"),
    ("Build Gold",         SRC / "build_gold.py"),
    ("Recommendations",    SRC / "analyse_recommendations.py"),
]


def run_stage(name: str, script: Path) -> None:
    logger.info("=" * 60)
    logger.info("STAGE: %s", name)
    logger.info("=" * 60)
    start = time.time()
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(SRC),
    )
    elapsed = time.time() - start
    if result.returncode != 0:
        logger.error("✗ %s FAILED after %.1fs", name, elapsed)
        sys.exit(1)
    logger.info("✓ %s completed in %.1fs", name, elapsed)


if __name__ == "__main__":
    total_start = time.time()
    for name, script in STAGES:
        run_stage(name, script)
    total = time.time() - total_start
    logger.info("Pipeline complete in %.1fs", total)
