"""
prefect_flow.py — Prefect orchestration for the London housing pipeline

Wraps the four PySpark ingestion stages and the dbt transformation layer
into a single observable flow. Each stage runs as a Prefect task with
automatic retry logic and structured logging.

Usage:
    python src/prefect_flow.py            # run once locally
    prefect server start                  # start local UI (http://127.0.0.1:4200)
    prefect deploy src/prefect_flow.py    # deploy to Prefect Cloud

Requires: pip install prefect
"""

import subprocess
import sys
from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger

SRC = Path(__file__).resolve().parent
DBT = SRC.parent / "dbt"


def _run(script: Path, cwd: Path = SRC) -> None:
    logger = get_run_logger()
    logger.info(f"Running {script.name}")
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(cwd),
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{script.name} exited with code {result.returncode}")


def _dbt(command: list[str]) -> None:
    logger = get_run_logger()
    logger.info(f"dbt {' '.join(command)}")
    result = subprocess.run(
        ["dbt", *command, "--profiles-dir", "."],
        cwd=str(DBT),
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command[0]} failed")


@task(name="EPC Ingest", retries=1)
def ingest_epc() -> None:
    _run(SRC / "ingest_epc.py")


@task(name="CORE Ingest", retries=1)
def ingest_core() -> None:
    _run(SRC / "ingest_core.py")


@task(name="Build Gold (PySpark)", retries=1)
def build_gold() -> None:
    _run(SRC / "build_gold.py")


@task(name="Recommendations Analysis", retries=1)
def analyse_recommendations() -> None:
    _run(SRC / "analyse_recommendations.py")


@task(name="dbt Run", retries=1)
def dbt_run() -> None:
    _dbt(["run"])


@task(name="dbt Test")
def dbt_test() -> None:
    _dbt(["test"])


@flow(name="London Housing Pipeline", log_prints=True)
def london_housing_pipeline(skip_ingest: bool = False) -> None:
    """
    Full pipeline: PySpark ingestion → gold build → dbt transformations → dbt tests.

    Args:
        skip_ingest: set True to skip PySpark stages (e.g. silver already exists)
    """
    if not skip_ingest:
        ingest_epc()
        ingest_core()
        build_gold()
        analyse_recommendations()

    dbt_run()
    dbt_test()


if __name__ == "__main__":
    london_housing_pipeline()
