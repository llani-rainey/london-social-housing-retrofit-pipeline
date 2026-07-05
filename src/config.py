"""Shared path constants and logging setup — resolves from project root regardless of working directory."""
import logging
from pathlib import Path

ROOT    = Path(__file__).resolve().parents[1]
BRONZE  = ROOT / "data" / "bronze"
SILVER  = ROOT / "data" / "silver"
GOLD    = ROOT / "data" / "gold"
IMD_CSV = BRONZE / "imd" / "imd2019_lsoa.csv"


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        format="%(levelname)-8s %(name)s — %(message)s",
        level=level,
    )
