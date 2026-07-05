"""
Pure Python helper functions shared by ingest_core.py and analyse_recommendations.py.
No PySpark imports — safe to import in tests without triggering a Spark session.
"""

import re
from collections.abc import Iterable


def validate_schema(actual_columns: Iterable[str], required: set[str], name: str = "") -> None:
    missing = required - set(actual_columns)
    if missing:
        label = f"[{name}] " if name else ""
        raise ValueError(f"{label}missing required columns: {sorted(missing)}")


def band_midpoint(s):
    """Convert CORE band strings to numeric midpoints.

    Examples:
        '151 to 190'    → 170.5
        'More than 500' → 500.0
        'Less than 50'  → 25.0
        'Missing'       → None
    """
    if s is None:
        return None
    s = s.strip()
    if s in ("", "Missing", "MISSING", "R", "NULL", "N/A", "No", "Yes", "Refused"):
        return None
    if "More than" in s or "more than" in s:
        nums = re.findall(r"[\d,]+", s)
        return float(nums[0].replace(",", "")) if nums else None
    if "Less than" in s or "less than" in s:
        nums = re.findall(r"[\d,]+", s)
        return float(nums[0].replace(",", "")) / 2 if nums else None
    if " to " in s:
        parts = s.split(" to ")
        try:
            return (float(parts[0].strip().replace(",", "")) + float(parts[1].strip().replace(",", ""))) / 2
        except (ValueError, IndexError):
            return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _parse_nums(s):
    if s is None:
        return []
    return [float(n.replace(",", "")) for n in re.findall(r"[\d,]+", s)]


def cost_midpoint(s):
    nums = _parse_nums(s)
    if len(nums) == 2:
        return (nums[0] + nums[1]) / 2
    if len(nums) == 1:
        return nums[0]
    return None


def cost_low(s):
    nums = _parse_nums(s)
    return nums[0] if nums else None


def cost_high(s):
    nums = _parse_nums(s)
    if len(nums) == 2:
        return nums[1]
    if len(nums) == 1:
        return nums[0]
    return None
