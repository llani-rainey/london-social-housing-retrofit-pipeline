"""
Pure Python tests for helper functions — no Spark required, runs in CI.
Tests: band_midpoint (CORE), cost_midpoint/cost_low/cost_high (recommendations).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from helpers import band_midpoint, cost_high, cost_low, cost_midpoint


class TestBandMidpoint:
    def test_range(self):
        assert band_midpoint("151 to 190") == 170.5

    def test_more_than(self):
        assert band_midpoint("More than 500") == 500.0

    def test_less_than(self):
        assert band_midpoint("Less than 50") == 25.0

    def test_missing(self):
        assert band_midpoint("Missing") is None

    def test_none(self):
        assert band_midpoint(None) is None

    def test_empty(self):
        assert band_midpoint("") is None

    def test_single_number(self):
        assert band_midpoint("300") == 300.0

    def test_comma_number(self):
        assert band_midpoint("1,000 to 2,000") == 1500.0

    def test_refused(self):
        assert band_midpoint("Refused") is None


class TestCostParsers:
    def test_midpoint_range(self):
        assert cost_midpoint("£800 - £1,200") == 1000.0

    def test_midpoint_single(self):
        assert cost_midpoint("£500") == 500.0

    def test_midpoint_none(self):
        assert cost_midpoint(None) is None

    def test_low_range(self):
        assert cost_low("£800 - £1,200") == 800.0

    def test_low_single(self):
        assert cost_low("£500") == 500.0

    def test_high_range(self):
        assert cost_high("£800 - £1,200") == 1200.0

    def test_high_single(self):
        assert cost_high("£500") == 500.0

    def test_high_none(self):
        assert cost_high(None) is None

    def test_large_range(self):
        # Solid wall: £8,000 - £25,000
        assert cost_midpoint("£8,000 - £25,000") == 16500.0
        assert cost_low("£8,000 - £25,000") == 8000.0
        assert cost_high("£8,000 - £25,000") == 25000.0
