"""
Spark unit tests for EPC silver transformation.
Tests the transform_epc() function using synthetic DataFrames — no data files required.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from ingest_epc import transform_epc

_COLS = [
    "certificate_number", "postcode", "local_authority", "local_authority_label",
    "tenure", "property_type", "built_form", "construction_age_band",
    "current_energy_rating", "current_energy_efficiency", "total_floor_area",
    "main_fuel", "co2_emissions_current", "energy_consumption_current",
    "heating_cost_current", "walls_energy_eff", "roof_energy_eff",
    "windows_energy_eff", "mains_gas_flag", "inspection_date", "region",
]


def _row(**overrides):
    defaults = dict(
        certificate_number="cert001",
        postcode="SW1A 1AA",
        local_authority="E09000022",
        local_authority_label="Lambeth",
        tenure="rental (social)",
        property_type="Flat",
        built_form="Mid-Terrace",
        construction_age_band="England and Wales: 1900-1929",
        current_energy_rating="D",
        current_energy_efficiency="60",
        total_floor_area="65.0",
        main_fuel="mains gas (not community)",
        co2_emissions_current="3.5",
        energy_consumption_current="150.0",
        heating_cost_current="800.0",
        walls_energy_eff="Poor",
        roof_energy_eff="Average",
        windows_energy_eff="Average",
        mains_gas_flag="Y",
        inspection_date="2020-06-15",
        region="E12000007",
    )
    defaults.update(overrides)
    return tuple(defaults[c] for c in _COLS)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_epc")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.mark.spark
def test_output_schema(spark):
    raw = spark.createDataFrame([_row()], _COLS)
    silver = transform_epc(raw)
    required = {
        "certificate_id", "borough", "epc_rating", "epc_score",
        "below_epc_c", "construction_age_band", "inspection_date",
        "fuel_category", "co2_emissions", "region_code",
    }
    assert required.issubset(set(silver.columns))


@pytest.mark.spark
def test_social_tenure_kept(spark):
    rows = [
        _row(certificate_number="s1", tenure="rental (social)"),
        _row(certificate_number="s2", tenure="SOCIAL RENTED"),
        _row(certificate_number="s3", tenure="Rented (Social)"),
    ]
    silver = transform_epc(spark.createDataFrame(rows, _COLS))
    assert silver.count() == 3


@pytest.mark.spark
def test_non_social_tenure_filtered(spark):
    rows = [
        _row(certificate_number="keep", tenure="rental (social)"),
        _row(certificate_number="drop", tenure="owner-occupied"),
    ]
    silver = transform_epc(spark.createDataFrame(rows, _COLS))
    assert silver.count() == 1
    assert silver.first()["certificate_id"] == "keep"


@pytest.mark.spark
def test_london_region_filter(spark):
    rows = [
        _row(certificate_number="lon", region="E12000007"),
        _row(certificate_number="mnc", region="E12000002"),
    ]
    silver = transform_epc(spark.createDataFrame(rows, _COLS))
    assert silver.count() == 1
    assert silver.first()["certificate_id"] == "lon"


@pytest.mark.spark
def test_null_rating_filtered(spark):
    rows = [
        _row(certificate_number="ok",   current_energy_rating="D"),
        _row(certificate_number="null", current_energy_rating=None),
    ]
    silver = transform_epc(spark.createDataFrame(rows, _COLS))
    assert silver.count() == 1


@pytest.mark.spark
def test_below_epc_c_flag(spark):
    rows = [
        _row(certificate_number="a", current_energy_rating="A"),
        _row(certificate_number="b", current_energy_rating="B"),
        _row(certificate_number="c", current_energy_rating="C"),
        _row(certificate_number="d", current_energy_rating="D"),
        _row(certificate_number="g", current_energy_rating="G"),
    ]
    silver = transform_epc(spark.createDataFrame(rows, _COLS))
    flags = {r["certificate_id"]: r["below_epc_c"] for r in silver.collect()}
    assert flags["a"] is False
    assert flags["c"] is False
    assert flags["d"] is True
    assert flags["g"] is True


@pytest.mark.spark
def test_fuel_category_mapping(spark):
    rows = [
        _row(certificate_number="gi", main_fuel="mains gas (not community)"),
        _row(certificate_number="gc", main_fuel="mains gas (community)"),
        _row(certificate_number="ei", main_fuel="electricity (not community)"),
        _row(certificate_number="ec", main_fuel="electricity (community)"),
        _row(certificate_number="ot", main_fuel="oil"),
        _row(certificate_number="nu", main_fuel=None),
    ]
    silver = transform_epc(spark.createDataFrame(rows, _COLS))
    cats = {r["certificate_id"]: r["fuel_category"] for r in silver.collect()}
    assert cats["gi"] == "gas_individual"
    assert cats["gc"] == "gas_community"
    assert cats["ei"] == "electric_individual"
    assert cats["ec"] == "electric_community"
    assert cats["ot"] == "other"
    assert cats["nu"] is None
