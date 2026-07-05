"""
Spark unit tests for CORE ingest — process_file() function.
Writes temporary .tab files and verifies filtering, schema, and schema-variant handling.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from ingest_core import process_file

_HEADER = "\t".join([
    "GOVREG", "YEAR", "LETTYPE", "TENANCY", "HHMEMBT", "BEDST",
    "BED_MINUS_BEDSTANDARD", "PREVTEN_R", "REASON_R",
    "WEEKINC_T_Bands", "WRENT_Bands", "WTSHORTFALLHB_Bands",
    "ETHNIC_Bands", "TENANCYLENGTH_Bands", "econstat_imputed_R",
])

_LONDON_ROW = "\t".join([
    "E12000007", "2020", "General needs social rented", "Assured", "3", "2",
    "0", "Owner occupier", "Mutual exchange",
    "151 to 190", "51 to 75", "Less than 50",
    "White British", "1 to 2 years", "Full-time employed",
])

_OTHER_REGION_ROW = "\t".join([
    "E12000002", "2020", "General needs social rented", "Assured", "2", "2",
    "0", "Rented", "New letting",
    "100 to 150", "51 to 75", "",
    "Asian or Asian British", "Less than 1 year", "Part-time employed",
])

_OLD_GOVREG_ROW = "\t".join([
    "7", "2010", "General needs", "Assured", "4", "3",
    "1", "Rented", "Transfer",
    "100 to 150", "51 to 75", "0 to 50",
    "White", "1 to 2 years", "Employed",
])


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_core")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.mark.spark
def test_london_row_kept(spark, tmp_path):
    tab = tmp_path / "test.tab"
    tab.write_text("\n".join([_HEADER, _LONDON_ROW, _OTHER_REGION_ROW]))
    result = process_file(spark, str(tab))
    assert result is not None
    assert result.count() == 1


@pytest.mark.spark
def test_old_govreg_code_kept(spark, tmp_path):
    tab = tmp_path / "old.tab"
    tab.write_text("\n".join([_HEADER, _OLD_GOVREG_ROW]))
    result = process_file(spark, str(tab))
    assert result is not None
    assert result.count() == 1


@pytest.mark.spark
def test_no_london_returns_none(spark, tmp_path):
    tab = tmp_path / "nolondon.tab"
    tab.write_text("\n".join([_HEADER, _OTHER_REGION_ROW]))
    result = process_file(spark, str(tab))
    assert result is None


@pytest.mark.spark
def test_output_schema(spark, tmp_path):
    tab = tmp_path / "schema.tab"
    tab.write_text("\n".join([_HEADER, _LONDON_ROW]))
    result = process_file(spark, str(tab))
    required = {
        "year", "region_code_raw", "weekly_income_band",
        "weekly_rent_band", "bedrooms_vs_need",
    }
    assert required.issubset(set(result.columns))


@pytest.mark.spark
def test_alternate_bed_col_name(spark, tmp_path):
    header = "\t".join([
        "GOVREG", "YEAR", "LETTYPE", "TENANCY", "HHMEMBT", "BEDST",
        "BED_MINUS_BEDSTANDARD2",
        "PREVTEN_R", "REASON_R",
        "WEEKINC_T_Bands", "WRENT_Bands", "WTSHORTFALL_Bands",
        "ETHNIC_Bands", "TENANCYLENGTH_Bands", "econstat_imputed_R",
    ])
    row = "\t".join([
        "E12000007", "2015", "General needs", "Assured", "2", "2",
        "1", "Rented", "New",
        "100 to 150", "51 to 75", "0 to 50",
        "White", "1 to 2 years", "Employed",
    ])
    tab = tmp_path / "alt_schema.tab"
    tab.write_text("\n".join([header, row]))
    result = process_file(spark, str(tab))
    assert result is not None
    assert result.count() == 1
