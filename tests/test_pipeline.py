from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from pipeline.run import execute_pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = PROJECT_ROOT / "data" / "sample_yellow_tripdata_2025-01.csv"


@pytest.fixture(scope="session")
def pipeline_output(tmp_path_factory):
    output_dir = tmp_path_factory.mktemp("pipeline")
    result = execute_pipeline(SAMPLE_PATH, output_dir)
    return result, output_dir


def test_pipeline_creates_expected_outputs(pipeline_output):
    _, output_dir = pipeline_output

    assert (output_dir / "metrics.json").exists()
    assert (output_dir / "report.html").exists()
    for table in [
        "daily_summary",
        "hourly_demand",
        "payment_summary",
        "pickup_zones",
    ]:
        assert (output_dir / "tables" / f"{table}.parquet").exists()


def test_raw_row_count_matches_sample(pipeline_output):
    result, _ = pipeline_output
    assert result.raw_rows == 1_000
    assert result.clean_rows > 0
    assert result.rejected_rows == result.raw_rows - result.clean_rows


def test_quality_breakdown_totals_raw_rows(pipeline_output):
    result, _ = pipeline_output
    quality = result.metrics["quality_breakdown"]
    assert quality["raw_rows"] == result.raw_rows


def test_daily_summary_matches_clean_rows(pipeline_output):
    result, output_dir = pipeline_output
    total = duckdb.sql(
        "SELECT SUM(trips) FROM read_parquet(?)",
        params=[str(output_dir / "tables" / "daily_summary.parquet")],
    ).fetchone()[0]
    assert total == result.clean_rows


def test_cleaning_rules_identify_impossible_trips(pipeline_output):
    result, _ = pipeline_output
    impossible_source_rows = duckdb.sql(
        """
        SELECT COUNT(*)
        FROM read_csv_auto(?, header=true)
        WHERE trip_distance <= 0
           OR date_diff(
               'second',
               CAST(tpep_pickup_datetime AS TIMESTAMP),
               CAST(tpep_dropoff_datetime AS TIMESTAMP)
           ) <= 0
        """,
        params=[str(SAMPLE_PATH)],
    ).fetchone()[0]
    quality = result.metrics["quality_breakdown"]
    assert impossible_source_rows >= 0
    assert quality["nonpositive_duration"] >= 0
    assert quality["invalid_distance"] >= 0


def test_metrics_contains_required_sections(pipeline_output):
    result, _ = pipeline_output
    metrics = result.metrics

    assert metrics["clean_rows"] > 0
    assert metrics["total_revenue"] >= 0
    assert metrics["peak_hour"]["hour"] in range(24)
    assert metrics["top_pickup_zone"]["trips"] > 0


def test_metrics_json_matches_result(pipeline_output):
    result, output_dir = pipeline_output
    saved = json.loads(
        (output_dir / "metrics.json").read_text(encoding="utf-8")
    )
    assert saved["clean_rows"] == result.clean_rows
    assert saved["raw_rows"] == 1_000
