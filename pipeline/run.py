"""DuckDB batch pipeline for NYC yellow taxi trip data."""

from __future__ import annotations

import argparse
import html
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = (
    PROJECT_ROOT / "data" / "sample_yellow_tripdata_2025-01.csv"
)


@dataclass(frozen=True)
class PipelineResult:
    raw_rows: int
    clean_rows: int
    rejected_rows: int
    output_dir: Path
    metrics: dict[str, Any]


def sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def source_relation(path: Path) -> str:
    escaped = sql_path(path)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return f"read_parquet('{escaped}')"
    if suffix in {".csv", ".tsv"}:
        return f"read_csv_auto('{escaped}', header=true)"
    raise ValueError("Source must be a .parquet, .csv or .tsv file")


def export_table(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    output_path: Path,
) -> None:
    connection.execute(
        f"COPY {table} TO '{sql_path(output_path)}' (FORMAT PARQUET)"
    )


def _rows_as_dicts(
    connection: duckdb.DuckDBPyConnection,
    query: str,
) -> list[dict[str, Any]]:
    cursor = connection.execute(query)
    columns = [item[0] for item in cursor.description]
    return [
        dict(zip(columns, row, strict=True))
        for row in cursor.fetchall()
    ]


def execute_pipeline(
    source_path: Path,
    output_dir: Path,
    *,
    month_start: str = "2025-01-01",
    month_end: str = "2025-02-01",
) -> PipelineResult:
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")
    if month_start >= month_end:
        raise ValueError("month_start must be earlier than month_end")

    output_dir.mkdir(parents=True, exist_ok=True)
    table_dir = output_dir / "tables"
    table_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    relation = source_relation(source_path)
    connection.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM {relation}")
    raw_rows = connection.execute("SELECT COUNT(*) FROM raw").fetchone()[0]

    connection.execute(
        f"""
        CREATE TEMP TABLE flagged AS
        SELECT
            *,
            CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
            CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
            date_diff(
                'second',
                CAST(tpep_pickup_datetime AS TIMESTAMP),
                CAST(tpep_dropoff_datetime AS TIMESTAMP)
            ) / 60.0 AS trip_duration_min
        FROM raw
        """
    )

    quality_breakdown = _rows_as_dicts(
        connection,
        f"""
        SELECT
            COUNT(*)::BIGINT AS raw_rows,
            SUM(
                CASE WHEN pickup_ts IS NULL OR dropoff_ts IS NULL
                     THEN 1 ELSE 0 END
            )::BIGINT AS missing_timestamps,
            SUM(
                CASE WHEN pickup_ts < TIMESTAMP '{month_start}'
                           OR pickup_ts >= TIMESTAMP '{month_end}'
                     THEN 1 ELSE 0 END
            )::BIGINT AS out_of_month,
            SUM(
                CASE WHEN dropoff_ts <= pickup_ts
                     THEN 1 ELSE 0 END
            )::BIGINT AS nonpositive_duration,
            SUM(
                CASE WHEN trip_duration_min > 180
                     THEN 1 ELSE 0 END
            )::BIGINT AS excessive_duration,
            SUM(
                CASE WHEN trip_distance IS NULL
                           OR trip_distance <= 0
                           OR trip_distance > 100
                     THEN 1 ELSE 0 END
            )::BIGINT AS invalid_distance,
            SUM(
                CASE WHEN total_amount IS NULL OR total_amount < 0
                     THEN 1 ELSE 0 END
            )::BIGINT AS invalid_total_amount,
            SUM(
                CASE WHEN passenger_count IS NOT NULL
                           AND (passenger_count < 0 OR passenger_count > 6)
                     THEN 1 ELSE 0 END
            )::BIGINT AS passenger_outlier
        FROM flagged
        """,
    )[0]

    connection.execute(
        f"""
        CREATE TEMP TABLE clean AS
        SELECT
            VendorID,
            pickup_ts,
            dropoff_ts,
            passenger_count,
            trip_distance,
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            payment_type,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount,
            trip_duration_min,
            ROUND(
                trip_distance / NULLIF(trip_duration_min / 60.0, 0),
                2
            ) AS average_speed_mph,
            CAST(pickup_ts AS DATE) AS pickup_date,
            EXTRACT(hour FROM pickup_ts)::INTEGER AS pickup_hour,
            dayname(pickup_ts) AS pickup_weekday,
            CASE payment_type
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided trip'
                ELSE 'Other'
            END AS payment_type_name
        FROM flagged
        WHERE pickup_ts >= TIMESTAMP '{month_start}'
          AND pickup_ts < TIMESTAMP '{month_end}'
          AND dropoff_ts > pickup_ts
          AND trip_duration_min <= 180
          AND trip_distance > 0
          AND trip_distance <= 100
          AND total_amount >= 0
          AND (
              passenger_count IS NULL
              OR passenger_count BETWEEN 0 AND 6
          )
        """
    )

    clean_rows = connection.execute("SELECT COUNT(*) FROM clean").fetchone()[0]
    rejected_rows = raw_rows - clean_rows

    connection.execute(
        """
        CREATE TEMP TABLE daily_summary AS
        SELECT
            pickup_date,
            COUNT(*)::BIGINT AS trips,
            SUM(passenger_count)::BIGINT AS passengers,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(fare_amount), 2) AS average_fare,
            ROUND(AVG(trip_distance), 2) AS average_distance_miles,
            ROUND(AVG(trip_duration_min), 2) AS average_duration_min,
            ROUND(AVG(average_speed_mph), 2) AS average_speed_mph
        FROM clean
        GROUP BY pickup_date
        ORDER BY pickup_date
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE hourly_demand AS
        SELECT
            pickup_hour,
            pickup_weekday,
            COUNT(*)::BIGINT AS trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(average_speed_mph), 2) AS average_speed_mph
        FROM clean
        GROUP BY pickup_hour, pickup_weekday
        ORDER BY pickup_hour, pickup_weekday
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE payment_summary AS
        SELECT
            payment_type_name,
            COUNT(*)::BIGINT AS trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS trip_share_pct
        FROM clean
        GROUP BY payment_type_name
        ORDER BY trips DESC
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE pickup_zones AS
        SELECT
            pickup_location_id,
            COUNT(*)::BIGINT AS trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(trip_distance), 2) AS average_distance_miles
        FROM clean
        GROUP BY pickup_location_id
        ORDER BY trips DESC
        """
    )

    for table in [
        "daily_summary",
        "hourly_demand",
        "payment_summary",
        "pickup_zones",
    ]:
        export_table(connection, table, table_dir / f"{table}.parquet")

    daily = _rows_as_dicts(
        connection,
        "SELECT * FROM daily_summary ORDER BY pickup_date",
    )
    payments = _rows_as_dicts(
        connection,
        "SELECT * FROM payment_summary ORDER BY trips DESC",
    )
    zones = _rows_as_dicts(
        connection,
        "SELECT * FROM pickup_zones ORDER BY trips DESC LIMIT 10",
    )
    hourly = _rows_as_dicts(
        connection,
        """
        SELECT pickup_hour, SUM(trips)::BIGINT AS trips
        FROM hourly_demand
        GROUP BY pickup_hour
        ORDER BY trips DESC
        """,
    )

    total_revenue = connection.execute(
        "SELECT ROUND(SUM(total_amount), 2) FROM clean"
    ).fetchone()[0]
    average_fare = connection.execute(
        "SELECT ROUND(AVG(fare_amount), 2) FROM clean"
    ).fetchone()[0]
    average_distance = connection.execute(
        "SELECT ROUND(AVG(trip_distance), 2) FROM clean"
    ).fetchone()[0]
    average_duration = connection.execute(
        "SELECT ROUND(AVG(trip_duration_min), 2) FROM clean"
    ).fetchone()[0]
    passenger_total = connection.execute(
        "SELECT SUM(passenger_count)::BIGINT FROM clean"
    ).fetchone()[0]
    date_min, date_max = connection.execute(
        "SELECT MIN(pickup_ts), MAX(pickup_ts) FROM clean"
    ).fetchone()

    peak_day = daily[0] if len(daily) == 1 else max(
        daily,
        key=lambda row: row["trips"],
    )
    peak_hour = hourly[0] if hourly else {"pickup_hour": None, "trips": 0}
    metrics: dict[str, Any] = {
        "source_file": source_path.name,
        "month_start": month_start,
        "month_end_exclusive": month_end,
        "raw_rows": int(raw_rows),
        "clean_rows": int(clean_rows),
        "rejected_rows": int(rejected_rows),
        "retention_rate_pct": round(
            100.0 * clean_rows / raw_rows,
            2,
        ),
        "total_revenue": float(total_revenue or 0),
        "total_passengers": int(passenger_total or 0),
        "average_fare": float(average_fare or 0),
        "average_distance_miles": float(average_distance or 0),
        "average_duration_min": float(average_duration or 0),
        "peak_day": {
            "date": str(peak_day["pickup_date"]),
            "trips": int(peak_day["trips"]),
        },
        "peak_hour": {
            "hour": int(peak_hour["pickup_hour"]),
            "trips": int(peak_hour["trips"]),
        },
        "top_pickup_zone": {
            "location_id": int(zones[0]["pickup_location_id"]),
            "trips": int(zones[0]["trips"]),
        },
        "pickup_range": {
            "min": str(date_min),
            "max": str(date_max),
        },
        "quality_breakdown": quality_breakdown,
        "generated_at": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ),
    }

    (output_dir / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    render_html_report(
        metrics=metrics,
        daily=daily,
        payments=payments,
        zones=zones,
        output_path=output_dir / "report.html",
    )
    connection.close()
    return PipelineResult(
        raw_rows=int(raw_rows),
        clean_rows=int(clean_rows),
        rejected_rows=int(rejected_rows),
        output_dir=output_dir,
        metrics=metrics,
    )


def _metric_card(label: str, value: str) -> str:
    return (
        '<div class="metric">'
        f"<strong>{html.escape(value)}</strong>"
        f"<span>{html.escape(label)}</span>"
        "</div>"
    )


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    head = "".join(f"<th>{html.escape(item)}</th>" for item in headers)
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(str(value))}</td>" for value in row)
        + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_html_report(
    *,
    metrics: dict[str, Any],
    daily: list[dict[str, Any]],
    payments: list[dict[str, Any]],
    zones: list[dict[str, Any]],
    output_path: Path,
) -> None:
    metrics_html = "".join(
        [
            _metric_card("有效行程", f"{metrics['clean_rows']:,}"),
            _metric_card("总营收", f"${metrics['total_revenue']:,.2f}"),
            _metric_card("平均客单价", f"${metrics['average_fare']:.2f}"),
            _metric_card("数据保留率", f"{metrics['retention_rate_pct']:.2f}%"),
        ]
    )
    daily_table = _table(
        ["日期", "行程数", "营收", "平均距离", "平均时长"],
        [
            [
                row["pickup_date"],
                f"{row['trips']:,}",
                f"${row['total_revenue']:,.2f}",
                row["average_distance_miles"],
                row["average_duration_min"],
            ]
            for row in daily
        ],
    )
    payments_table = _table(
        ["支付方式", "行程数", "营收", "占比"],
        [
            [
                row["payment_type_name"],
                f"{row['trips']:,}",
                f"${row['total_revenue']:,.2f}",
                f"{row['trip_share_pct']:.2f}%",
            ]
            for row in payments
        ],
    )
    zones_table = _table(
        ["上车区域 ID", "行程数", "营收", "平均距离"],
        [
            [
                row["pickup_location_id"],
                f"{row['trips']:,}",
                f"${row['total_revenue']:,.2f}",
                row["average_distance_miles"],
            ]
            for row in zones
        ],
    )
    quality = metrics["quality_breakdown"]
    quality_table = _table(
        ["质量规则", "异常数量"],
        [[key, f"{value:,}"] for key, value in quality.items()],
    )
    output_path.write_text(
        f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NYC Yellow Taxi Data Pipeline</title>
  <style>
    body {{ margin: 0; background: #f6f8fb; color: #13273d;
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif; line-height: 1.6; }}
    main {{ width: min(1100px, calc(100% - 32px)); margin: 0 auto; padding: 48px 0 70px; }}
    h1 {{ margin: 0 0 10px; font-size: clamp(38px, 7vw, 68px); line-height: 1.05; }}
    .lead {{ max-width: 760px; color: #607184; }}
    .links {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 24px 0 30px; }}
    .links a {{ padding: 9px 14px; color: white; background: #13273d;
      border: 1px solid #13273d; border-radius: 9px; text-decoration: none;
      font-weight: 700; font-size: 14px; }}
    .links a.secondary {{ color: #13273d; background: white; border-color: #dce5ed; }}
    .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin: 30px 0; }}
    .metric, section {{ background: white; border: 1px solid #dce5ed; border-radius: 16px; }}
    .metric {{ padding: 20px; }}
    .metric strong {{ display: block; font-size: 28px; }}
    .metric span {{ color: #607184; font-size: 14px; }}
    section {{ padding: 24px; margin-top: 18px; overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #e5ebf1; text-align: left; }}
    th {{ background: #edf4fb; }}
    @media (max-width: 760px) {{ .metrics {{ grid-template-columns: 1fr 1fr; }} }}
  </style>
</head>
<body>
<main>
  <h1>NYC Yellow Taxi<br>Batch Data Pipeline</h1>
  <p class="lead">
    使用 DuckDB 对纽约市黄色出租车公开 Parquet 数据执行数据质量检查、清洗、
    日期与小时维度聚合、支付方式分析和热门上车区域分析。
  </p>
  <div class="links">
    <a href="https://github.com/ri-ce/nyc-taxi-data-pipeline">GitHub 源码</a>
    <a class="secondary" href="test-report.html">自动化测试报告</a>
    <a class="secondary" href="https://ri-ce.github.io/data-ai-portfolio/">返回作品集</a>
  </div>
  <div class="metrics">{metrics_html}</div>
  <section>
    <h2>关键结果</h2>
    <p>高峰日：{html.escape(str(metrics['peak_day']))}</p>
    <p>高峰小时：{html.escape(str(metrics['peak_hour']))}</p>
    <p>热门上车区域：{html.escape(str(metrics['top_pickup_zone']))}</p>
  </section>
  <section><h2>每日指标</h2>{daily_table}</section>
  <section><h2>支付方式</h2>{payments_table}</section>
  <section><h2>热门上车区域</h2>{zones_table}</section>
  <section><h2>数据质量检查</h2>{quality_table}</section>
  <p>生成时间：{html.escape(str(metrics['generated_at']))}</p>
</main>
</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run NYC taxi pipeline")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports",
    )
    parser.add_argument("--month-start", default="2025-01-01")
    parser.add_argument("--month-end", default="2025-02-01")
    args = parser.parse_args()
    result = execute_pipeline(
        args.source,
        args.output_dir,
        month_start=args.month_start,
        month_end=args.month_end,
    )
    print(json.dumps(result.metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
