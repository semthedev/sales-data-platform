from __future__ import annotations

import csv
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from sales_pipeline.parity import NUMERIC_COLUMNS, compare_marts  # noqa: E402
from sales_pipeline.postgres import TABLES  # noqa: E402
from sales_pipeline.spark_mart import (  # noqa: E402
    MART_SCHEMA,
    build_store_performance_mart,
    read_csv_sources,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "sample_data"

DUCKDB_TABLES = {
    "stores": "plant varchar, store_name varchar",
    "promo_types": "type_id varchar, description varchar",
    "promos": "promo_id varchar, material varchar, type_id varchar, discount decimal(18, 2)",
    "bills_head": "billnum varchar, plant varchar, calday date",
    "bills_item": "billnum varchar, material varchar, rpa_sat decimal(18, 2), qty bigint",
    "traffic": "plant varchar, traffic_date date, quantity bigint",
    "coupons": (
        "coupon_id varchar, plant varchar, billnum varchar, material varchar, "
        "promo_id varchar, coupon_date date"
    ),
}

Rows = dict[str, dict[str, object]]


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-store-performance-mart")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def sources(spark: SparkSession) -> dict[str, DataFrame]:
    return read_csv_sources(spark, DATA_DIR)


def collect_by_plant(frame: DataFrame) -> Rows:
    return {row["plant"]: row.asDict() for row in frame.collect()}


@pytest.fixture(scope="module")
def mart_rows(sources: dict[str, DataFrame]) -> Rows:
    return collect_by_plant(build_store_performance_mart(sources))


def sql_reference_mart() -> list[dict[str, object]]:
    """Run the reference SQL query on the same CSV files with DuckDB."""
    connection = duckdb.connect()
    connection.execute("create schema sales")
    for table in TABLES:
        csv_path = (DATA_DIR / f"{table}.csv").as_posix()
        connection.execute(f"create table sales.{table} ({DUCKDB_TABLES[table]})")
        connection.execute(f"copy sales.{table} from '{csv_path}' (header true)")
    query = (ROOT / "sql" / "marts" / "store_performance.sql").read_text(encoding="utf-8")
    result = connection.execute(query)
    columns = [description[0] for description in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def test_one_row_per_store(mart_rows: Rows, sources: dict[str, DataFrame]) -> None:
    assert len(mart_rows) == 15
    assert set(mart_rows) == {row["plant"] for row in sources["stores"].collect()}


def test_schema_matches_serving_table(sources: dict[str, DataFrame]) -> None:
    mart = build_store_performance_mart(sources)
    actual = [(field.name, field.dataType) for field in mart.schema]
    expected = [(field.name, field.dataType) for field in MART_SCHEMA]
    assert actual == expected


def test_gross_revenue_reconciles_with_line_items(mart_rows: Rows) -> None:
    with (DATA_DIR / "bills_item.csv").open(encoding="utf-8", newline="") as source:
        line_item_revenue = sum(Decimal(row["rpa_sat"]) for row in csv.DictReader(source))

    assert sum(row["gross_revenue"] for row in mart_rows.values()) == line_item_revenue


def test_measures_are_not_negative(mart_rows: Rows) -> None:
    for plant, row in mart_rows.items():
        for column in NUMERIC_COLUMNS:
            assert row[column] >= 0, f"{plant}.{column} is negative"


def test_rates_are_percentages(mart_rows: Rows) -> None:
    for row in mart_rows.values():
        assert 0 <= row["conversion_rate_pct"] <= 100
        assert 0 <= row["promo_rate_pct"] <= 100


def test_net_revenue_is_gross_minus_discount(mart_rows: Rows) -> None:
    for row in mart_rows.values():
        expected = row["gross_revenue"] - row["discount"]
        assert abs(row["net_revenue"] - expected) <= Decimal("0.01")


def test_matches_reference_sql_mart(mart_rows: Rows) -> None:
    differences = compare_marts(sql_reference_mart(), list(mart_rows.values()))
    assert differences == []


def test_store_without_facts_is_preserved(
    spark: SparkSession, sources: dict[str, DataFrame]
) -> None:
    extra_store = spark.createDataFrame(
        [("M999", "Test store without facts")], schema=sources["stores"].schema
    )
    rows = collect_by_plant(
        build_store_performance_mart({**sources, "stores": sources["stores"].union(extra_store)})
    )

    assert len(rows) == 16
    assert all(rows["M999"][column] == 0 for column in NUMERIC_COLUMNS)


def test_duplicate_coupon_rows_are_counted_once(
    sources: dict[str, DataFrame], mart_rows: Rows
) -> None:
    coupons = sources["coupons"]
    duplicated = coupons.union(coupons.limit(1))
    rows = collect_by_plant(build_store_performance_mart({**sources, "coupons": duplicated}))

    for plant, row in rows.items():
        assert row["discount"] == mart_rows[plant]["discount"]
        assert row["promo_sold"] == mart_rows[plant]["promo_sold"]
