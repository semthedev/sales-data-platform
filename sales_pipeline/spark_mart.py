"""PySpark implementation of the store performance mart.

The SQL query in sql/marts/store_performance.sql is the reference. This module rebuilds the same
mart with the DataFrame API, checks it against the SQL mart row by row and writes it to a separate
ClickHouse table over JDBC.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import clickhouse_connect
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_pipeline.config import Settings
from sales_pipeline.parity import check_parity
from sales_pipeline.postgres import TABLES, fetch_mart

AMOUNT = T.DecimalType(18, 2)
RATE = T.DecimalType(8, 2)
# Intermediate precision for divisions before rounding; PostgreSQL numeric is arbitrary precision.
WORK = T.DecimalType(38, 18)

POSTGRES_DRIVER = "org.postgresql.Driver"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.Driver"
CLICKHOUSE_TABLE = "sales.mart_store_performance_spark"

SOURCE_SCHEMAS: dict[str, T.StructType] = {
    "stores": T.StructType(
        [
            T.StructField("plant", T.StringType(), False),
            T.StructField("store_name", T.StringType(), False),
        ]
    ),
    "promo_types": T.StructType(
        [
            T.StructField("type_id", T.StringType(), False),
            T.StructField("description", T.StringType(), False),
        ]
    ),
    "promos": T.StructType(
        [
            T.StructField("promo_id", T.StringType(), False),
            T.StructField("material", T.StringType(), False),
            T.StructField("type_id", T.StringType(), False),
            T.StructField("discount", AMOUNT, False),
        ]
    ),
    "bills_head": T.StructType(
        [
            T.StructField("billnum", T.StringType(), False),
            T.StructField("plant", T.StringType(), False),
            T.StructField("calday", T.DateType(), False),
        ]
    ),
    "bills_item": T.StructType(
        [
            T.StructField("billnum", T.StringType(), False),
            T.StructField("material", T.StringType(), False),
            T.StructField("rpa_sat", AMOUNT, False),
            T.StructField("qty", T.LongType(), False),
        ]
    ),
    "traffic": T.StructType(
        [
            T.StructField("plant", T.StringType(), False),
            T.StructField("traffic_date", T.DateType(), False),
            T.StructField("quantity", T.LongType(), False),
        ]
    ),
    "coupons": T.StructType(
        [
            T.StructField("coupon_id", T.StringType(), False),
            T.StructField("plant", T.StringType(), False),
            T.StructField("billnum", T.StringType(), False),
            T.StructField("material", T.StringType(), False),
            T.StructField("promo_id", T.StringType(), False),
            T.StructField("coupon_date", T.DateType(), False),
        ]
    ),
}

MART_SCHEMA = T.StructType(
    [
        T.StructField("plant", T.StringType(), False),
        T.StructField("store_name", T.StringType(), False),
        T.StructField("gross_revenue", AMOUNT, False),
        T.StructField("discount", AMOUNT, False),
        T.StructField("net_revenue", AMOUNT, False),
        T.StructField("sold_quantity", T.LongType(), False),
        T.StructField("bills_count", T.LongType(), False),
        T.StructField("traffic", T.LongType(), False),
        T.StructField("promo_sold", T.LongType(), False),
        T.StructField("promo_rate_pct", RATE, False),
        T.StructField("avg_items_per_bill", AMOUNT, False),
        T.StructField("conversion_rate_pct", RATE, False),
        T.StructField("avg_bill_amount", AMOUNT, False),
        T.StructField("revenue_per_visitor", AMOUNT, False),
    ]
)


def create_spark_session(
    settings: Settings, app_name: str = "store-performance-mart"
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        # The sample dataset is small; the default 200 shuffle partitions only add overhead.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if settings.spark_jars:
        builder = builder.config("spark.jars", settings.spark_jars)
    elif settings.spark_packages:
        builder = builder.config("spark.jars.packages", settings.spark_packages)
    return builder.getOrCreate()


def read_csv_sources(spark: SparkSession, data_dir: Path) -> dict[str, DataFrame]:
    return {
        table: spark.read.csv(
            str(data_dir / f"{table}.csv"),
            header=True,
            schema=SOURCE_SCHEMAS[table],
            mode="FAILFAST",
        )
        for table in TABLES
    }


def read_postgres_sources(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    """Read the source tables over JDBC; Greenplum speaks the same protocol and driver."""
    reader = (
        spark.read.format("jdbc")
        .option("url", settings.postgres_jdbc_url)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", POSTGRES_DRIVER)
    )
    sources: dict[str, DataFrame] = {}
    for table in TABLES:
        frame = reader.option("dbtable", f"sales.{table}").load()
        schema = SOURCE_SCHEMAS[table]
        sources[table] = frame.select(
            *(F.col(field.name).cast(field.dataType).alias(field.name) for field in schema)
        )
    return sources


def _safe_ratio(numerator: Column, denominator: Column) -> Column:
    """numerator / denominator, or 0 when the denominator is 0, as in the SQL CASE guards."""
    return F.when(denominator == 0, F.lit(0).cast(WORK)).otherwise(
        numerator.cast(WORK) / denominator
    )


def build_store_performance_mart(sources: Mapping[str, DataFrame]) -> DataFrame:
    """Build sales.mart_store_performance from source DataFrames, independent of their origin."""
    stores = sources["stores"].select("plant", "store_name")
    promos = sources["promos"].select("promo_id", "material", "type_id", "discount")
    bills_head = sources["bills_head"].select("billnum", "plant")
    bills_item = sources["bills_item"].select("billnum", "material", "rpa_sat", "qty")
    traffic = sources["traffic"].select("plant", "quantity")
    coupons = sources["coupons"].select("coupon_id", "plant", "billnum", "material", "promo_id")

    gross_sales = (
        bills_head.join(bills_item, "billnum")
        .groupBy("plant")
        .agg(
            F.sum("rpa_sat").alias("gross_revenue"),
            F.sum("qty").alias("sold_quantity"),
            F.countDistinct("billnum").alias("bills_count"),
        )
    )

    # A coupon is counted once, on its first matching line item ordered by billnum, material.
    coupon_order = Window.partitionBy("coupon_id").orderBy("billnum", "material")
    unit_price = F.col("rpa_sat").cast(WORK) / F.nullif(F.col("qty"), F.lit(0))
    line_discount = (
        F.when(F.col("type_id") == "001", F.col("discount").cast(WORK))
        .when(F.col("type_id") == "002", (unit_price * F.col("discount") / 100).cast(WORK))
        .otherwise(F.lit(0).cast(WORK))
    )
    promo_totals = (
        coupons.join(bills_item, ["billnum", "material"])
        .join(promos, ["promo_id", "material"])
        .withColumn("row_number", F.row_number().over(coupon_order))
        .where(F.col("row_number") == 1)
        .groupBy("plant")
        .agg(F.sum(line_discount).alias("discount"))
    )

    traffic_totals = traffic.groupBy("plant").agg(F.sum("quantity").alias("traffic"))
    coupon_totals = coupons.groupBy("plant").agg(F.countDistinct("coupon_id").alias("promo_sold"))

    gross_revenue = F.coalesce(F.col("gross_revenue"), F.lit(0).cast(AMOUNT))
    discount = F.coalesce(F.col("discount"), F.lit(0).cast(WORK))
    sold_quantity = F.coalesce(F.col("sold_quantity"), F.lit(0)).cast(T.LongType())
    bills_count = F.coalesce(F.col("bills_count"), F.lit(0)).cast(T.LongType())
    visitors = F.coalesce(F.col("traffic"), F.lit(0)).cast(T.LongType())
    promo_sold = F.coalesce(F.col("promo_sold"), F.lit(0)).cast(T.LongType())

    return (
        stores.join(gross_sales, "plant", "left")
        .join(promo_totals, "plant", "left")
        .join(traffic_totals, "plant", "left")
        .join(coupon_totals, "plant", "left")
        .select(
            F.col("plant"),
            F.col("store_name"),
            F.round(gross_revenue, 2).cast(AMOUNT).alias("gross_revenue"),
            F.round(discount, 2).cast(AMOUNT).alias("discount"),
            F.round(gross_revenue - discount, 2).cast(AMOUNT).alias("net_revenue"),
            sold_quantity.alias("sold_quantity"),
            bills_count.alias("bills_count"),
            visitors.alias("traffic"),
            promo_sold.alias("promo_sold"),
            F.round(_safe_ratio(promo_sold * 100, sold_quantity), 2)
            .cast(RATE)
            .alias("promo_rate_pct"),
            F.round(_safe_ratio(sold_quantity, bills_count), 2)
            .cast(AMOUNT)
            .alias("avg_items_per_bill"),
            F.round(_safe_ratio(bills_count * 100, visitors), 2)
            .cast(RATE)
            .alias("conversion_rate_pct"),
            F.round(_safe_ratio(gross_revenue, bills_count), 2)
            .cast(AMOUNT)
            .alias("avg_bill_amount"),
            F.round(_safe_ratio(gross_revenue, visitors), 2)
            .cast(AMOUNT)
            .alias("revenue_per_visitor"),
        )
        .orderBy("plant")
    )


def _clickhouse_type(data_type: T.DataType) -> str:
    if isinstance(data_type, T.StringType):
        return "String"
    if isinstance(data_type, T.LongType):
        return "Int64"
    if isinstance(data_type, T.DecimalType):
        return f"Decimal({data_type.precision}, {data_type.scale})"
    raise ValueError(f"no ClickHouse type mapping for {data_type}")


def _prepare_clickhouse_table(settings: Settings) -> None:
    columns = ",\n".join(
        f"    {field.name} {_clickhouse_type(field.dataType)}" for field in MART_SCHEMA
    )
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    try:
        client.command("create database if not exists sales")
        client.command(
            f"create table if not exists {CLICKHOUSE_TABLE}\n(\n{columns}\n)\n"
            "engine = MergeTree\norder by plant"
        )
        client.command(f"truncate table {CLICKHOUSE_TABLE}")
    finally:
        client.close()


def write_to_clickhouse(mart: DataFrame, settings: Settings) -> None:
    """Replace the contents of the Spark serving table; DDL stays explicit, rows go over JDBC."""
    _prepare_clickhouse_table(settings)
    (
        mart.coalesce(1)
        .write.format("jdbc")
        .option("url", settings.clickhouse_jdbc_url)
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", settings.clickhouse_user)
        .option("password", settings.clickhouse_password)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )


def run_spark_stage(settings: Settings) -> int:
    """Build the mart in Spark, verify it against the SQL mart and publish it to ClickHouse."""
    spark = create_spark_session(settings)
    try:
        mart = build_store_performance_mart(read_postgres_sources(spark, settings)).cache()
        spark_rows = [row.asDict() for row in mart.collect()]

        columns, rows = fetch_mart(settings)
        sql_rows = [dict(zip(columns, row, strict=True)) for row in rows]
        check_parity(sql_rows, spark_rows)

        write_to_clickhouse(mart, settings)
        return len(spark_rows)
    finally:
        spark.stop()
