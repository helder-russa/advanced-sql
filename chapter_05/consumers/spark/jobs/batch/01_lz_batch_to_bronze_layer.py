import argparse
import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import (
    col, lit, input_file_name, regexp_extract,
    to_timestamp, coalesce, when
)


# -----------------------------
# Spark
# -----------------------------
def build_spark(app_name: str, log_level: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark


# -----------------------------
# Schemas (aligned to your batch JSON examples)
# -----------------------------
def schema_customers() -> StructType:
    return StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("_ingest_time_utc", StringType(), True),
        StructField("_ingest_type", StringType(), True),
        StructField("_entity", StringType(), True),
    ])


def schema_products() -> StructType:
    return StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("_ingest_time_utc", StringType(), True),
        StructField("_ingest_type", StringType(), True),
        StructField("_entity", StringType(), True),
    ])


def schema_orders() -> StructType:
    return StructType([
        StructField("id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("created_at", StringType(), True),  # "2026-01-06 11:26:26.043133"
        StructField("status", StringType(), True),      # if in the future we have different statuses
        StructField("source", StringType(), True),      # if in the future we have multiple sources
        StructField("_pubsub_message_id", StringType(), True),
        StructField("_pubsub_publish_time", StringType(), True),
        StructField("_ingest_time_utc", StringType(), True),
        StructField("_ingest_type", StringType(), True),
        StructField("_entity", StringType(), True),
    ])


def get_schema(entity: str) -> StructType:
    if entity == "customers":
        return schema_customers()
    if entity == "products":
        return schema_products()
    if entity == "orders":
        return schema_orders()
    raise ValueError(f"Unsupported ENTITY={entity}")


# -----------------------------
# Read LZ (NDJSON) + add snapshot partitions from path
# -----------------------------
def read_landing_zone(spark: SparkSession, entity: str, input_path: str) -> DataFrame:
    schema = get_schema(entity)

    df = (
        spark.read
        .schema(schema)
        .json(input_path)  # NDJSON is supported by spark.read.json
        .withColumn("_input_file", input_file_name())
    )

    # Extract snapshot_dt / snapshot_ts from path like:
    # .../snapshot_dt=YYYY-MM-DD/snapshot_ts=YYYYMMDDTHHMMSSZ/...
    df = (
        df.withColumn("snapshot_dt", regexp_extract(col("_input_file"), r"snapshot_dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1))
          .withColumn("snapshot_ts", regexp_extract(col("_input_file"), r"snapshot_ts=([0-9]{8}T[0-9]{6}Z)", 1))
    )

    return df


# -----------------------------
# Normalize + project columns (must match target table)
# -----------------------------
def normalize_customers(df: DataFrame) -> DataFrame:
    return df.select(
        col("id").cast("long").alias("id"),
        col("name").cast("string").alias("name"),
        col("email").cast("string").alias("email"),
        col("country").cast("string").alias("country"),
        to_timestamp(col("_ingest_time_utc")).alias("ingest_ts"),
        col("snapshot_dt").cast("string").alias("snapshot_dt"),
        col("snapshot_ts").cast("string").alias("snapshot_ts"),
        col("_input_file").cast("string").alias("input_file"),
        col("_ingest_type").cast("string").alias("ingest_type"),
        col("_entity").cast("string").alias("entity"),
    )


def normalize_products(df: DataFrame) -> DataFrame:
    return df.select(
        col("id").cast("long").alias("id"),
        col("name").cast("string").alias("name"),
        col("category").cast("string").alias("category"),
        col("price").cast("double").alias("price"),
        to_timestamp(col("_ingest_time_utc")).alias("ingest_ts"),
        col("snapshot_dt").cast("string").alias("snapshot_dt"),
        col("snapshot_ts").cast("string").alias("snapshot_ts"),
        col("_input_file").cast("string").alias("input_file"),
        col("_ingest_type").cast("string").alias("ingest_type"),
        col("_entity").cast("string").alias("entity"),
    )


def normalize_orders(df: DataFrame) -> DataFrame:
    # created_at is like "2026-01-06 11:26:26.043133"
    created_at_ts = to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS")

    # derive unit_price = total_amount / quantity when possible
    unit_price = when(
        (col("quantity").isNotNull()) & (col("quantity") != 0) & col("total_amount").isNotNull(),
        col("total_amount") / col("quantity")
    ).otherwise(lit(None).cast("double"))

    return df.select(
        col("id").cast("long").alias("id"),
        col("customer_id").cast("long").alias("customer_id"),
        col("product_id").cast("long").alias("product_id"),
        col("quantity").cast("int").alias("quantity"),
        unit_price.alias("unit_price"),
        col("total_amount").cast("double").alias("total_amount"),
        col("currency").cast("string").alias("currency"),
        created_at_ts.alias("created_at"),
        coalesce(col("status"), lit("created")).cast("string").alias("status"),
        coalesce(col("source"), lit("batch_snapshot")).cast("string").alias("source"),
        col("_pubsub_message_id").cast("string").alias("pubsub_message_id"),
        col("_pubsub_publish_time").cast("string").alias("pubsub_publish_time"),
        to_timestamp(col("_ingest_time_utc")).alias("ingest_ts"),
        col("snapshot_dt").cast("string").alias("snapshot_dt"),
        col("snapshot_ts").cast("string").alias("snapshot_ts"),
        col("_input_file").cast("string").alias("input_file"),
        col("_ingest_type").cast("string").alias("ingest_type"),
        col("_entity").cast("string").alias("entity"),
    )


def normalize(entity: str, df: DataFrame) -> DataFrame:
    if entity == "customers":
        return normalize_customers(df)
    if entity == "products":
        return normalize_products(df)
    if entity == "orders":
        return normalize_orders(df)
    raise ValueError(f"Unsupported ENTITY={entity}")


# -----------------------------
# Iceberg table DDL (Bronze)
# -----------------------------
def create_table_sql(catalog: str, db: str, table: str, entity: str) -> str:
    fqn = f"{catalog}.{db}.{table}"

    # Partition by snapshot to make batch snapshots naturally idempotent
    if entity == "customers":
        return f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
          id BIGINT,
          name STRING,
          email STRING,
          country STRING,
          ingest_ts TIMESTAMP,
          snapshot_dt STRING,
          snapshot_ts STRING,
          input_file STRING,
          ingest_type STRING,
          entity STRING
        )
        USING iceberg
        PARTITIONED BY (snapshot_dt, snapshot_ts)
        """
    if entity == "products":
        return f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
          id BIGINT,
          name STRING,
          category STRING,
          price DOUBLE,
          ingest_ts TIMESTAMP,
          snapshot_dt STRING,
          snapshot_ts STRING,
          input_file STRING,
          ingest_type STRING,
          entity STRING
        )
        USING iceberg
        PARTITIONED BY (snapshot_dt, snapshot_ts)
        """
    if entity == "orders":
        return f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
          id BIGINT,
          customer_id BIGINT,
          product_id BIGINT,
          quantity INT,
          unit_price DOUBLE,
          total_amount DOUBLE,
          currency STRING,
          created_at TIMESTAMP,
          status STRING,
          source STRING,
          pubsub_message_id STRING,
          pubsub_publish_time STRING,
          ingest_ts TIMESTAMP,
          snapshot_dt STRING,
          snapshot_ts STRING,
          input_file STRING,
          ingest_type STRING,
          entity STRING
        )
        USING iceberg
        PARTITIONED BY (snapshot_dt, snapshot_ts)
        """
    raise ValueError(f"Unsupported ENTITY={entity}")


def ensure_namespace(spark: SparkSession, catalog: str, db: str):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{db}")


def drop_table_if_requested(spark: SparkSession, fqn: str, reset: bool):
    if reset:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")


# -----------------------------
# Write (idempotent for same snapshot partitions)
# -----------------------------
def write_bronze_overwrite_partitions(df: DataFrame, fqn: str):
    # overwrites only the partitions present in df (snapshot_dt, snapshot_ts)
    df.writeTo(fqn).overwritePartitions()


# -----------------------------
# Args
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("--ENTITY", required=True, choices=["customers", "products", "orders"])
    p.add_argument("--INPUT_PATH", required=True)
    p.add_argument("--CATALOG_NAME", default="local")
    p.add_argument("--DB_NAME", default="bronze")
    p.add_argument("--TABLE_NAME", required=True)
    p.add_argument("--SPARK_LOG_LEVEL", default="WARN")

    # Demo-friendly default: drop table to avoid schema mismatch from earlier runs
    p.add_argument("--RESET_TABLE", default="true")  # "true"/"false"

    # Optional: you can force filtering to one snapshot date if you want
    p.add_argument("--SNAPSHOT_DATE", default="")  # e.g. "2026-01-06"

    return p.parse_args()


# -----------------------------
# Main
# -----------------------------
def main():
    args = parse_args()

    entity = args.ENTITY
    input_path = args.INPUT_PATH
    catalog = args.CATALOG_NAME
    db = args.DB_NAME
    table = args.TABLE_NAME
    log_level = args.SPARK_LOG_LEVEL
    reset_table = str(args.RESET_TABLE).lower() == "true"
    snapshot_date_filter = (args.SNAPSHOT_DATE or "").strip()

    spark = build_spark(app_name=f"ch05_batch_lz_to_bronze_{entity}", log_level=log_level)

    # Read
    raw = read_landing_zone(spark, entity, input_path)

    # Optional filter by snapshot_dt (useful in demos)
    if snapshot_date_filter:
        raw = raw.filter(col("snapshot_dt") == lit(snapshot_date_filter))

    # Normalize + ensure we have snapshot partition values
    bronze = normalize(entity, raw)

    # Safety: if snapshot columns are missing, fail fast with clear message
    missing_snapshot = bronze.filter((col("snapshot_dt") == "") | (col("snapshot_ts") == "")).limit(1).count() > 0
    if missing_snapshot:
        raise ValueError(
            "snapshot_dt/snapshot_ts are empty. Ensure INPUT_PATH includes folders snapshot_dt=YYYY-MM-DD/snapshot_ts=YYYYMMDDTHHMMSSZ."
        )

    fqn = f"{catalog}.{db}.{table}"

    # Create table
    ensure_namespace(spark, catalog, db)
    drop_table_if_requested(spark, fqn, reset_table)
    spark.sql(create_table_sql(catalog, db, table, entity))

    # Write
    print(f"[batch->bronze] writing entity={entity} to {fqn} (overwritePartitions on snapshot_dt/snapshot_ts)")
    write_bronze_overwrite_partitions(bronze, fqn)

    # Basic counts
    cnt = bronze.count()
    print(f"[batch->bronze] wrote rows={cnt} to {fqn}")

    spark.stop()


if __name__ == "__main__":
    main()