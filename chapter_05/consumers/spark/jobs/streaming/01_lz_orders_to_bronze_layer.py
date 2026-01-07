import os
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, DoubleType, StringType
)
from pyspark.sql.functions import (
    col, lit, input_file_name, regexp_extract,
    to_timestamp, current_timestamp, to_date,
    when, date_format
)


# ----------------------------
# Schema (Landing Zone JSON)
# ----------------------------
def landing_zone_schema() -> StructType:
    # Matches your actual payload (example you shared)
    # {"id":19,"customer_id":2,"product_id":5,"quantity":1,"total_amount":94.18,"currency":"EUR",
    #  "created_at":"2026-01-06 11:26:26.043133",
    #  "_pubsub_message_id":"...","_pubsub_publish_time":"...","_ingest_time_utc":"..."}
    return StructType([
        StructField("id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("created_at", StringType(), True),

        # metadata you append upstream (may be absent depending on producer)
        StructField("_pubsub_message_id", StringType(), True),
        StructField("_pubsub_publish_time", StringType(), True),
        StructField("_ingest_time_utc", StringType(), True),
    ])


def build_spark(app_name: str, log_level: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


def ensure_iceberg_objects(
    spark: SparkSession,
    catalog: str,
    db: str,
    table: str
) -> None:
    # Database (namespace)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")

    # Table: keep a stable set of columns.
    # Partitioning: partition by ingest_date (day) is simple + works well for demos.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{db}.{table} (
            id BIGINT,
            customer_id BIGINT,
            product_id BIGINT,
            quantity INT,
            total_amount DOUBLE,
            unit_price DOUBLE,
            currency STRING,
            status STRING,
            source STRING,
            created_at TIMESTAMP,
            pubsub_message_id STRING,
            pubsub_publish_time TIMESTAMP,
            ingest_ts TIMESTAMP,
            ingest_date DATE,
            ingest_dt STRING,
            ingest_hr STRING,
            input_file STRING
        )
        USING iceberg
        PARTITIONED BY (ingest_date)
    """)


def normalize(df: DataFrame) -> DataFrame:
    # Track source file
    df = df.withColumn("input_file", input_file_name())

    # Partition fields from path (if you partition like ingest_dt=YYYY-MM-DD/ingest_hr=HH)
    df = (
        df
        .withColumn("ingest_dt", regexp_extract(col("input_file"), r"ingest_dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1))
        .withColumn("ingest_hr", regexp_extract(col("input_file"), r"ingest_hr=([0-9]{2})", 1))
    )

    # Parse timestamps
    # created_at example: "2026-01-06 11:26:26.043133"
    df = df.withColumn("created_at_ts", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))

    # Pub/Sub publish time example: "2026-01-06T11:26:26.152000+00:00"
    df = df.withColumn("pubsub_publish_time_ts", to_timestamp(col("_pubsub_publish_time")))

    # Ingest time example: "2026-01-06T11:38:55.439592+00:00"
    df = df.withColumn("ingest_time_utc_ts", to_timestamp(col("_ingest_time_utc")))

    # Canonical ingest timestamp
    df = df.withColumn("ingest_ts", current_timestamp())

    # If ingest_dt not present in path, derive it from ingest_ts for partition convenience
    df = df.withColumn(
        "ingest_dt",
        when(col("ingest_dt") != "", col("ingest_dt")).otherwise(date_format(col("ingest_ts"), "yyyy-MM-dd"))
    )
    df = df.withColumn(
        "ingest_hr",
        when(col("ingest_hr") != "", col("ingest_hr")).otherwise(date_format(col("ingest_ts"), "HH"))
    )

    df = df.withColumn("ingest_date", to_date(col("ingest_ts")))

    # Defaults / derived fields
    df = df.withColumn("status", lit("unknown"))
    df = df.withColumn("source", lit("pubsub"))

    df = df.withColumn(
        "unit_price",
        when((col("quantity").isNotNull()) & (col("quantity") != 0) & col("total_amount").isNotNull(),
             col("total_amount") / col("quantity")
        ).otherwise(lit(None).cast("double"))
    )

    # Rename metadata fields to stable names
    df = df.withColumnRenamed("_pubsub_message_id", "pubsub_message_id")

    return df


def split_valid_invalid(df: DataFrame) -> (DataFrame, DataFrame):
    # Minimal validity for this pipeline
    valid_cond = (
        col("id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("total_amount").isNotNull()
        & col("currency").isNotNull()
        & (col("currency") != "")
        & col("created_at_ts").isNotNull()
    )

    valid = df.where(valid_cond)
    invalid = df.where(~valid_cond)

    return valid, invalid


def write_batch_factory(
    quarantine_path: str,
    catalog: str,
    db: str,
    table: str
):
    full_table = f"{catalog}.{db}.{table}"

    target_cols = [
        "id",
        "customer_id",
        "product_id",
        "quantity",
        "total_amount",
        "unit_price",
        "currency",
        "status",
        "source",
        "created_at_ts",
        "pubsub_message_id",
        "pubsub_publish_time_ts",
        "ingest_ts",
        "ingest_date",
        "ingest_dt",
        "ingest_hr",
        "input_file",
    ]

    def write_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            print(f"[Batch {batch_id}] Empty batch, skipping.")
            return

        batch_df = normalize(batch_df)
        valid_df, invalid_df = split_valid_invalid(batch_df)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        print(f"[Batch {batch_id}] Valid: {valid_count}, Invalid: {invalid_count}")

        # Quarantine invalid rows (append-only, partitioned by ingest_dt/hr via path already extracted)
        if invalid_count > 0:
            (
                invalid_df
                .withColumn("quarantine_reason", lit("failed_minimal_validation"))
                .write.mode("append")
                .json(quarantine_path)
            )

        if valid_count == 0:
            return

        # Write only the columns the Iceberg table expects (prevents column arity mismatch)
        to_write = (
            valid_df
            .select(*target_cols)
            .withColumnRenamed("created_at_ts", "created_at")
            .withColumnRenamed("pubsub_publish_time_ts", "pubsub_publish_time")
        )

        (
            to_write.writeTo(full_table)
            .append()
        )

    return write_batch


def parse_args():
    p = argparse.ArgumentParser(add_help=True)

    # Use args but also allow env fallback (your gcloud command uses --KEY=VALUE style)
    p.add_argument("--GCS_BUCKET", default=os.getenv("GCS_BUCKET", "advance-sql-de-bucket"))
    p.add_argument("--INPUT_PATH", default=os.getenv("INPUT_PATH"))
    p.add_argument("--CHECKPOINT_PATH", default=os.getenv("CHECKPOINT_PATH"))
    p.add_argument("--QUARANTINE_PATH", default=os.getenv("QUARANTINE_PATH"))

    p.add_argument("--CATALOG_NAME", default=os.getenv("CATALOG_NAME", "local"))
    p.add_argument("--DB_NAME", default=os.getenv("DB_NAME", "bronze"))
    p.add_argument("--TABLE_NAME", default=os.getenv("TABLE_NAME", "live_orders"))

    p.add_argument("--MAX_FILES_PER_TRIGGER", default=os.getenv("MAX_FILES_PER_TRIGGER", "50"))
    p.add_argument("--TRIGGER_SECONDS", default=os.getenv("TRIGGER_SECONDS", "15"))
    p.add_argument("--SPARK_LOG_LEVEL", default=os.getenv("SPARK_LOG_LEVEL", "WARN"))

    # IMPORTANT: tolerate unknown args so Dataproc tooling won't break this script
    args, _ = p.parse_known_args()
    return args


def main():
    args = parse_args()

    bucket = args.GCS_BUCKET
    input_path = args.INPUT_PATH or f"gs://{bucket}/landing_zone/streaming/orders/"
    checkpoint_path = args.CHECKPOINT_PATH or f"gs://{bucket}/_checkpoints/ch05/orders_to_iceberg/"
    quarantine_path = args.QUARANTINE_PATH or f"gs://{bucket}/quarantine/live_orders/"

    max_files = int(args.MAX_FILES_PER_TRIGGER)
    trigger_seconds = int(args.TRIGGER_SECONDS)

    spark = build_spark("ch05_lz_orders_to_iceberg", args.SPARK_LOG_LEVEL)

    ensure_iceberg_objects(spark, args.CATALOG_NAME, args.DB_NAME, args.TABLE_NAME)

    df = (
        spark.readStream
        .schema(landing_zone_schema())
        .option("maxFilesPerTrigger", max_files)
        .json(input_path)
    )

    query = (
        df.writeStream
        .queryName("ch05_orders_to_iceberg")
        .foreachBatch(write_batch_factory(
            quarantine_path=quarantine_path,
            catalog=args.CATALOG_NAME,
            db=args.DB_NAME,
            table=args.TABLE_NAME,
        ))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
