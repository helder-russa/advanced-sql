import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import input_file_name, regexp_extract, col


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ch05_stream_read_lz_orders")
        .getOrCreate()
    )


def lz_orders_schema() -> StructType:
    """
    Schema aligned with what your streaming ingestion job writes to GCS.
    Keep it strict on the known fields; allow evolution by adding optional fields later.
    """
    return StructType([
        StructField("id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("created_at", StringType(), True),

        # metadata added by ingestion job
        StructField("_pubsub_message_id", StringType(), True),
        StructField("_pubsub_publish_time", StringType(), True),
        StructField("_ingest_time_utc", StringType(), True),
    ])


def main():
    bucket = os.getenv("GCS_BUCKET", "advance-sql-de-bucket")
    input_path = os.getenv("INPUT_PATH", f"gs://{bucket}/landing_zone/streaming/orders/")
    checkpoint_path = os.getenv("CHECKPOINT_PATH", f"gs://{bucket}/_checkpoints/ch05/orders_lz_stream_read/")
    max_files_per_trigger = int(os.getenv("MAX_FILES_PER_TRIGGER", "50"))

    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    df = (
        spark.readStream
        .schema(lz_orders_schema())
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .json(input_path)
        .withColumn("_input_file", input_file_name())
    )

    # Extract ingestion partitions from the path: ingest_dt=YYYY-MM-DD/ingest_hr=HH
    df = (
        df
        .withColumn("ingest_dt", regexp_extract(col("_input_file"), r"ingest_dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1))
        .withColumn("ingest_hr", regexp_extract(col("_input_file"), r"ingest_hr=([0-9]{2})", 1))
    )

    def foreach_batch(batch_df, batch_id: int):
        cnt = batch_df.count()
        print(f"\n=== Batch {batch_id} ===")
        print(f"Rows: {cnt}")

        batch_df.select(
            "id", "customer_id", "product_id", "quantity",
            "total_amount", "currency", "created_at",
            "_pubsub_message_id", "_pubsub_publish_time", "_ingest_time_utc",
            "ingest_dt", "ingest_hr"
        ).show(10, truncate=False)

    query = (
        df.writeStream
        .queryName("ch05_orders_lz_file_stream")
        .foreachBatch(foreach_batch)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
