Generict Requirements - either producer or consumer:
1. Python - python 3.10+ version installed
1.1 macOS: brew install git
1.2 Windows: install from the official Git site
1.3 Linux: sudo apt-get install git

2. Pip

NOTE: To setup chapter_05 producers, please refer to the readme file within producers folder.



If all setup up, the steps are:

Launch uvicorn/Swagger if not live yet and generate some data

Create vars for streaming in your terminal:

```bash
export PROJECT_ID="advance-sql-de-demo"
export REGION="europe-west1"
export BUCKET="advance-sql-de-bucket"
export JOB_NAME="ch05-streaming-ingestion-job"
```

The run the job

```bash
gcloud run jobs execute "$JOB_NAME" --region="$REGION"
```

read the data and move to a bronze structured layer

```bash 
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/streaming/01_lz_orders_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
                spark.driver.cores=4,\
                spark.executor.cores=4,\
                spark.dataproc.driver.disk.size=250g,\
                spark.dataproc.executor.disk.size=250g,\
                spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
                spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
                spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
                spark.sql.catalog.local.type=hadoop,\
                spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --GCS_BUCKET="$BUCKET" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/streaming/orders/" \
  --CHECKPOINT_PATH="gs://$BUCKET/_checkpoints/ch05/orders_to_iceberg/" \
  --QUARANTINE_PATH="gs://$BUCKET/quarantine/orders/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="orders" \
  --MAX_FILES_PER_TRIGGER="50" \
  --TRIGGER_SECONDS="15" \
  --SPARK_LOG_LEVEL="WARN"
  ```

ok, now focus on batch layer, execute the following:

Create vars for batch in your terminal:

```bash
export PROJECT_ID="advance-sql-de-demo"
export REGION="europe-west1"
export BUCKET="advance-sql-de-bucket"
export SNAPSHOT_DATE="2026-01-06"
```

and execute the py file to ingest customers:

```bash
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/batch/01_lz_batch_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dataproc.driver.disk.size=250g,\
spark.dataproc.executor.disk.size=250g,\
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.local.type=hadoop,\
spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --ENTITY="customers" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/batch/customers/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="customers" \
  --RESET_TABLE="true" \
  --SPARK_LOG_LEVEL="WARN"
```

then execute the py file to ingest products:

```bash
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/batch/01_lz_batch_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dataproc.driver.disk.size=250g,\
spark.dataproc.executor.disk.size=250g,\
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.local.type=hadoop,\
spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --ENTITY="customers" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/batch/products/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="products" \
  --RESET_TABLE="true" \
  --SPARK_LOG_LEVEL="WARN"
```


  gcloud dataproc batches list --region=europe-west1

  gcloud dataproc batches cancel BATCH_ID --region=europe-west1

  gcloud dataproc batches cancel b551cfe730434cea97ae6bd38c3df268 --region=europe-west1


