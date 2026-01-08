import os
from google.cloud import bigquery
from google.cloud import storage


# ----------------------------
# Hard-coded tables to refresh
# ----------------------------
# BigQuery table name  -> Iceberg table folder name (usually same)
TABLES = {
    "customers": "customers",
    "products": "products",
    "orders": "orders",
    "live_orders": "live_orders",
}


def env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise KeyError(f"Missing required env var: {name}")
    return v


def read_gcs_text(bucket: str, blob_path: str) -> str:
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket}/{blob_path} not found")
    return blob.download_as_text().strip()


def latest_metadata_file(bucket: str, metadata_dir: str) -> str:
    """
    metadata_dir example:
      iceberg/warehouse/bronze/customers/metadata
    Reads version-hint.text -> uses vN.metadata.json
    """
    metadata_dir = metadata_dir.strip("/")
    hint_path = f"{metadata_dir}/version-hint.text"
    version_str = read_gcs_text(bucket, hint_path)

    try:
        n = int(version_str)
    except ValueError as e:
        raise ValueError(
            f"Invalid version-hint.text at gs://{bucket}/{hint_path}: {version_str}"
        ) from e

    return f"gs://{bucket}/{metadata_dir}/v{n}.metadata.json"


def create_or_replace_external_iceberg_table(
    bq: bigquery.Client,
    project_id: str,
    dataset: str,
    table: str,
    connection: str,
    metadata_file: str,
) -> None:
    fqn = f"`{project_id}.{dataset}.{table}`"
    conn = f"`{connection}`"

    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE {fqn}
    WITH CONNECTION {conn}
    OPTIONS (
      format = 'ICEBERG',
      uris = ['{metadata_file}']
    )
    """
    job = bq.query(sql)
    job.result()


def main():
    project_id = env_required("PROJECT_ID")
    bucket = env_required("BUCKET")

    # BigQuery dataset where you expose the external tables
    bq_dataset = env_required("BQ_DATASET")

    # Fully-qualified connection:
    # projects/<PROJECT_ID>/locations/<REGION>/connections/<CONNECTION_NAME>
    bq_connection = env_required("BQ_CONNECTION")

    # Warehouse prefix inside bucket (keep configurable, but simple)
    warehouse_prefix = os.getenv("WAREHOUSE_PREFIX", "iceberg/warehouse").strip("/")

    # Iceberg namespace for these tables (you said bronze)
    iceberg_db = os.getenv("ICEBERG_DB", "bronze").strip()

    bq = bigquery.Client(project=project_id)

    print(f"[refresh] project={project_id} dataset={bq_dataset}")
    print(f"[refresh] connection={bq_connection}")
    print(f"[refresh] bucket={bucket} warehouse_prefix={warehouse_prefix} iceberg_db={iceberg_db}")
    print(f"[refresh] tables={list(TABLES.keys())}")

    for bq_table, iceberg_table in TABLES.items():
        metadata_dir = f"{warehouse_prefix}/{iceberg_db}/{iceberg_table}/metadata"
        metadata_file = latest_metadata_file(bucket, metadata_dir)

        print(f"[refresh] {bq_table} -> {metadata_file}")

        create_or_replace_external_iceberg_table(
            bq=bq,
            project_id=project_id,
            dataset=bq_dataset,
            table=bq_table,
            connection=bq_connection,
            metadata_file=metadata_file,
        )

    print("[refresh] done")


if __name__ == "__main__":
    main()
