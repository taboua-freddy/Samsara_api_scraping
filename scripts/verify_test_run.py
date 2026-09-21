import json
import os
from pathlib import Path

from google.cloud import bigquery, storage


def require_test_name(variable: str) -> str:
    value = os.getenv(variable)
    if not value or "test" not in value.lower():
        raise RuntimeError(f"{variable} doit contenir explicitement 'test'")
    return value


def configure_credentials() -> None:
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    filename = os.getenv("GCP_CREDENTIALS_FILE_NAME")
    if filename:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
            Path("/app/credentials") / filename
        )


def main() -> None:
    configure_credentials()
    raw_bucket_name = require_test_name("GCS_RAW_BUCKET_NAME")
    flattened_bucket_name = require_test_name("GCS_FLATTENED_BUCKET_NAME")
    dataset_id = require_test_name("DATABASE_ID")
    prefix = "vehicle_stats/fleet_vehicle_stats_faultCodes/"

    storage_client = storage.Client()
    for label, bucket_name in (
        ("raw", raw_bucket_name),
        ("flattened", flattened_bucket_name),
    ):
        blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))
        print(f"{label}_files={len(blobs)}")
        for blob in blobs:
            print(f"  {blob.name} ({blob.size} bytes, generation={blob.generation})")

    raw_bucket = storage_client.bucket(raw_bucket_name)
    checkpoint_blob = raw_bucket.blob("resources/configs/configs_for_update.json")
    checkpoint = json.loads(checkpoint_blob.download_as_text())
    table_checkpoint = checkpoint.get("fleet_vehicle_stats_faultCodes", {})
    print(f"checkpoint={json.dumps(table_checkpoint, sort_keys=True)}")

    flattened_bucket = storage_client.bucket(flattened_bucket_name)
    manifest_blob = flattened_bucket.blob(
        "resources/configs/bigquery_load_manifest.json"
    )
    manifest = json.loads(manifest_blob.download_as_text())
    print(f"manifest_entries={len(manifest)}")

    bigquery_client = bigquery.Client()
    qualified_dataset = f"{bigquery_client.project}.{dataset_id}"
    tables = [
        table.table_id
        for table in bigquery_client.list_tables(qualified_dataset)
        if table.table_id.startswith("fleet_vehicle_stats_faultCodes")
    ]
    print(f"bigquery_tables={len(tables)}")
    for table_id in sorted(tables):
        query = f"SELECT COUNT(*) AS row_count FROM `{qualified_dataset}.{table_id}`"
        row_count = next(iter(bigquery_client.query(query).result())).row_count
        print(f"  {table_id}: {row_count} rows")


if __name__ == "__main__":
    main()
