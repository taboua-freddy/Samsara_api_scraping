import os
from pathlib import Path

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound


def require_test_name(variable: str, value: str | None) -> str:
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
    raw_bucket = require_test_name(
        "GCS_RAW_BUCKET_NAME", os.getenv("GCS_RAW_BUCKET_NAME")
    )
    flattened_bucket = require_test_name(
        "GCS_FLATTENED_BUCKET_NAME", os.getenv("GCS_FLATTENED_BUCKET_NAME")
    )
    dataset_id = require_test_name("DATABASE_ID", os.getenv("DATABASE_ID"))

    storage_client = storage.Client()
    for bucket_name in (raw_bucket, flattened_bucket):
        try:
            storage_client.get_bucket(bucket_name)
            print(f"Bucket de test déjà présent: {bucket_name}")
        except NotFound:
            storage_client.create_bucket(bucket_name, location="EU")
            print(f"Bucket de test créé: {bucket_name}")

    bigquery_client = bigquery.Client()
    qualified_dataset = f"{bigquery_client.project}.{dataset_id}"
    try:
        bigquery_client.get_dataset(qualified_dataset)
        print(f"Dataset de test déjà présent: {qualified_dataset}")
    except NotFound:
        dataset = bigquery.Dataset(qualified_dataset)
        dataset.location = "EU"
        bigquery_client.create_dataset(dataset)
        print(f"Dataset de test créé: {qualified_dataset}")


if __name__ == "__main__":
    main()
