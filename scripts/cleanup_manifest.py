import argparse
import json
import os
from pathlib import Path

from modules.gcp import GCSClient


def configure_credentials() -> None:
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    filename = os.getenv("GCP_CREDENTIALS_FILE_NAME")
    if filename:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
            Path("/app/credentials") / filename
        )


def require_safe_environment(allow_production: bool) -> None:
    if allow_production:
        return
    variables = (
        "GCS_RAW_BUCKET_NAME",
        "GCS_FLATTENED_BUCKET_NAME",
        "DATABASE_ID",
    )
    unsafe = [name for name in variables if "test" not in os.getenv(name, "").lower()]
    if unsafe:
        raise RuntimeError(
            "Nettoyage refusé hors environnement de test: " + ", ".join(unsafe)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Nettoyage sûr du manifeste BigQuery")
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Applique le nettoyage. Sans cette option, seule une simulation est faite.",
    )
    parser.add_argument(
        "--allow-production",
        action="store_true",
        help="Autorise explicitement des ressources dont le nom ne contient pas test.",
    )
    args = parser.parse_args()

    configure_credentials()
    require_safe_environment(args.allow_production)
    raw_client = GCSClient(os.environ["GCS_RAW_BUCKET_NAME"])
    flattened_client = GCSClient(os.environ["GCS_FLATTENED_BUCKET_NAME"])
    report = flattened_client.cleanup_bigquery_load_manifest(
        configs_for_update=raw_client.get_configs_for_update(),
        retention_days=args.retention_days,
        dry_run=not args.apply,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
