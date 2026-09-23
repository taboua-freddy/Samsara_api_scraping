
import argparse
import atexit
import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from modules.gcp import BucketManager, GCSBigQueryLoader, GCSClient
from modules.interface import ColumnToUpdate, DownloadType
from modules.logs import MyLogger
from modules.metadata import (
    build_metadata,
    get_metadata,
    get_table_name_by_category,
    get_tables_default_table_names,
    make_meta_data,
)
from modules.processing import DataFetcher
from modules.raters import EndpointRateLimiter, MemoryAccess
from modules.samsara import SamsaraClient
from modules.transformation_configs import MAPPING_TABLES
from modules.utils import (
    CREDENTIALS_DIR,
    DEFAULT_START_DATE,
    LOGS_DIR,
    file_buffer,
    get_start_end_date_config,
    parallelize_execution,
    process_params,
)

load_dotenv()

standard_logger = MyLogger("standard_logger")

# Chargement des configurations à partir des variables d'environnement
samsara_api_token = os.getenv("SAMSARA_API_TOKEN")
gcs_raw_bucket_name = os.getenv("GCS_RAW_BUCKET_NAME")
gcs_flattened_bucket_name = os.getenv("GCS_FLATTENED_BUCKET_NAME")
database_id = os.getenv("DATABASE_ID")
gcp_credentials_file_name = os.getenv("GCP_CREDENTIALS_FILE_NAME")
if gcp_credentials_file_name:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
        CREDENTIALS_DIR, gcp_credentials_file_name
    )


def validate_runtime_config() -> None:
    required_values = {
        "SAMSARA_API_TOKEN": samsara_api_token,
        "GCS_RAW_BUCKET_NAME": gcs_raw_bucket_name,
        "GCS_FLATTENED_BUCKET_NAME": gcs_flattened_bucket_name,
        "DATABASE_ID": database_id,
    }
    missing = [name for name, value in required_values.items() if not value]
    if missing:
        raise RuntimeError(
            "Variables d'environnement manquantes: " + ", ".join(missing)
        )

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and not os.path.isfile(credentials_path):
        raise RuntimeError(
            "Le fichier GOOGLE_APPLICATION_CREDENTIALS configuré est introuvable"
        )


def download_missing_files(
    configs_for_update: dict,
    metadata: pd.DataFrame,
    start_date: str,
    end_date: str,
    max_workers: int = None,
):
    rate_limiter = EndpointRateLimiter()

    # Initialisation des clients
    delta_days = 1
    samsara_client = SamsaraClient(
        api_token=samsara_api_token, rate_limiter=rate_limiter, delta_days=delta_days
    )
    gcs_client = GCSClient(bucket_name=gcs_raw_bucket_name)

    missing_dates = gcs_client.bucket_manager.missing_dates(metadata=metadata, configs_for_update=configs_for_update,
                                                            end_date=datetime.strptime(end_date, "%d/%m/%Y"),
                                                            start_date=datetime.strptime(start_date, "%d/%m/%Y"))
    tasks = []

    for table_name, dates in missing_dates.items():
        for date in dates:
            start_date = date.strftime("%d/%m/%Y")
            end_date = (date + pd.DateOffset(days=1)).strftime("%d/%m/%Y")
            all_metadata = make_meta_data(start_date, end_date)
            metadata = get_metadata(all_metadata, table_names=[MAPPING_TABLES.get(table_name, table_name)])
            if metadata.empty or metadata.iloc[0].get("download_type") == DownloadType.ONESHOT.value:
                continue

            for _index, row in metadata.iterrows():
                endpoint_info = row.to_dict()
                data_fetcher = DataFetcher(
                    samsara_client, gcs_client, endpoint_info, max_workers=max_workers
                )
                tasks.append(data_fetcher)

    # Exécution des tâches en parallèle
    parallelize_execution(
        tasks=tasks,
        func="fetch_and_upload",
        logger=standard_logger,
        max_workers=max_workers,
    )


def scrape_samsara_to_gcs(
    metadata: pd.DataFrame,
    is_exception: bool = False,
    iteration: int = 0,
    max_workers: int = None,
):

    # Initialisation du rate limiter global et par endpoint
    rate_limiter = EndpointRateLimiter()

    shared_vars_manager = MemoryAccess()
    shared_vars_manager.write("metadata", metadata)
    shared_vars_manager.write("is_exception", is_exception)

    # Initialisation des clients
    delta_days = 1
    samsara_client = SamsaraClient(
        api_token=samsara_api_token,
        rate_limiter=rate_limiter,
        shared_vars_manager=shared_vars_manager,
        delta_days=delta_days,
    )
    gcs_client = GCSClient(bucket_name=gcs_raw_bucket_name)

    # Liste des tâches à exécuter
    tasks = []
    for index, row in metadata.iterrows():
        index += 1
        if bool(row.get("is_processed", False)):
            standard_logger.info(
                f"La ligne {index} est marquée comme déjà traitée, saut"
            )
            continue
        if not row.get("is_exception") and is_exception:
            standard_logger.info(
                f"La ligne {index} n'est pas marquée comme une exception, elle sera traitée lors de l'execution normale"
            )
            continue

        endpoint_info = row.to_dict()
        if not is_exception:
            if iteration > 0:
                params = endpoint_info.get("params")
                params = params if not pd.isna(params) else {}
                try:
                    params = process_params(params)
                    if get_start_end_date_config(params) is None:
                        standard_logger.info(
                            f"la ligne {index} a ete déjà traitée lors de la premiere itération"
                        )
                        continue
                except Exception as e:
                    standard_logger.error(
                        f"Erreur lors de la conversion des paramètres pour {endpoint_info.get('table_name')}: {e}"
                    )
                    continue
            if row.get("is_exception", True):
                standard_logger.info(
                    f"La ligne {index} est marquée comme une exception, elle sera traitée pendant l'execution des exceptions"
                )
                continue
        data_fetcher = DataFetcher(
            samsara_client, gcs_client, endpoint_info, max_workers=max_workers
        )
        tasks.append(data_fetcher)

        # Exécution des tâches en parallèle
    parallelize_execution(
        tasks=tasks,
        func="fetch_and_upload",
        logger=standard_logger,
        max_workers=max_workers,
    )


def load_to_bigquery(
    metadata: pd.DataFrame,
    configs_for_update: dict,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    shared_vars_manager = MemoryAccess()
    shared_vars_manager.write("metadata", metadata)
    shared_vars_manager.write(
        "bucket_manager", BucketManager(bucket_name=gcs_flattened_bucket_name)
    )
    GCSBigQueryLoader(
        bucket_name=gcs_flattened_bucket_name,
        dataset_id=database_id,
        memory_manager=shared_vars_manager,
        _from=start_date,
        _to=end_date,
    ).run(configs_for_update=configs_for_update, metadata=metadata)


def upload_logs(version: str = "v1"):
    gcs_client = GCSClient(bucket_name=gcs_raw_bucket_name)
    local_logs_path = LOGS_DIR
    for file in os.listdir(local_logs_path):
        if file.endswith(".log") or file.startswith(".log.") or file.split(".log.")[-1].isdigit():
            with open(os.path.join(local_logs_path, file), "rb") as log_file:
                buffer = file_buffer(log_file.read())
            destination = f"{gcs_client.bucket_manager.gcs_log_path}/{datetime.now().strftime('%Y_%m_%d')}/{version}/{file}"
            gcs_client.upload_bytes(buffer, destination)


def cleanup_load_manifest(gcs_client: GCSClient) -> dict:
    retention_days = int(os.getenv("BIGQUERY_MANIFEST_RETENTION_DAYS", "30"))
    configs = gcs_client.get_configs_for_update()
    flattened_client = GCSClient(bucket_name=gcs_flattened_bucket_name)
    report = flattened_client.cleanup_bigquery_load_manifest(
        configs_for_update=configs,
        retention_days=retention_days,
        dry_run=False,
    )
    standard_logger.info(f"Nettoyage du manifeste BigQuery terminé: {report}")
    return report


def cleanup_logs(gcs_client: GCSClient) -> dict:
    retention_days = int(os.getenv("LOG_RETENTION_DAYS", "30"))
    if retention_days < 1:
        raise ValueError("LOG_RETENTION_DAYS doit être supérieur ou égal à 1")

    cloud_report = gcs_client.bucket_manager.cleanup_logs(retention_days)
    cutoff = datetime.now() - timedelta(days=retention_days)
    local_deleted = 0
    for file_name in os.listdir(LOGS_DIR):
        file_path = os.path.join(LOGS_DIR, file_name)
        if not os.path.isfile(file_path) or ".log" not in file_name:
            continue
        modified_at = datetime.fromtimestamp(os.path.getmtime(file_path))
        if modified_at <= cutoff:
            os.remove(file_path)
            local_deleted += 1

    report = {
        "retention_days": retention_days,
        "local_deleted": local_deleted,
        "cloud": cloud_report,
    }
    standard_logger.info(f"Nettoyage des logs terminé: {report}")
    return report


def print_stage_progress(current: int, total: int, label: str) -> None:
    width = 24
    completed = round(width * current / total)
    bar = "#" * completed + "-" * (width - completed)
    print(f"[{bar}] {current}/{total} {label}")


def run_download_stage(
    gcs_client: GCSClient,
    metadata: pd.DataFrame,
    configs_for_update: dict,
    start_date: str,
    end_date: str,
    max_workers: int | None,
    historical: bool = False,
) -> None:
    started_at = datetime.now()
    scrape_samsara_to_gcs(metadata=metadata, iteration=0, max_workers=max_workers)
    scrape_samsara_to_gcs(
        metadata=metadata,
        iteration=0,
        is_exception=True,
        max_workers=max_workers,
    )
    if not historical:
        download_missing_files(
            configs_for_update=configs_for_update,
            metadata=metadata,
            start_date=start_date,
            end_date=end_date,
            max_workers=max_workers,
        )
        gcs_client.update_configs_for_update(
            metadata=metadata,
            end_time=end_date,
            col_to_update=ColumnToUpdate.DOWNLOAD,
        )
        missing_dates = gcs_client.bucket_manager.missing_dates(
            metadata=metadata,
            configs_for_update=configs_for_update,
            end_date=datetime.strptime(end_date, "%d/%m/%Y"),
            start_date=datetime.strptime(start_date, "%d/%m/%Y"),
        )
        gcs_client.update_configs_for_update(
            metadata=metadata,
            end_time=end_date,
            col_to_update=ColumnToUpdate.DATE_NO_DATA,
            missing_dates=missing_dates,
        )
    elapsed = (datetime.now() - started_at).total_seconds()
    standard_logger.info(f"Étape download terminée en {elapsed} secondes")


def run_transform_stage(
    gcs_client: GCSClient,
    metadata: pd.DataFrame,
    configs_for_update: dict,
    end_date: str,
    start_date: str | None = None,
    historical: bool = False,
) -> None:
    report = gcs_client.transform_and_save_data(
        target_bucket_name=gcs_flattened_bucket_name,
        metadata=metadata,
        configs_for_update={} if historical else configs_for_update,
        skip_existing=historical,
        start_date=(
            datetime.strptime(start_date, "%d/%m/%Y")
            if historical and start_date
            else None
        ),
        end_date=(
            datetime.strptime(end_date, "%d/%m/%Y") if historical else None
        ),
    )
    if historical and isinstance(report, dict):
        selected = report.get("selected_files", 0)
        uploaded = report.get("uploaded_files", 0)
        skipped = report.get("skipped_files", 0)
        print(
            "Transformation historique: "
            f"{selected} fichier(s) brut(s), {uploaded} fichier(s) aplati(s) créé(s), "
            f"{skipped} déjà présent(s)."
        )
        if selected and not uploaded and not skipped:
            raise RuntimeError(
                "La transformation historique a lu des fichiers bruts mais n'a produit "
                "aucun fichier aplati. Vérifier le schéma des données source."
            )
    if not historical:
        gcs_client.update_configs_for_update(
            metadata=metadata,
            end_time=end_date,
            col_to_update=ColumnToUpdate.TRANSFORMATION,
        )


def run_load_stage(
    gcs_client: GCSClient,
    metadata: pd.DataFrame,
    configs_for_update: dict,
    end_date: str,
    start_date: str | None = None,
    historical: bool = False,
) -> None:
    load_to_bigquery(
        metadata=metadata,
        configs_for_update={} if historical else configs_for_update,
        start_date=(
            datetime.strptime(start_date, "%d/%m/%Y")
            if historical and start_date
            else None
        ),
        end_date=(
            datetime.strptime(end_date, "%d/%m/%Y") if historical else None
        ),
    )
    if not historical:
        gcs_client.update_configs_for_update(
            metadata=metadata,
            end_time=end_date,
            col_to_update=ColumnToUpdate.DATABASE,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Téléchargement des données Samsara et chargement dans BigQuery"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        help="Date de debut pour la récupération des données format: jj/mm/aaaa",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        help="Date de fin pour la récupération des données format: jj/mm/aaaa",
    )
    parser.add_argument(
        "--table_file_path",
        type=str,
        help="Chemin du fichier contenant les noms des tables à traiter, si == ALL, toutes les tables seront traitées",
    )
    parser.add_argument(
        "--max_workers", type=int, help="Nombre de requêtes à traiter en parallèle"
    )
    parser.add_argument(
        "--table_cat", type=str, help="Quelle version de la table à utiliser pour le traitement"
    )
    parser.add_argument(
        "--table",
        dest="selected_tables",
        action="append",
        help="Table précise à traiter. L'option peut être répétée.",
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=("download", "transform", "load"),
        default=("download", "transform", "load"),
        help="Étapes à exécuter, dans l'ordre du pipeline.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valide et affiche le plan sans appeler Samsara, GCS ou BigQuery.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        help="Fenêtre glissante se terminant aujourd'hui, adaptée aux exécutions planifiées.",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help=(
            "Retraite exactement la période start_date/end_date sans utiliser ni "
            "modifier les curseurs incrémentaux. La date de fin est exclusive."
        ),
    )
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date
    table_file_path = args.table_file_path
    table_cat = args.table_cat

    if args.lookback_days is not None:
        if args.lookback_days < 1:
            parser.error("lookback-days doit être supérieur ou égal à 1")
        if args.start_date or args.end_date:
            parser.error("lookback-days ne peut pas être combiné avec start_date/end_date")
        today = datetime.now()
        start_date = (today - timedelta(days=args.lookback_days)).strftime("%d/%m/%Y")
        end_date = today.strftime("%d/%m/%Y")

    if args.historical and (args.start_date is None or args.end_date is None):
        parser.error("historical exige start_date et end_date")
    if args.historical and args.lookback_days is not None:
        parser.error("historical ne peut pas être combiné avec lookback-days")

    # start_date = "26/05/2025"
    # end_date = "30/05/2025"

    if end_date is None:
        end_date = datetime.now().strftime("%d/%m/%Y")

    if start_date is None:
        start_date = DEFAULT_START_DATE

    try:
        parsed_start_date = datetime.strptime(start_date, "%d/%m/%Y")
        parsed_end_date = datetime.strptime(end_date, "%d/%m/%Y")
    except ValueError:
        parser.error("Les dates doivent respecter le format jj/mm/aaaa")
    if parsed_start_date > parsed_end_date:
        parser.error("start_date doit être antérieure ou égale à end_date")
    if args.historical and parsed_start_date == parsed_end_date:
        parser.error("en mode historical, start_date doit être antérieure à end_date")
    if args.max_workers is not None and args.max_workers < 1:
        parser.error("max_workers doit être supérieur ou égal à 1")

    if args.selected_tables:
        table_names = list(dict.fromkeys(args.selected_tables))
    elif table_file_path == "ALL":
        table_names = make_meta_data(start_date, end_date)["table_name"].tolist()
    elif table_file_path is not None and os.path.isfile(table_file_path):
        df = pd.read_excel(table_file_path)
        table_names = df.iloc[:, 0].tolist()
    else:
        table_names = get_table_name_by_category().get(table_cat, get_tables_default_table_names())
    standard_logger.info(f"Table names to process: {table_names}")
    print(f"Table names to process: {table_names}")

    if args.dry_run:
        plan = get_metadata(
            make_meta_data(start_date, end_date), table_names=table_names
        )
        if plan.empty:
            parser.error("Aucune table valide dans le plan demandé")
        missing_tables = sorted(set(table_names) - set(plan["table_name"]))
        if missing_tables:
            parser.error("Tables inconnues: " + ", ".join(missing_tables))
        columns = ["table_name", "family", "endpoint", "download_type"]
        print(f"Étapes: {', '.join(args.stages)}")
        print(plan[columns].to_string(index=False))
        raise SystemExit(0)

    validate_runtime_config()

    log_version = "v1" if table_cat is None else table_cat
    gcs_client = GCSClient(bucket_name=gcs_raw_bucket_name)
    lock_ttl = int(os.getenv("PIPELINE_LOCK_TTL_MINUTES", "1440"))
    execution_lock = gcs_client.bucket_manager.acquire_execution_lock(lock_ttl)
    atexit.register(execution_lock.release)
    standard_logger.info("Initialisation du client GCS et récupération des configurations pour la mise à jour.")
    configs_for_update = gcs_client.get_configs_for_update()
    if configs_for_update is None:
        raise RuntimeError("Le fichier de configuration est endommagé")
    standard_logger.info("Fichier de configuration récupéré avec succès.")
    standard_logger.info("Construction des métadonnées à partir des configurations et des noms de tables.")
    metadata = build_metadata(
        configs_for_update=configs_for_update,
        table_names=table_names,
        start_date=start_date,
        end_date=end_date,
        use_configured_start=not args.historical,
    )
    standard_logger.info("Metadata construite avec succès.")
    if metadata.empty:
        raise RuntimeError("Aucune metadata trouvée pour les tables spécifiées")

    max_workers = args.max_workers

    active_stages = [stage for stage in ("download", "transform", "load") if stage in args.stages]
    standard_logger.info(f"Début des étapes: {active_stages}")
    total_stages = len(active_stages)
    completed_stages = 0
    print_stage_progress(completed_stages, total_stages, "démarrage")
    if "download" in args.stages:
        run_download_stage(
            gcs_client,
            metadata,
            configs_for_update,
            start_date,
            end_date,
            max_workers,
            historical=args.historical,
        )
        completed_stages += 1
        print_stage_progress(completed_stages, total_stages, "download terminé")
    if "transform" in args.stages:
        run_transform_stage(
            gcs_client,
            metadata,
            configs_for_update,
            end_date,
            start_date=start_date,
            historical=args.historical,
        )
        completed_stages += 1
        print_stage_progress(completed_stages, total_stages, "transform terminé")
    if "load" in args.stages:
        run_load_stage(
            gcs_client,
            metadata,
            configs_for_update,
            end_date,
            start_date=start_date,
            historical=args.historical,
        )
        completed_stages += 1
        print_stage_progress(completed_stages, total_stages, "load terminé")

    # Chargement des logs dans GCS
    upload_logs(log_version)
    standard_logger.info("Logs chargés dans GCS avec succès.")

    cleanup_load_manifest(gcs_client)
    cleanup_logs(gcs_client)

    execution_lock.release()
    atexit.unregister(execution_lock.release)

    print("----------------------> Fin de l'execution <----------------------")
