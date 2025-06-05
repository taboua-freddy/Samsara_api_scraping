import json
import logging
import os
from datetime import datetime
from logging import exception

import pandas as pd
from dotenv import load_dotenv
import argparse

from maintenance_predictive.Samsara_api_scraping.modules.interface import DownloadType
from modules.interface import ColumnToUpdate
from modules.metadata import get_metadata, build_metadata
from modules.transformation_configs import MAPPING_TABLES
from modules.utils import (
    CREDENTIALS_DIR,
    parquet_buffer,
    get_start_end_date_config,
    process_params,
    DATA_DIR,
    LOGS_DIR,
    file_buffer, DEFAULT_START_DATE,
)
from modules.raters import MemoryAccess
from modules.logs import MyLogger
from modules.gcp import GCSBigQueryLoader, GCSClient, BucketManager
from modules.processing import DataFetcher, TransformData
from modules.raters import EndpointRateLimiter
from modules.samsara import SamsaraClient
from modules.utils import parallelize_execution
from modules.metadata import make_meta_data, get_metadata_by_table_names

load_dotenv()

standard_logger = MyLogger("standard_logger")

# Chargement des configurations à partir des variables d'environnement
samsara_api_token = os.getenv("SAMSARA_API_TOKEN")
gcs_raw_bucket_name = os.getenv("GCS_RAW_BUCKET_NAME")
gcs_flattened_bucket_name = os.getenv("GCS_FLATTENED_BUCKET_NAME")
database_id = os.getenv("DATABASE_ID")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
    CREDENTIALS_DIR, os.getenv("GCP_CREDENTIALS_FILE_NAME")
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

            for index, row in metadata.iterrows():
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
        # _to=datetime.strptime(end_date, "%d/%m/%Y")
    ).run(configs_for_update=configs_for_update, metadata=metadata)


def upload_logs():
    gcs_client = GCSClient(bucket_name=gcs_raw_bucket_name)
    local_logs_path = LOGS_DIR
    for file in os.listdir(local_logs_path):
        if file.endswith(".log"):
            buffer = file_buffer(open(os.path.join(local_logs_path, file), "rb").read())
            destination = f"{gcs_client.bucket_manager.gcs_log_path}/{datetime.now().strftime('%Y_%m_%d')}/{file}"
            gcs_client.upload_bytes(buffer, destination)


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
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date
    table_file_path = args.table_file_path

    # start_date = "01/02/2024"
    # end_date = "02/02/2024"

    if end_date is None:
        end_date = datetime.now().strftime("%d/%m/%Y")

    if start_date is None:
        start_date = DEFAULT_START_DATE

    if table_file_path == "ALL":
        table_names = make_meta_data(start_date, end_date)["table_name"].tolist()
    elif table_file_path is not None and os.path.isfile(table_file_path):
        df = pd.read_excel(table_file_path)
        table_names = df.iloc[:, 0].tolist()
    else:
        table_names = [
            # EV ok
            "fleet_vehicle_stats_evAverageBatteryTemperatureMilliCelsius",
            "fleet_vehicle_stats_evBatteryStateOfHealthMilliPercent",
            "fleet_vehicle_stats_evChargingCurrentMilliAmp",
            "fleet_vehicle_stats_evChargingEnergyMicroWh",
            "fleet_vehicle_stats_evChargingStatus",
            "fleet_vehicle_stats_evChargingVoltageMilliVolt",
            "fleet_vehicle_stats_evConsumedEnergyMicroWh",
            "fleet_vehicle_stats_evDistanceDrivenMeters",
            "fleet_vehicle_stats_evRegeneratedEnergyMicroWh",
            "fleet_vehicle_stats_evStateOfChargeMilliPercent",
            # time ok
            "fleet_vehicle_stats_obdEngineSeconds",
            "fleet_vehicle_stats_engineStates",
            "fleet_vehicle_stats_gpsOdometerMeters",
            "fleet_vehicle_stats_ecuSpeedMph",
            "fleet_vehicles_fuel_energy",
            "fleet_vehicle_stats_faultCodes",
            "fleet_safety_events",
            "fleet_assets_reefers",
            # stats
            # Todo: add the rest of the stats to transformation configs
            # "fleet_vehicle_stats_intakeManifoldTemperatureMilliC",
            # "fleet_vehicle_stats_engineRpm",
            # "fleet_vehicle_stats_engineOilPressureKPa",
            # "fleet_vehicle_stats_engineLoadPercent",
            # "fleet_vehicle_stats_engineImmobilizer",
            # "fleet_vehicle_stats_engineCoolantTemperatureMilliC",
            # "fleet_vehicle_stats_defLevelMilliPercent",
            # "fleet_vehicle_stats_batteryMilliVolts",
            # "fleet_vehicle_stats_barometricPressurePa",
            # "fleet_vehicle_stats_ambientAirTemperatureMilliC"
            # core ok
            "fleet_vehicles",
            "fleet_assets",
            "fleet_trailers",
            "fleet_tags",
            "fleet_devices",
        ]

    gcs_client = GCSClient(bucket_name=gcs_raw_bucket_name)
    configs_for_update = {} # gcs_client.get_configs_for_update()
    print(f"Table names to process avant: {configs_for_update}")
    if configs_for_update is None:
        standard_logger.error("Le fichier de conf est endommagé.")
        exit()

    metadata = build_metadata(
        configs_for_update=configs_for_update,
        table_names=table_names,
        start_date=start_date,
        end_date=end_date,
    )
    standard_logger.info("Metadata construite avec succès.")
    if metadata.empty:
        standard_logger.error("Aucune metadata trouvée pour les tables spécifiées.")
        exit()

    max_workers = args.max_workers

    start = datetime.now()
    scrape_samsara_to_gcs(metadata=metadata, iteration=0, max_workers=max_workers)
    end = datetime.now()
    td = (end - start).total_seconds()
    standard_logger.info(
        f"Temps d'exécution sans les endpoint avec des exceptions: {td} secondes "
    )
    print(f"Temps d'exécution : {td} secondes ")

    filters = ["is_exception"]
    for index, filter_ in enumerate(filters):
        start = datetime.now()
        scrape_samsara_to_gcs(
            metadata=metadata,
            iteration=index,
            is_exception=True,
            max_workers=max_workers,
        )
        end = datetime.now()
        td = (end - start).total_seconds()
        standard_logger.info(
            f"Temps d'exécution pour l'exception '{filter_}': {td} secondes "
        )

    download_missing_files(
        configs_for_update=configs_for_update,
        metadata=metadata,
        start_date=start_date,
        end_date=end_date,
        max_workers=max_workers,
    )
    gcs_client.update_configs_for_update(metadata=metadata, end_time=end_date, col_to_update=ColumnToUpdate.DOWNLOAD)

    # Récupération et sauvegarde des dates sans données dans le fichier de configuration
    missing_dates = gcs_client.bucket_manager.missing_dates(
        metadata=metadata,
        configs_for_update=configs_for_update,
        end_date=datetime.strptime(end_date, "%d/%m/%Y"),
        start_date=datetime.strptime(start_date, "%d/%m/%Y")
    )
    standard_logger.info("Dates sans données récupérées avec succès.")
    gcs_client.update_configs_for_update(
        metadata=metadata,
        end_time=end_date,
        col_to_update=ColumnToUpdate.DATE_NO_DATA,
        missing_dates=missing_dates
    )
    standard_logger.info("Fichier de configuration mis à jour avec les dates sans données.")

    # Transformation des données
    gcs_client.transform_and_save_data(
        target_bucket_name=gcs_flattened_bucket_name,
        metadata=metadata
    )
    gcs_client.update_configs_for_update(
        metadata=metadata,
        end_time=end_date,
        col_to_update=ColumnToUpdate.TRANSFORMATION
    )

    # Chargement des données dans BigQuery
    load_to_bigquery(
        metadata=metadata,
        configs_for_update=configs_for_update,
    )
    gcs_client.update_configs_for_update(metadata=metadata, end_time=end_date, col_to_update=ColumnToUpdate.DATABASE)
    # Chargement des logs dans GCS
    upload_logs()

    print("----------------------> Fin de l'execution <----------------------")
    configs_for_update = gcs_client.get_configs_for_update()
    print(f"Table names to process: {configs_for_update}")
