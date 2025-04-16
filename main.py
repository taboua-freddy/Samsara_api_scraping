import json
import logging
import os
from datetime import datetime
from logging import exception

import pandas as pd
from dotenv import load_dotenv
import argparse

from modules.metadata import get_metadata
from modules.utils import CREDENTIALS_DIR, parquet_buffer, get_start_end_date_config, process_params, DATA_DIR, \
    LOGS_DIR, file_buffer
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
samsara_api_token = os.getenv('SAMSARA_API_TOKEN')
# gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
# database_id = os.getenv('DATABASE_ID')
gcs_bucket_name = os.getenv('GCS_BUCKET_FLATTENED_NAME')
database_id = os.getenv('DWH_ID')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CREDENTIALS_DIR, os.getenv("GCP_CREDENTIALS_FILE_NAME"))

def upload_missing_files(tables_names:list[str], start_date: str, end_date: str, max_workers:int = None):
    # samsara_api_token = os.getenv('SAMSARA_API_TOKEN')
    # gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(CREDENTIALS_DIR, os.getenv("GCP_CREDENTIALS_FILE_NAME"))

    rate_limiter = EndpointRateLimiter()

    # Initialisation des clients
    delta_days = 1
    samsara_client = SamsaraClient(api_token=samsara_api_token, rate_limiter=rate_limiter, delta_days=delta_days)
    gcs_client = GCSClient(bucket_name=gcs_bucket_name)

    missing_files = gcs_client.bucket_manager.missing_files(
        tables_names=tables_names,
        start_date=datetime.strptime(start_date, "%d/%m/%Y"),
        end_date=datetime.strptime(end_date, "%d/%m/%Y")
    )
    tasks = []

    for table_name, dates in missing_files.items():
        for date in dates:
            start_date = date.strftime("%d/%m/%Y")
            end_date = (date + pd.DateOffset(days=1)).strftime("%d/%m/%Y")
            all_metadata = make_meta_data("metadata.xlsx", start_date, end_date)
            metadata = get_metadata(all_metadata, [table_name])
            # metadata = get_metadata_by_table_names([table_name], start_date, end_date)

            for index, row in metadata.iterrows():
                endpoint_info = row.to_dict()
                data_fetcher = DataFetcher(samsara_client, gcs_client, endpoint_info, max_workers=max_workers)
                tasks.append(data_fetcher)

    # Exécution des tâches en parallèle
    parallelize_execution(tasks=tasks, func="fetch_and_upload", logger=standard_logger, max_workers=max_workers)


def scrape_samsara_to_gcs(start_date: str, end_date: str, iteration: int, is_exception=False, filter="all", table_names:list[str]=None, max_workers:int = None):


    # Données de métadonnées pour les endpoints
    metadata = make_meta_data("metadata.xlsx", start_date, end_date)
    metadata = get_metadata(metadata, table_names, filter)

    # Initialisation du rate limiter global et par endpoint
    rate_limiter = EndpointRateLimiter()

    shared_vars_manager = MemoryAccess()
    shared_vars_manager.write("metadata", metadata)
    shared_vars_manager.write("is_exception", is_exception)

    # Initialisation des clients
    delta_days = 1
    samsara_client = SamsaraClient(api_token=samsara_api_token, rate_limiter=rate_limiter, shared_vars_manager=shared_vars_manager,delta_days=delta_days)
    gcs_client = GCSClient(bucket_name=gcs_bucket_name)


    # Liste des tâches à exécuter
    tasks = []
    for index, row in metadata.iterrows():
        index += 1
        if bool(row.get('is_processed', False)):
            standard_logger.info(f"La ligne {index} est marquée comme déjà traitée, saut")
            continue
        if not row.get('is_exception') and is_exception:
            standard_logger.info(f"La ligne {index} n'est pas marquée comme une exception, elle sera traitée lors de l'execution normale")
            continue

        endpoint_info = row.to_dict()
        if not is_exception:
            if iteration > 0:
                params = endpoint_info.get('params')
                params = params if not pd.isna(params) else {}
                try:
                    params = process_params(params)
                    if get_start_end_date_config(params) is None:
                        standard_logger.info(f"la ligne {index} a ete déjà traitée lors de la premiere itération")
                        continue
                except Exception as e:
                    standard_logger.error(
                        f"Erreur lors de la conversion des paramètres pour {endpoint_info.get('table_name')}: {e}")
                    continue
            if row.get('is_exception', True):
                standard_logger.info(f"La ligne {index} est marquée comme une exception, elle sera traitée pendant l'execution des exceptions")
                continue
        data_fetcher = DataFetcher(samsara_client, gcs_client, endpoint_info, max_workers=max_workers)
        tasks.append(data_fetcher)

        # Exécution des tâches en parallèle
    parallelize_execution(tasks=tasks, func="fetch_and_upload", logger=standard_logger, max_workers=max_workers)


def load_to_bigquery(table_names=None, last_update_str_date: str|None = None):
    def get_last_update_date(last_update_str_date: str|None):
        if last_update_str_date is not None:
            return datetime.strptime(last_update_str_date, "%d/%m/%Y")
        return datetime.now()

    GCSBigQueryLoader(
        bucket_name=gcs_bucket_name,
        dataset_id=database_id,
        table_names=table_names,
        _from=get_last_update_date(last_update_str_date)
    ).run()


def upload_logs():
    gcs_client = GCSClient(bucket_name=gcs_bucket_name)
    local_logs_path = LOGS_DIR
    for file in os.listdir(local_logs_path):
        if file.endswith(".log"):
            buffer = file_buffer(open(os.path.join(local_logs_path,file), "rb").read())
            destination = gcs_client.bucket_manager.gcs_log_path + "/" + file
            gcs_client.upload_bytes(buffer, destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Téléchargement des données Samsara et chargement dans BigQuery")
    parser.add_argument("--start_date", type=str, help="Date de debut pour la récupération des données format: jj/mm/aaaa")
    parser.add_argument("--end_date", type=str, help="Date de fin pour la récupération des données format: jj/mm/aaaa")
    parser.add_argument("--table_file_path", type=str, help="Chemin du fichier contenant les noms des tables à traiter, si == ALL, toutes les tables seront traitées")
    parser.add_argument("--max_workers", type=int, help="Nombre de requêtes à traiter en parallèle")
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date
    table_file_path = args.table_file_path

    if table_file_path == "ALL":
        table_names = None
    elif table_file_path is not None and os.path.isfile(table_file_path):
        df = pd.read_excel(table_file_path)
        table_names = df.iloc[:, 0].tolist()
    else:
        table_names = []

    gcs_client = GCSClient(bucket_name=gcs_bucket_name)
    if end_date is None:
        end_date = datetime.now().strftime("%d/%m/%Y")
    if start_date is None:
        try:
            config = gcs_client.get_config()
            start_date = config.get("last_update_date")
        except Exception as e:
            msg = f"Erreur lors de la récupération de la date de début: {e}, La date de debut par défaut 01/01/2021 sera utilisée"
            print(msg)
            standard_logger.warning(msg)
            start_date = "01/01/2021"

    max_workers = args.max_workers

    dates = [
        {"start_date": start_date, "end_date": end_date},
    ]
    for index, date in enumerate(dates):
        start = datetime.now()
        scrape_samsara_to_gcs(
            start_date=start_date,
            end_date=end_date,
            iteration=index,
            table_names=table_names,
            max_workers=max_workers
        )
        end = datetime.now()
        td = (end - start).total_seconds()
        standard_logger.info(f"Temps d'exécution {index} : {td} secondes pour la période {date['start_date']} - {date['end_date']}")
        print(f"Temps d'exécution {index}: {td} secondes pour la période {date['start_date']} - {date['end_date']}")

    filters = ["is_exception"]
    for index, filter in enumerate(filters):
        start = datetime.now()
        scrape_samsara_to_gcs(start_date, end_date, index, is_exception=True, filter=filter, table_names=table_names, max_workers=max_workers)
        end = datetime.now()
        td = (end - start).total_seconds()
        standard_logger.info(f"Temps d'exécution exception '{filter}': {td} secondes pour la période '{start_date}' - '{end_date}'")
        print(f"Temps d'exécution exception '{filter}': {td} secondes pour la période '{start_date}' - '{end_date}'")

    upload_missing_files(table_names, start_date, end_date, max_workers)

    load_to_bigquery(table_names=table_names, last_update_str_date=start_date)

    upload_logs()

    gcs_client.update_config({"last_update_date": end_date})
