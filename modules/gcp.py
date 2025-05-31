import json
import os
import re
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from io import BytesIO
from collections import defaultdict

from .interface import SearchRetrieveType
from .logs import MyLogger
from .metadata import get_metadata, TypeFilter
from .raters import MemoryAccess
from .transformation_configs import MAPPING_TABLES, REVERSED_MAPPING_TABLES

from .utils import make_path, parallelize_execution, extract_date_range, pandas_to_bq_schema


class BucketManager:

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket: storage.bucket.Bucket = self.storage_client.bucket(self.bucket_name)
        self.logger = MyLogger("BucketManager")
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.tmp_folder = make_path(os.path.join(parent_dir, "resources", "tmp"))
        # self.folder_pattern = r"/([^/]+)/\1_(\d{4}_\d{2}_\d{2})(?:_to_(\d{4}_\d{2}_\d{2}))?(?:_(\d+))?\.parquet"
        self.file_path_regex = re.compile(
            r"^(?P<family>[^/]+)/"
            r"(?P<main_family>[^/]+)/"
            r"(?P<table_name>.+?)_"
            r"(?P<start_date>\d{4}_\d{2}_\d{2})"
            r"(?:_to_(?P<end_date>\d{4}_\d{2}_\d{2}))?"
            r"(?:_(?P<index>\d+))?"
            r"\.parquet$"
        )
        self.gcs_log_path = "resources/logs" #os.path.join("resources", "logs")
        self.gcs_config_path = "resources/configs" #os.path.join("resources", "configs")

    def delete_file(self, blob_name: str) -> None:
        """
        Supprime un fichier du bucket.
        :param blob_name: Nom du blob à supprimer
        :return: None
        """
        blob_exists, blob = self.file_exists(blob_name)
        if blob_exists:
            blob.delete()
            self.logger.info(f"Fichier {blob_name} supprimé avec succès.")
        else:
            self.logger.warning(f"Le fichier {blob_name} n'existe pas dans le bucket.")

    def file_exists(self, blob_name: str) -> tuple[bool, storage.blob.Blob]:
        """
        Vérifie si un fichier existe déjà dans le bucket.
        :param blob_name: Nom du blob à vérifier
        :return: True si le fichier existe, False sinon
        """
        blob = self.bucket.blob(blob_name)
        return blob.exists(), blob

    def get_unique_blob_name(self, destination_blob_name: str) -> str:
        """
        Renomme un fichier en ajoutant un index s'il existe déjà.
        :param destination_blob_name: Nom du blob cible
        :return: Nom unique pour le blob
        """
        base_name, ext = os.path.splitext(destination_blob_name)
        index = 1
        unique_name = destination_blob_name

        while self.file_exists(unique_name)[0]:
            unique_name = f"{base_name}_{index}{ext}"
            index += 1

        return unique_name
    def upload_bytes(self, buffer: BytesIO, destination_blob_name: str, allow_unique_name: bool = False, delete_if_exists=False) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param buffer: buffer contenant les données
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
        if delete_if_exists:
            self.delete_file(destination_blob_name)
            self.logger.info(f"Fichier {destination_blob_name} existe déjà, il a été supprimé avant le téléchargement.")
        if allow_unique_name:
            destination_blob_name = self.get_unique_blob_name(destination_blob_name)
        self.logger.info(f"Téléchargement des données vers gs://{self.bucket_name}/{destination_blob_name}")
        blob: storage.blob.Blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_file(
            buffer,
            content_type='application/octet-stream'
        )
        self.logger.info(f"Données téléchargées vers {destination_blob_name} dans le bucket {self.bucket_name}")

    def list_parquet_files(self) -> list[str]:
        """Lister les fichiers Parquet dans le bucket GCS."""
        self.logger.info(f"Listing Parquet files in bucket: {self.bucket_name}")
        files = [blob.name for blob in self.bucket.list_blobs() if blob.name.endswith(".parquet")]
        self.logger.debug(f"Found files: {len(files)}.........")
        return files

    def parse_file_path(self, file_path: str, retrieve_type: SearchRetrieveType) -> str | None:
        match = self.file_path_regex.match(file_path)
        if not match:
            return None
        return match.groupdict().get(retrieve_type.value)

    def get_table_name(self, file_path: str) -> str | None:
        """Extraire le nom de la table du chemin d'accès."""
        # Extraction
        return self.parse_file_path(file_path, SearchRetrieveType.TABLE_NAME)

    def get_start_date(self, file_path: str) -> datetime | None:
        """Extraire la date de début du chemin d'accès."""
        # Extraction
        date = self.parse_file_path(file_path, SearchRetrieveType.DATE_START)
        if date:
            start_date = datetime.strptime(date, "%Y_%m_%d")
            return start_date
        return None
    def get_end_date(self, file_path: str) -> datetime|None:
        """Extraire la date de fin du chemin d'accès."""
        # Extraction
        date = self.parse_file_path(file_path, SearchRetrieveType.DATE_END)
        if date:
            end_date = datetime.strptime(date, "%Y_%m_%d")
            return end_date
        return None

    def get_start_end_date(self, file_path: str) -> tuple[datetime, datetime] | None:
        """Extraire la date de début et de fin du chemin d'accès."""
        # Extraction
        start_date = self.get_start_date(file_path)
        end_date = self.get_end_date(file_path)
        if start_date and end_date:
            return start_date, end_date
        return None

    def download_file(self, source_blob_name, destination_file_name, destination_folder=None):
        """
        Télécharge un fichier .parquet depuis un bucket GCS.

        Args:
            source_blob_name (str): Chemin complet du fichier dans le bucket.
            destination_file_name (str): Chemin local où le fichier sera enregistré.
            destination_folder (str, optional): Dossier de destination. Defaults to None.
        """
        destination_file_name = os.path.join(self.tmp_folder if destination_folder is None else destination_folder, destination_file_name)
        # Accède au fichier (blob) à télécharger
        blob = self.bucket.blob(source_blob_name)

        # Télécharge le fichier
        blob.download_to_filename(destination_file_name)
        self.logger.info(f"Fichier {source_blob_name} téléchargé dans {destination_file_name}.")

    def missing_files(self, configs_for_update: dict, table_names: list[str], end_date: datetime, start_date: datetime | None = None) -> dict[str, list[datetime]]:
        """Vérifie si des fichiers sont manquants dans le bucket."""
        # Récupérer les fichiers Parquet déjà présents dans le bucket
        if start_date is None:
            start_date = datetime.strptime("2021-01-01", "%Y-%m-%d")

        files = self.list_parquet_files()
        # Grouper les fichiers par table (en fonction du chemin)
        current_dates = defaultdict(list)
        missing_dates = defaultdict(list)
        for file_path in files:
            table_name = self.get_table_name(file_path)
            file_name = file_path.split("/")[-1]
            if all([
                MAPPING_TABLES.get(table_name, table_name) in table_names,
                (date_range := extract_date_range(file_name)),
                configs_for_update.get(MAPPING_TABLES.get(table_name, table_name), {}).get("download_type", "time") == "time"
            ]):
                if len(date_range) == 1:
                    current_dates[table_name].append(date_range[0])
                else:
                    _start_date = date_range[0] #datetime.strptime(date_range[0], "%Y_%m_%d")
                    _end_date = date_range[1] #datetime.strptime(date_range[1], "%Y_%m_%d")
                    current_dates[table_name].extend([_start_date + timedelta(days=i) for i in range((_end_date - _start_date).days)])

        tables_to_map = defaultdict(list)
        # Vérifier les dates manquantes
        for table_name, dates in current_dates.items():
            if main_table_name := MAPPING_TABLES.get(table_name):
                tables_to_map[main_table_name].append(table_name)
            if config_for_update := configs_for_update.get(MAPPING_TABLES.get(table_name, table_name), {}):
                start_date = config_for_update.get("last_update_time", start_date)
                start_date = datetime.strptime(start_date, "%d/%m/%Y") if isinstance(start_date, str) else start_date

            for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]:
                if date not in dates:
                    missing_dates[table_name].append(date)

        for main_table_name, sub_tables in tables_to_map.items():
            _missing_dates = []
            for sub_table in sub_tables:
                if sub_table in missing_dates:
                    _missing_dates.extend(missing_dates.pop(sub_table))
            if _missing_dates:
                missing_dates[sub_tables[0]].extend(list(set(_missing_dates)))

        self.logger.info(f"Total des fichiers manquants entre {start_date} et {end_date} : {len(missing_dates.values())} fichiers")
        return missing_dates



class BigQueryManager:

    def __init__(self, dataset_id: str, **kwargs):

        self.bigquery_client = bigquery.Client()
        self.dataset_id = dataset_id
        self.logger = MyLogger("BigQueryManager")
        self.memory_manager: MemoryAccess | None = kwargs.get("memory_manager")
        self.partition_expiration_days: int = kwargs.get("partition_expiration_days", 30)  # Durée de conservation des partitions

    def load_parquet_to_bigquery(self, uri: list[str], table_name: str) -> None:
        """Charger un fichier Parquet de GCS vers BigQuery."""
        table_id = f"{self.bigquery_client.project}.{self.dataset_id}.{table_name}"

        # Supprimer la table si elle existe déjà
        # self.logger.info(f"Deleting existing table (if any): {table_id}")
        # self.bigquery_client.delete_table(table_id, not_found_ok=True)

        # Vérifier et créer le dataset s'il n'existe pas
        try:
            self.bigquery_client.get_dataset(self.dataset_id)  # Vérifier si le dataset existe
        except NotFound:
            self.logger.info(f"le Dataset '{self.dataset_id}' n'existe pas, création en cours...")
            dataset = bigquery.Dataset(f"{self.bigquery_client.project}.{self.dataset_id}")
            dataset.location = "EU"  # Spécifiez la région de votre choix
            self.bigquery_client.create_dataset(dataset, exists_ok=True)
            self.logger.info(f"Dataset '{self.dataset_id}' created successfully.")

        # gestion du cas où on charge tous les fichiers d'un dossier il faut supprimer la table afin de ne pas avoir de doublon
        if len(uri) == 1 and uri[0].endswith("/*"):
            self.logger.info(f"Deleting existing table (if any): {table_id} before loading files from {uri[0]}")
            self.bigquery_client.delete_table(table_id, not_found_ok=True)

        # Configurer la source GCS et l'opération de chargement
        default_write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        extra_config = {}
        if self.memory_manager is not None:
            metadata: pd.DataFrame = self.memory_manager.read("metadata")
            schema = None # self.get_table_schema(table_name)
            if schema:
                extra_config.update({"schema": schema})
            else:
                extra_config.update({"autodetect": True})
            metadata = metadata[metadata["table_name"] == MAPPING_TABLES.get(table_name, table_name)]
            if not metadata.empty:
                metadata = metadata.iloc[0]
                if not pd.isnull(time_partitioning_field := metadata.get("time_partitioning_field")):
                    extra_config.update({
                        "time_partitioning": bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=time_partitioning_field,
                            expiration_ms=int(timedelta(days=self.partition_expiration_days).total_seconds() * 1000),
                        )
                    })
                if not pd.isnull(clustering_fields := metadata.get("clustering_fields")):
                    extra_config.update({
                        "clustering_fields": clustering_fields if isinstance(clustering_fields, list) else [clustering_fields]
                    })

                default_write_disposition = metadata.get("download_type", "time") == "oneshot" and bigquery.WriteDisposition.WRITE_TRUNCATE or bigquery.WriteDisposition.WRITE_APPEND
            # print(f"extra_config: {extra_config} - table_name: {table_name}")
        if default_write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
            extra_config.update({
                "schema_update_options": [
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
                ]
            })

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=default_write_disposition,
            **extra_config
        )

        # Charger le fichier Parquet dans BigQuery
        self.logger.info(f"Loading files into BigQuery table '{table_name}' from URIs: {uri}")
        try:
            load_job = self.bigquery_client.load_table_from_uri(list(uri), table_id, job_config=job_config)
              # Attendre la fin du job
            if load_job.result():
                self.logger.info(f"Table '{table_name}' successfully updated in BigQuery with files from {uri}.")
        except Exception as e:
            self.logger.error(f"Error loading files into BigQuery table '{table_name}': {e}")

    def get_table_schema(self, table_name: str) -> list[bigquery.SchemaField] | None:
        """
        Récupère le schéma d'une table BigQuery.
        :param table_name: Nom de la table
        :return: Liste des champs du schéma
        """
        metadata: pd.DataFrame = self.memory_manager.read("metadata")
        metadata = metadata[metadata["table_name"] == MAPPING_TABLES.get(table_name, table_name)]
        time_col = metadata.iloc[0].get("time_partitioning_field", None)
        bucket_manager: BucketManager = self.memory_manager.read("bucket_manager")
        all_file_paths = bucket_manager.list_parquet_files()
        downloaded_files = []
        for file_path in all_file_paths:
            file_name = file_path.split("/")[-1]
            __table_name = bucket_manager.get_table_name(file_path)
            if table_name == __table_name:
                bucket_manager.download_file(file_path, file_name)
                downloaded_files.append(os.path.join(bucket_manager.tmp_folder, file_name))
                break

        for file_path in downloaded_files:
            try:
                df = pd.read_parquet(file_path)

                schema = pandas_to_bq_schema(df, time_col=time_col)
                # delete_file(file_path)
                return schema
            except Exception as e:
                self.logger.error(f"Erreur lors de la récupération du schéma pour la table {table_name}: {e}")
                return None


class GCSClient:
    """
    Cette classe encapsule les opérations de téléchargement de données vers Google Cloud Storage.
    """

    def __init__(self, bucket_name: str):
        self.bucket_manager = BucketManager(bucket_name)

    def upload_bytes(self, buffer: BytesIO, destination_blob_name: str, allow_unique_name: bool = False) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param buffer: buffer contenant les données
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
        try:
            self.bucket_manager.upload_bytes(buffer, destination_blob_name, allow_unique_name)
        except Exception as e:
            self.bucket_manager.logger.error(f"erreur lors de la migration des données vers GCS: {e}")

    def _upload_dict(self, data: dict, destination_blob_name: str) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param data: Données à télécharger
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
        buffer = BytesIO(json.dumps(data, indent=4).encode())
        self.upload_bytes(buffer, destination_blob_name)

    def _get_config(self, filename: str) -> dict:
        """
        Télécharge les configurations à partir d'un fichier JSON dans un bucket GCS.
        :return: Dictionnaire de configurations
        """
        blob_name = f"{self.bucket_manager.gcs_config_path}/{filename}.json"
        blob_exists, blob = self.bucket_manager.file_exists(blob_name)
        if not blob_exists:
            self.bucket_manager.logger.warning(f"Le fichier de configuration est introuvable dans le bucket.")
            # raise FileNotFoundError(f"Le fichier de configuration est introuvable dans le bucket.")
            return {}

        return json.loads(blob.download_as_string())

    def get_configs_for_update(self) -> dict:
        """
        configs_for_update = {
            "fleet_devices": {
                "download_type": "one_shot",
            },
            "fleet_vehicles_fuel_energy": {
                "download_type": "time",
                "last_update_date": start_date
            },
        }
        """
        return self._get_config("configs_for_update")

    def _update_config(self, data: dict, filename: str, erase=False) -> None:
        """
        Met à jour les configurations dans un fichier JSON dans un bucket GCS.
        :param data: Dictionnaire de configurations
        :param erase: Effacer les configurations existantes
        :return: None
        """
        blob_name = f"{self.bucket_manager.gcs_config_path}/{filename}.json"
        if not erase:
            config = self.get_configs_for_update()
            config.update(data)
            data = config
        # self.bucket_manager.delete_file(blob_name)
        self._upload_dict(data, blob_name)

    def update_configs_for_update(self, metadata: pd.DataFrame, end_time: str) -> None:
        data = defaultdict(dict)
        for _, row in metadata.iterrows():
            data[row.get("table_name")].update({
                "download_type": row.get("download_type", "time"),
                "last_update_time": end_time
            })
        self._update_config(data, "configs_for_update", erase=False)

class GCSBigQueryLoader:
    def __init__(self, bucket_name: str, dataset_id: str, **kwargs):
        # Initialiser les clients et les paramètres
        self.bucket_name: str = bucket_name
        self.dataset_id: str = dataset_id
        self.bucket_manager: BucketManager = BucketManager(bucket_name)
        self.bigquery_manager: BigQueryManager = BigQueryManager(dataset_id, **kwargs)
        # Configurer le logger
        self.logger: MyLogger = MyLogger("GCSBigQueryLoader", with_console=False)
        self._from: datetime | None = kwargs.get("_from", None)
        self._to: datetime | None = kwargs.get("_to", None)
        self.max_workers = 2
        self.memory_manager: MemoryAccess | None = kwargs.get("memory_manager")

    def run(self, configs_for_update: dict, table_names: list[str] | None = None) -> None:
        """Exécuter le processus de chargement pour tous les fichiers Parquet."""
        files = self.bucket_manager.list_parquet_files()
        # Grouper les fichiers par table (en fonction du chemin)
        table_to_paths = defaultdict(list)
        for file_path in files:
            table_name = self.bucket_manager.get_table_name(file_path)
            if table_names and MAPPING_TABLES.get(table_name, table_name) not in table_names:
                continue
            start_date = self.bucket_manager.get_start_date(file_path)
            if self._from:
                if start_date and start_date < self._from:
                    continue
            if self._to:
                end_date = self.bucket_manager.get_end_date(file_path) or start_date
                if end_date and end_date > self._to:
                    continue
            if configs := configs_for_update.get(MAPPING_TABLES.get(table_name, table_name), {}):
                if last_update_time := configs.get("last_update_time"):
                    last_update_date = datetime.strptime(last_update_time, "%d/%m/%Y")
                    if start_date < last_update_date:
                        continue

            table_to_paths[table_name].append(file_path)
            # folder_path = "/".join(file_path.split("/")[:-1])  # Extraire le chemin sans le fichier
            # table_to_paths[table_name].append(f"gs://{self.bucket_name}/{folder_path}/*")

        # tasks = [{'uri': set(uris), 'table_name': table_name} for table_name, uris in table_to_paths.items()]
        tasks = []
        for table_name, file_paths in table_to_paths.items():
            # if configs := configs_for_update.get(MAPPING_TABLES.get(table_name, table_name), {}):
            #     if configs.get("download_type", None) == "oneshot":
            #         self.bigquery_manager.bigquery_client.delete_table(table_name, not_found_ok=True)
            # si supérieur à 100 fichiers, on suppose qu'il ne s'agit pas d'une MAJ mais d'une première insertion donc on regroupe par dossier
            # TODO: Point of failure, si plus de 50 fichiers, risque de charger toutes les données avec l'historique
            if len(file_paths) > 50:
                file_path = file_paths[0]
                folder_path = "/".join(file_path.split("/")[:-1])
                uris = [f"gs://{self.bucket_name}/{folder_path}/*"]
                tasks.append({'uri': list(set(uris)), 'table_name': table_name})
            else:
                uris = [f"gs://{self.bucket_name}/{file_path}" for file_path in file_paths]
                tasks.append({'uri': list(set(uris)), 'table_name': table_name})
                # tasks.extend([{'uri': f"gs://{self.bucket_name}/{file_path}", 'table_name': table_name} for file_path in uris])

        parallelize_execution(
            tasks=tasks,
            func=self.bigquery_manager.load_parquet_to_bigquery,
            logger=self.logger,
            max_workers=self.max_workers
        )

