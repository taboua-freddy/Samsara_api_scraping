import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from typing import Literal
import os
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile

import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

from .interface import SearchRetrieveType, ColumnToUpdate, DownloadType
from .logs import MyLogger
from .raters import MemoryAccess
from .transformation import TransformData
from .transformation_configs import MAPPING_TABLES
from .utils import (
    make_path,
    parallelize_execution,
    extract_date_range,
    pandas_to_bq_schema, DEFAULT_START_DATE, parquet_buffer, extract_suffixe, TMP_DIR, CustomNamedTemporaryFile,
)


class BucketManager:

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.logger = MyLogger("BucketManager")
        self.storage_client = storage.Client()
        self._create_bucket()
        self.bucket: storage.bucket.Bucket = self.storage_client.bucket(
            self.bucket_name
        )
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
        self.gcs_log_path = "resources/logs"  # os.path.join("resources", "logs")
        self.gcs_config_path = (
            "resources/configs"  # os.path.join("resources", "configs")
        )

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

    def upload_bytes(
            self,
            buffer: BytesIO,
            destination_blob_name: str,
            allow_unique_name: bool = False,
            delete_if_exists=False,
    ) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param buffer: buffer contenant les données
        :param destination_blob_name: chemin du blob de destination
        :param allow_unique_name: Si True, le nom du blob sera unique (ajout d'un index si nécessaire)
        :param delete_if_exists: Si True, supprime le fichier s'il existe déjà
        :return: None
        """

        if delete_if_exists:
            self.delete_file(destination_blob_name)
            self.logger.info(
                f"Fichier {destination_blob_name} existe déjà, il a été supprimé avant le téléchargement."
            )
        if allow_unique_name:
            destination_blob_name = self.get_unique_blob_name(destination_blob_name)
        self.logger.info(
            f"Téléchargement des données vers gs://{self.bucket_name}/{destination_blob_name}"
        )
        blob: storage.blob.Blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_file(buffer, content_type="application/octet-stream")
        self.logger.info(
            f"Données téléchargées vers {destination_blob_name} dans le bucket {self.bucket_name}"
        )

    def list_parquet_files(self, input_folder: str = None) -> list[str]:
        """Lister les fichiers Parquet dans le bucket GCS."""
        self.logger.info(f"Listing Parquet files in bucket: {self.bucket_name}")
        files = [
            blob.name
            for blob in self.bucket.list_blobs(prefix=input_folder)
            if blob.name.endswith(".parquet")
        ]
        self.logger.debug(f"Found files: {len(files)} Parquet files")
        return files

    def parse_file_path(
            self, file_path: str, retrieve_type: SearchRetrieveType
    ) -> str | None:
        """
        Extrait une information spécifique d'un chemin de fichier à l'aide d'une expression régulière.

        :param file_path: Chemin du fichier à analyser (ex: 'fleet/vehicles/fleet_vehicles_2024_06_01.parquet')
        :param retrieve_type: Type d'information à extraire (valeur de l'énumération SearchRetrieveType)
        :return: La valeur extraite correspondant au type demandé (ex: nom de la table, date de début, etc.), ou None si le format ne correspond pas
        """
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

    def get_end_date(self, file_path: str) -> datetime | None:
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

    def download_file(
            self, source_blob_name: str, destination_file_name: str, destination_folder: str = None
    ):
        """
        Télécharge un fichier .parquet depuis un bucket GCS.

        Args:
            source_blob_name (str): Chemin complet du fichier dans le bucket.
            destination_file_name (str): Chemin local où le fichier sera enregistré.
            destination_folder (str, optional): Dossier de destination. Defaults to None.
        """
        destination_file_name = os.path.join(
            self.tmp_folder if destination_folder is None else destination_folder,
            destination_file_name,
        )
        # Accède au fichier (blob) à télécharger
        blob = self.bucket.blob(source_blob_name)

        # Télécharge le fichier
        blob.download_to_filename(destination_file_name)
        self.logger.info(
            f"Fichier {source_blob_name} téléchargé dans {destination_file_name}."
        )

    def missing_dates(
            self,
            metadata: pd.DataFrame,
            configs_for_update: dict,
            end_date: datetime,
            start_date: datetime | None = None,
    ) -> dict[str, list[datetime]]:
        """Vérifie si des fichiers sont manquants dans le bucket."""
        # Récupérer les fichiers Parquet déjà présents dans le bucket
        if start_date is None:
            start_date = datetime.strptime(DEFAULT_START_DATE, "%d/%m/%Y")

        # Grouper les fichiers par table (en fonction du chemin)
        current_dates = defaultdict(list)
        missing_dates = defaultdict(list)

        for _, row in metadata.iterrows():
            table_name = row.get("table_name")
            if not table_name:
                continue
            family = row.get("family")
            if not family:
                continue
            input_folder = f"{family}/{table_name}"
            if not pd.isnull(row.get("download_type", None)) and row.get("download_type") == DownloadType.ONESHOT.value:
                continue

            files = self.list_parquet_files(input_folder=input_folder)
            if not files:
                self.logger.warning(
                    f"Aucun fichier trouvé pour la table {table_name} dans le bucket {self.bucket_name}."
                )
                continue

            # recuperation des dates à partir des noms de fichiers
            for file_path in files:
                file_name = file_path.split("/")[-1]
                if MAPPING_TABLES.get(table_name, table_name) == table_name and (
                        date_range := extract_date_range(file_name)):
                    if len(date_range) == 1:
                        current_dates[table_name].append(date_range[0])
                    else:
                        _start_date = date_range[0]
                        _end_date = date_range[1]
                        current_dates[table_name].extend(
                            [
                                _start_date + timedelta(days=i)
                                for i in range((_end_date - _start_date).days)
                            ]
                        )

        tables_to_map = defaultdict(list)
        # Vérifier les dates manquantes
        for table_name, dates in current_dates.items():
            if main_table_name := MAPPING_TABLES.get(table_name):
                tables_to_map[main_table_name].append(table_name)
            if config_for_update_ := configs_for_update.get(
                    MAPPING_TABLES.get(table_name, table_name), {}
            ):
                # Privilegier la date de début du fichier de configuration au lieu de la date par défaut
                start_date = config_for_update_.get(ColumnToUpdate.DOWNLOAD.value, start_date)
                start_date = (
                    datetime.strptime(start_date, "%d/%m/%Y")
                    if isinstance(start_date, str)
                    else start_date
                )
            # Récupération des dates où il n'y a pas de données depuis le fichier de configuration
            dates_with_no_data = [datetime.strptime(date, "%d/%m/%Y") for date in
                                  config_for_update_.get(ColumnToUpdate.DATE_NO_DATA.value, [])]
            dates_ = list(set(dates + dates_with_no_data))
            # Calculer les dates manquantes entre start_date et end_date
            for date in [
                start_date + timedelta(days=i)
                for i in range((end_date - start_date).days)
            ]:
                if date not in dates_:
                    missing_dates[table_name].append(date)

        # Regroupement des sous-tables sous le label de la table principale
        # par exemple fleet_tag_vehicles, fleet_tag_drivers sous fleet_tags qui est connu dans les metadata
        for main_table_name, sub_tables in tables_to_map.items():
            _missing_dates = []
            for sub_table in sub_tables:
                if sub_table in missing_dates:
                    _missing_dates.extend(missing_dates.pop(sub_table))
            if _missing_dates:
                missing_dates[sub_tables[0]].extend(list(set(_missing_dates)))

        self.logger.info(
            f"Total des fichiers manquants entre {start_date} et {end_date} : {sum(len(dates) for dates in missing_dates.values())}"
        )
        return missing_dates

    def _create_bucket(self) -> None:
        """
        Crée un bucket GCS si il n'existe pas.
        :return: None
        """
        try:
            self.storage_client.create_bucket(self.bucket_name, location="EU")
            self.logger.info(f"Bucket {self.bucket_name} created successfully.")
        except Exception as e:
            if "already exists" in str(e):
                self.logger.info(f"Bucket {self.bucket_name} already exists.")
            else:
                self.logger.error(f"Error creating bucket {self.bucket_name}: {e}")


class BigQueryManager:

    def __init__(self, dataset_id: str, **kwargs):

        self.bigquery_client = bigquery.Client()
        self.dataset_id = dataset_id
        self.logger = MyLogger("BigQueryManager")
        self.memory_manager: MemoryAccess | None = kwargs.get("memory_manager")
        self.partition_expiration_days: int = kwargs.get(
            "partition_expiration_days", 365 * 4
        )  # Durée de conservation des partitions
        self.partition_expiration_ms = int(timedelta(days=self.partition_expiration_days).total_seconds() * 1000)

    def load_parquet_to_bigquery(self, uri: list[str], table_name: str) -> None:
        """Charger un fichier Parquet de GCS vers BigQuery."""
        table_id = f"{self.bigquery_client.project}.{self.dataset_id}.{table_name}"

        # Supprimer la table si elle existe déjà
        # self.logger.info(f"Deleting existing table (if any): {table_id}")
        # self.bigquery_client.delete_table(table_id, not_found_ok=True)

        # Vérifier et créer le dataset s'il n'existe pas
        try:
            self.bigquery_client.get_dataset(
                self.dataset_id
            )  # Vérifier si le dataset existe
        except NotFound:
            self.logger.info(
                f"le Dataset '{self.dataset_id}' n'existe pas, création en cours..."
            )
            dataset = bigquery.Dataset(
                f"{self.bigquery_client.project}.{self.dataset_id}"
            )
            dataset.location = "EU"  # Spécifiez la région de votre choix
            self.bigquery_client.create_dataset(dataset, exists_ok=True)
            self.logger.info(f"Dataset '{self.dataset_id}' created successfully.")

        # gestion du cas où on charge tous les fichiers d'un dossier il faut supprimer la table afin de ne pas avoir de doublon
        if len(uri) == 1 and uri[0].endswith("/*"):
            self.logger.info(
                f"Deleting existing table (if any): {table_id} before loading files from {uri[0]}"
            )
            self.bigquery_client.delete_table(table_id, not_found_ok=True)

        # Configurer la source GCS et l'opération de chargement
        default_write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        extra_config = {}
        if self.memory_manager is not None:
            metadata: pd.DataFrame = self.memory_manager.read("metadata")
            schema = None  # self.get_table_schema(table_name)
            if schema:
                extra_config.update({"schema": schema})
            else:
                extra_config.update({"autodetect": True})
            metadata = metadata[
                metadata["table_name"] == MAPPING_TABLES.get(table_name, table_name)
                ]
            if not metadata.empty:
                metadata = metadata.iloc[0]
                if not pd.isnull(
                        time_partitioning_field := metadata.get("time_partitioning_field")
                ):
                    extra_config.update(
                        {
                            "time_partitioning": bigquery.TimePartitioning(
                                type_=bigquery.TimePartitioningType.DAY,
                                field=time_partitioning_field,
                                # expiration_ms=self.partition_expiration_ms,
                            )
                        }
                    )
                if not pd.isnull(
                        clustering_fields := metadata.get("clustering_fields")
                ):
                    extra_config.update(
                        {
                            "clustering_fields": (
                                clustering_fields
                                if isinstance(clustering_fields, list)
                                else [clustering_fields]
                            )
                        }
                    )

                default_write_disposition = (
                        metadata.get("download_type", "time") == DownloadType.ONESHOT.value
                        and bigquery.WriteDisposition.WRITE_TRUNCATE
                        or bigquery.WriteDisposition.WRITE_APPEND
                )
            # print(f"extra_config: {extra_config} - table_name: {table_name}")
        if default_write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
            extra_config.update(
                {
                    "schema_update_options": [
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    ]
                }
            )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=default_write_disposition,
            **extra_config,
        )

        # Charger le fichier Parquet dans BigQuery
        self.logger.info(
            f"Loading files into BigQuery table '{table_name}' from URIs: {uri}"
        )
        try:
            load_job = self.bigquery_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            # Attendre la fin du job
            result = load_job.result()
            if result.errors:
                self.logger.error(
                    f"Errors occurred while loading files into BigQuery table '{table_name}': {load_job.result().errors}"
                )
            if result.done() > 0:
                self.logger.info(
                    f"Table '{table_name}' successfully updated in BigQuery with files from {uri}."
                )
        except Exception as e:
            self.logger.error(
                f"Error loading files into BigQuery table '{table_name}': {e}"
            )

    def get_table_schema(self, table_name: str) -> list[bigquery.SchemaField] | None:
        """
        Récupère le schéma d'une table BigQuery.
        :param table_name: Nom de la table
        :return: Liste des champs du schéma
        """
        metadata: pd.DataFrame = self.memory_manager.read("metadata")
        metadata = metadata[
            metadata["table_name"] == MAPPING_TABLES.get(table_name, table_name)
            ]
        time_col = metadata.iloc[0].get("time_partitioning_field", None)
        bucket_manager: BucketManager = self.memory_manager.read("bucket_manager")
        all_file_paths = bucket_manager.list_parquet_files()
        downloaded_files = []
        for file_path in all_file_paths:
            file_name = file_path.split("/")[-1]
            __table_name = bucket_manager.get_table_name(file_path)
            if table_name == __table_name:
                bucket_manager.download_file(file_path, file_name)
                downloaded_files.append(
                    os.path.join(bucket_manager.tmp_folder, file_name)
                )
                break

        for file_path in downloaded_files:
            try:
                df = pd.read_parquet(file_path)

                schema = pandas_to_bq_schema(df, time_col=time_col)
                # delete_file(file_path)
                return schema
            except Exception as e:
                self.logger.error(
                    f"Erreur lors de la récupération du schéma pour la table {table_name}: {e}"
                )
                return None


class GCSClient:
    """
    Cette classe encapsule les opérations de téléchargement de données vers Google Cloud Storage.
    """
    _target_bucket_manager: BucketManager | None = None

    def __init__(self, bucket_name: str):
        self.bucket_manager = BucketManager(bucket_name)

    @property
    def target_bucket_manager(self) -> BucketManager:
        if self._target_bucket_manager is None:
            raise ValueError("Le BucketManager n'est pas initialisé.")
        return self._target_bucket_manager

    @target_bucket_manager.setter
    def target_bucket_manager(self, value: BucketManager):
        if not isinstance(value, BucketManager):
            raise TypeError("target_bucket_manager doit être une instance de BucketManager.")
        self._target_bucket_manager = value

    def upload_bytes(
            self,
            buffer: BytesIO,
            destination_blob_name: str,
            allow_unique_name: bool = False,
    ) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param buffer: buffer contenant les données
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
        try:
            self.bucket_manager.upload_bytes(
                buffer, destination_blob_name, allow_unique_name
            )
        except Exception as e:
            self.bucket_manager.logger.error(
                f"erreur lors de la migration des données vers GCS: {e}"
            )

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
            self.bucket_manager.logger.warning(
                f"Le fichier de configuration est introuvable dans le bucket."
            )
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

    def update_configs_for_update(self, metadata: pd.DataFrame, end_time: str, col_to_update: ColumnToUpdate,
                                  **kwargs) -> None:
        """
        Met à jour les configurations pour les tables dans un fichier JSON dans un bucket GCS.

        Args:
            metadata (pd.DataFrame): DataFrame contenant les métadonnées des tables, incluant les noms des tables et leurs types de téléchargement.
            end_time (str): Date de fin à mettre à jour dans les configurations (format attendu: chaîne de caractères).
            col_to_update (ColumnToUpdate): Enum représentant la colonne à mettre à jour dans les configurations.

        Returns:
            None: Cette méthode ne retourne rien, elle met à jour les configurations directement dans le bucket GCS.
        """
        # Récupère les configurations existantes depuis le bucket GCS.
        data = self.get_configs_for_update()
        all_missing_dates: dict[str, list[datetime]] = kwargs.get("missing_dates", {})
        # Parcourt chaque ligne des métadonnées pour mettre à jour les configurations.
        for _, row in metadata.iterrows():
            # Récupère le nom de la table depuis les métadonnées.
            if table_name := row.get("table_name"):
                missing_dates = [date.strftime("%d/%m/%Y") for date in all_missing_dates.get(table_name, [])]

                # Si la table n'existe pas dans les configurations, l'ajoute.
                if table_name not in data:
                    data[table_name] = {}
                # Met à jour les configurations pour la table avec la date de fin et le type de téléchargement.
                data[table_name].update(
                    {
                        col_to_update.value: end_time if col_to_update != ColumnToUpdate.DATE_NO_DATA else list(set(data.get(table_name, {}).get(col_to_update.value, []) + missing_dates)),
                        "download_type": row.get("download_type", "time"),
                        # Définit le type de téléchargement (par défaut: "time").
                    }
                )

        # Enregistre les configurations mises à jour dans le bucket GCS.
        self._update_config(data, "configs_for_update", erase=False)

    def transform_and_save_data(self, target_bucket_name: str, metadata: pd.DataFrame) -> None:
        """
        Transforme les données et les télécharge dans un bucket GCS.

        Args:
            target_bucket_name (str): Nom du bucket cible pour le téléchargement.
            metadata (pd.DataFrame): DataFrame contenant les métadonnées des tables.

        Returns:
            None: Cette méthode ne retourne rien, elle effectue des transformations et des téléchargements.
        """
        # Crée un client GCS pour le bucket cible
        self.target_bucket_manager = BucketManager(target_bucket_name)
        tasks = []
        for index, row in metadata.iterrows():
            table_name = row.get("table_name")
            family = row.get("family")
            input_folder = family + "/" + table_name
            files = self.bucket_manager.list_parquet_files(input_folder=input_folder)
            self.bucket_manager.logger.info(
                f"Found {len(files)} files for table {table_name} in bucket {self.bucket_manager.bucket_name}"
            )
            end_point_info = {
                "table_name": table_name,
                "family": family,
                "input_folder": input_folder,
                "folder_path": input_folder,
            }
            for file_path in files[:1]:
                tasks.append(
                    {
                        "file_path": file_path,
                        "endpoint_info": end_point_info,
                    }
                )
        parallelize_execution(
            tasks=tasks,
            func=self._apply_transformations_and_save,
            logger=self.bucket_manager.logger,
        )

    def _apply_transformations_and_save(
            self, file_path: str, endpoint_info: dict
    ) -> None:
        with CustomNamedTemporaryFile(dir=TMP_DIR) as temp_input:
            # Télécharger le fichier Parquet dans un fichier temporaire
            blob = self.bucket_manager.bucket.blob(file_path)
            blob.download_to_filename(temp_input.name)

            # Lire le fichier Parquet
            df = pq.read_table(temp_input.name).to_pandas()

            transformer = TransformData()

            df = transformer.set_data(
                data=df,
                endpoint_info=endpoint_info,
                except_table_names=["fleet_vehicles_fuel_energy"]
            ).transform()

            dfs = transformer.split_data(df, table_name=endpoint_info.get("table_name"))

            file_name = file_path.split("/")[-1].split(".")[0]
            suffixe = extract_suffixe(file_name)
            for table, table_df in dfs.items():
                if not table_df.empty:
                    file_name = f'{table}_{suffixe}'
                    # table_df.to_json(f'{TMP_DIR}/{file_name}.json', orient='records', lines=False)
                    destination_blob_name = f'{endpoint_info.get("folder_path")}/{file_name}.parquet'
                    buffer = parquet_buffer(table_df)
                    self.target_bucket_manager.upload_bytes(buffer, destination_blob_name)


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

    def run(
            self, configs_for_update: dict, metadata: pd.DataFrame | None = None
    ) -> None:
        """Exécuter le processus de chargement pour tous les fichiers Parquet."""
        table_to_paths = defaultdict(list)
        for _, row in metadata.iterrows():
            table_name = row.get("table_name")
            if not table_name:
                continue
            family = row.get("family")
            if not family:
                continue
            input_folder = f"{family}/{table_name}"
            files = self.bucket_manager.list_parquet_files(input_folder=input_folder)

            # Grouper les fichiers par table (en fonction du chemin)
            for file_path in files:
                table_name = self.bucket_manager.get_table_name(file_path)
                start_date = self.bucket_manager.get_start_date(file_path)
                if self._from:
                    if start_date and start_date < self._from:
                        continue
                if self._to:
                    end_date = self.bucket_manager.get_end_date(file_path) or start_date
                    if end_date and end_date > self._to:
                        continue
                if configs := configs_for_update.get(
                        MAPPING_TABLES.get(table_name, table_name), {}
                ):
                    if last_update_time := configs.get(ColumnToUpdate.DATABASE.value, None):
                        last_update_date = datetime.strptime(last_update_time, "%d/%m/%Y")
                        if start_date < last_update_date:
                            continue

                table_to_paths[table_name].append(file_path)

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
                tasks.append({"uri": list(set(uris)), "table_name": table_name})
            else:
                uris = [
                    f"gs://{self.bucket_name}/{file_path}" for file_path in file_paths
                ]
                tasks.append({"uri": list(set(uris)), "table_name": table_name})
                # tasks.extend([{'uri': f"gs://{self.bucket_name}/{file_path}", 'table_name': table_name} for file_path in uris])

        parallelize_execution(
            tasks=tasks,
            func=self.bigquery_manager.load_parquet_to_bigquery,
            logger=self.logger,
            max_workers=self.max_workers,
        )
