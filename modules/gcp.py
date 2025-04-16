import json
import os
import re
from datetime import datetime, timedelta

from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from io import BytesIO
from collections import defaultdict

from .logs import MyLogger

from .utils import make_path, parallelize_execution, extract_date_range


class BucketManager:

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket: storage.bucket.Bucket = self.storage_client.bucket(self.bucket_name)
        self.logger = MyLogger("BucketManager")
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.tmp_folder = make_path(os.path.join(parent_dir, "resources", "tmp"))
        self.folder_pattern = r"/([^/]+)/\1_(\d{4}_\d{2}_\d{2})(?:_to_(\d{4}_\d{2}_\d{2}))?(?:_(\d+))?\.parquet"
        self.gcs_log_path = os.path.join("resources", "logs")
        self.gcs_config_path = os.path.join("resources", "config")

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
    def upload_bytes(self, buffer: BytesIO, destination_blob_name: str) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param buffer: buffer contenant les données
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
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
        self.logger.debug(f"Found files: {files[:5]}.........")
        return files

    def get_table_name(self, file_path: str) -> str:
        """Extraire le nom de la table du chemin d'accès."""
        # Extraction
        match = re.search(self.folder_pattern, file_path)
        table_name = match.group(1) if match else None
        return table_name

    def get_start_date(self, file_path: str) -> datetime|None:
        """Extraire la date de début du chemin d'accès."""
        # Extraction
        match = re.search(self.folder_pattern, file_path)
        if match:
            start_date = datetime.strptime(match.group(2), "%Y_%m_%d")
            return start_date
        return None
    def get_end_date(self, file_path: str) -> datetime|None:
        """Extraire la date de fin du chemin d'accès."""
        # Extraction
        match = re.search(self.folder_pattern, file_path)
        if match:
            end_date = datetime.strptime(match.group(3), "%Y_%m_%d") if match.group(4) else None
            return end_date
        return None

    def get_start_end_date(self, file_path: str) -> tuple[datetime, datetime]|None:
        """Extraire la date de début et de fin du chemin d'accès."""
        # Extraction
        match = re.search(self.folder_pattern, file_path)
        if match:
            start_date = datetime.strptime(match.group(2), "%Y_%m_%d")
            end_date = datetime.strptime(match.group(3), "%Y_%m_%d") if match.group(3) else start_date
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

    def missing_files(self, tables_names:list[str], start_date: datetime, end_date: datetime) -> dict[str, list[datetime]]:
        """Vérifie si des fichiers sont manquants dans le bucket."""
        # Récupérer les fichiers Parquet déjà présents dans le bucket
        files = self.list_parquet_files()
        # Grouper les fichiers par table (en fonction du chemin)
        current_dates = defaultdict(list)
        missing_dates = defaultdict(list)
        for file_path in files:
            table_name = self.get_table_name(file_path)
            file_name = file_path.split("/")[-1]
            if table_name in tables_names and (date_range := extract_date_range(file_name)):
                if len(date_range) == 1:
                    current_dates[table_name].append(date_range[0])
                else:
                    _start_date = date_range[0] #datetime.strptime(date_range[0], "%Y_%m_%d")
                    _end_date = date_range[1] #datetime.strptime(date_range[1], "%Y_%m_%d")
                    current_dates[table_name].extend([_start_date + timedelta(days=i) for i in range((_end_date - _start_date).days + 1)])

        # Vérifier les dates manquantes
        for table_name, dates in current_dates.items():
            if len(dates) == 1:
                continue
            for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
                if date not in dates:
                    missing_dates[table_name].append(date)
        return missing_dates



class BigQueryManager:

    def __init__(self, dataset_id: str):
        self.bigquery_client = bigquery.Client()
        self.dataset_id = dataset_id
        self.logger = MyLogger("BigQueryManager")

    def load_parquet_to_bigquery(self, uri: set, table_name: str) -> None:
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

        # Configurer la source GCS et l'opération de chargement

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )

        # Charger le fichier Parquet dans BigQuery
        self.logger.info(f"Loading files into BigQuery table '{table_name}' from URIs: {uri}")
        try:
            load_job = self.bigquery_client.load_table_from_uri(list(uri), table_id, job_config=job_config)
            load_job.result()  # Attendre la fin du job
            self.logger.info(f"Table '{table_name}' successfully updated in BigQuery with files from {uri}.")
        except Exception as e:
            self.logger.error(f"Error loading files into BigQuery table '{table_name}': {e}")



class GCSClient:
    """
    Cette classe encapsule les opérations de téléchargement de données vers Google Cloud Storage.
    """

    def __init__(self, bucket_name: str):
        self.bucket_manager = BucketManager(bucket_name)

    def upload_bytes(self, buffer: BytesIO, destination_blob_name: str) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param buffer: buffer contenant les données
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
        try:
            self.bucket_manager.upload_bytes(buffer, destination_blob_name)
        except Exception as e:
            self.bucket_manager.logger.error(f"erreur lors de la migration des données vers GCS: {e}")

    def upload_dict(self, data: dict, destination_blob_name: str) -> None:
        """
        Télécharge les données dans un bucket GCS à partir d'un buffer.
        :param data: Données à télécharger
        :param destination_blob_name: chemin du blob de destination
        :return: None
        """
        buffer = BytesIO(json.dumps(data, indent=4).encode())
        self.upload_bytes(buffer, destination_blob_name)

    def get_config(self) -> dict:
        """
        Télécharge les configurations à partir d'un fichier JSON dans un bucket GCS.
        :return: Dictionnaire de configurations
        """
        blob_name = f"{self.bucket_manager.gcs_config_path}/config.json"
        blob_exists, blob = self.bucket_manager.file_exists(blob_name)
        if not blob_exists:
            self.bucket_manager.logger.error(f"Le fichier de configuration est introuvable dans le bucket.")
            # raise FileNotFoundError(f"Le fichier de configuration est introuvable dans le bucket.")
            return {}
        return json.loads(blob.download_as_string())

    def update_config(self, data: dict, erase=False) -> None:
        """
        Met à jour les configurations dans un fichier JSON dans un bucket GCS.
        :param data: Dictionnaire de configurations
        :param erase: Effacer les configurations existantes
        :return: None
        """
        blob_name = f"{self.bucket_manager.gcs_config_path}/config.json"
        if not erase:
            config = self.get_config()
            config.update(data)
        self.bucket_manager.delete_file(blob_name)
        self.upload_dict(data, blob_name)

class GCSBigQueryLoader:
    def __init__(self, bucket_name: str, dataset_id: str, **kwargs):
        # Initialiser les clients et les paramètres
        self.bucket_name: str = bucket_name
        self.dataset_id: str = dataset_id
        self.bucket_manager: BucketManager = BucketManager(bucket_name)
        self.bigquery_manager: BigQueryManager = BigQueryManager(dataset_id)
        # Configurer le logger
        self.logger: MyLogger = MyLogger("GCSBigQueryLoader", with_console=False)
        self.table_names : list[str]|None = kwargs.get("table_names", None)
        self._from = kwargs.get("_from", None)
        self._to = kwargs.get("_to", None)
        self.max_workers = 2

    def run(self):
        """Exécuter le processus de chargement pour tous les fichiers Parquet."""
        files = self.bucket_manager.list_parquet_files()
        # Grouper les fichiers par table (en fonction du chemin)
        table_to_paths = defaultdict(list)
        for file_path in files:
            table_name = self.bucket_manager.get_table_name(file_path)
            if self.table_names and table_name not in self.table_names:
                continue
            if self._from:
                start_date = self.bucket_manager.get_start_date(file_path)
                if start_date and start_date < self._from:
                    continue

            table_to_paths[table_name].append(file_path)
            # folder_path = "/".join(file_path.split("/")[:-1])  # Extraire le chemin sans le fichier
            # table_to_paths[table_name].append(f"gs://{self.bucket_name}/{folder_path}/*")

        # tasks = [{'uri': set(uris), 'table_name': table_name} for table_name, uris in table_to_paths.items()]
        tasks = []
        for table_name, file_paths in table_to_paths.items():
            # si supérieur à 100 fichiers, on suppose qu'il ne s'agit pas d'une MAJ mais d'une première insertion donc on regroupe par dossier
            if len(file_paths) > 50:
                file_path = file_paths[0]
                folder_path = "/".join(file_path.split("/")[:-1])
                uris = [f"gs://{self.bucket_name}/{folder_path}/*"]
                tasks.append({'uri': set(uris), 'table_name': table_name})
            else:
                uris = [f"gs://{self.bucket_name}/{file_path}" for file_path in file_paths]
                tasks.append({'uri': set(uris), 'table_name': table_name})
                # tasks.extend([{'uri': f"gs://{self.bucket_name}/{file_path}", 'table_name': table_name} for file_path in uris])

        parallelize_execution(
            tasks=tasks,
            func=self.bigquery_manager.load_parquet_to_bigquery,
            logger=self.logger,
            max_workers=self.max_workers
        )

