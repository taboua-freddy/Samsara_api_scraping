import hashlib
import json
import os
import random
import re
import socket
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import BytesIO
from itertools import count
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import Conflict, PreconditionFailed, TooManyRequests
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.resumable_media.common import InvalidResponse

from .interface import ColumnToUpdate, DownloadType, SearchRetrieveType
from .logs import MyLogger
from .raters import MemoryAccess
from .transformation import TransformData
from .transformation_configs import MAPPING_TABLES
from .utils import (
    DEFAULT_START_DATE,
    TMP_DIR,
    CustomNamedTemporaryFile,
    extract_date_range,
    extract_suffixe,
    make_path,
    pandas_to_bq_schema,
    parallelize_execution,
    parquet_buffer,
    process_params,
)


def build_load_fingerprint(
    bucket_name: str,
    file_path: str,
    generation: str | int | None,
    download_type: str,
) -> str:
    """Build a stable load identity; time files are treated as immutable."""
    identity = f"gs://{bucket_name}/{file_path}"
    if download_type == DownloadType.ONESHOT.value:
        identity = f"{identity}#{generation or 'unknown'}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def current_oneshot_files(manifest: dict) -> list[str] | None:
    """Return the latest published snapshot, or None for pre-manifest data.

    An in-progress refresh must not expose its partial files to later stages.
    """
    states = [
        state for state in manifest.values()
        if isinstance(state, dict) and state.get("status") in ("complete", "in_progress")
    ]
    if not states:
        return None
    latest = max(states, key=lambda state: state.get("updated_at") or "")
    if latest["status"] != "complete":
        previous = latest.get("previous_complete") or {}
        return list(previous.get("uploaded_files") or [])
    return list(latest.get("uploaded_files") or [])


def _is_generation_conflict(exc: Exception) -> bool:
    if isinstance(exc, PreconditionFailed):
        return True
    response = getattr(exc, "response", None)
    return isinstance(exc, InvalidResponse) and getattr(response, "status_code", None) == 412


def _is_retryable_table_rate_error(exc: Exception) -> bool:
    """Only retry transient BigQuery table-write throttling, not bad schemas."""
    message = str(exc)
    return isinstance(exc, TooManyRequests) or (
        "rateLimitExceeded" in message
        and "table" in message.lower()
    ) or "too many table update operations" in message.lower()


class ExecutionLock:
    """Atomic, expiring GCS lock used to prevent concurrent pipeline runs."""

    def __init__(self, bucket, name: str, ttl_minutes: int):
        self.blob = bucket.blob(name)
        self.ttl = timedelta(minutes=ttl_minutes)
        self.owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        self.generation = None

    def acquire(self) -> "ExecutionLock":
        now = datetime.now(timezone.utc)
        payload = {
            "owner": self.owner,
            "acquired_at": now.isoformat(),
            "expires_at": (now + self.ttl).isoformat(),
        }
        try:
            self.blob.upload_from_string(
                json.dumps(payload),
                content_type="application/json",
                if_generation_match=0,
            )
        except (PreconditionFailed, InvalidResponse) as exc:
            if not _is_generation_conflict(exc):
                raise
            self.blob.reload()
            existing = json.loads(self.blob.download_as_text())
            expires_at = datetime.fromisoformat(existing["expires_at"])
            if expires_at > now:
                raise RuntimeError(
                    "Une autre exécution du pipeline est active "
                    f"(verrou détenu par {existing.get('owner', 'inconnu')})"
                ) from None
            stale_generation = self.blob.generation
            self.blob.delete(if_generation_match=stale_generation)
            self.blob.upload_from_string(
                json.dumps(payload),
                content_type="application/json",
                if_generation_match=0,
            )
        self.blob.reload()
        self.generation = self.blob.generation
        return self

    def release(self) -> None:
        if self.generation is None:
            return
        try:
            self.blob.delete(if_generation_match=self.generation)
        except (NotFound, PreconditionFailed):
            pass
        finally:
            self.generation = None


class BucketManager:
    FILE_PATH_REGEX = re.compile(
        r"^(?P<family>[^/]+)/"
        r"(?P<main_family>[^/]+)/"
        r"(?P<table_name>.+?)"
        r"(?:_(?P<start_date>\d{4}_\d{2}_\d{2})(?:_\d{6})?)?"
        r"(?:_to_(?P<end_date>\d{4}_\d{2}_\d{2})(?:_\d{6})?)?"
        r"(?:_(?P<index>\d+))?"
        r"\.parquet$"
    )

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
        self.file_path_regex = self.FILE_PATH_REGEX
        self.gcs_log_path = "resources/logs"  # os.path.join("resources", "logs")
        self.gcs_config_path = (
            "resources/configs"  # os.path.join("resources", "configs")
        )

    def acquire_execution_lock(self, ttl_minutes: int = 1440) -> ExecutionLock:
        if ttl_minutes < 1:
            raise ValueError("ttl_minutes doit être supérieur ou égal à 1")
        lock = ExecutionLock(
            self.bucket,
            f"{self.gcs_config_path}/pipeline_execution.lock",
            ttl_minutes,
        )
        return lock.acquire()

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
        return [
            item["name"]
            for item in self.list_parquet_file_metadata(input_folder=input_folder)
        ]

    def list_parquet_file_metadata(self, input_folder: str = None) -> list[dict]:
        """List Parquet paths with their GCS generation."""
        self.logger.info(f"Listing Parquet files in bucket: {self.bucket_name}")
        if input_folder:
            input_folder = input_folder if input_folder.endswith("/") else input_folder + "/"
        files = [
            {"name": blob.name, "generation": str(blob.generation or "")}
            for blob in self.bucket.list_blobs(prefix=input_folder)
            if blob.name.endswith(".parquet")
        ]
        self.logger.debug(f"Found files: {len(files)} Parquet files")
        return files

    def cleanup_logs(self, retention_days: int, dry_run: bool = False) -> dict:
        """Delete GCS log objects older than the configured retention period."""
        if retention_days < 1:
            raise ValueError("retention_days doit être supérieur ou égal à 1")
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        candidates = []
        for blob in self.bucket.list_blobs(prefix=f"{self.gcs_log_path}/"):
            updated = blob.updated
            if updated is not None and updated <= cutoff:
                candidates.append(blob)
        if not dry_run:
            for blob in candidates:
                blob.delete()
        report = {
            "retention_days": retention_days,
            "dry_run": dry_run,
            "scanned_prefix": f"{self.gcs_log_path}/",
            "deleted": len(candidates) if not dry_run else 0,
            "would_delete": len(candidates) if dry_run else 0,
        }
        self.logger.info(f"Nettoyage des logs GCS terminé: {report}")
        return report

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
        manifest_tables = set()
        requested_start_date = start_date

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

            # For timestamp endpoints a Parquet file may represent only a few
            # hours. The per-table manifest, not its filename, proves coverage.
            raw_params = row.get("params")
            params = process_params(
                raw_params if isinstance(raw_params, (str, dict)) else {}
            )
            if "startMs" in params and "endMs" in params:
                manifest_blob = self.bucket.blob(
                    f"{self.gcs_config_path}/extraction_manifests/{table_name}.json"
                )
                if manifest_blob.exists():
                    from .extraction_manifest import (
                        has_complete_coverage,
                        request_signature,
                    )

                    manifest_tables.add(table_name)
                    manifest = json.loads(manifest_blob.download_as_bytes())
                    signature = request_signature(
                        table_name, row.get("endpoint"), params
                    )
                    base_ms = int(params["startMs"])
                    request_end_ms = int(params["endMs"])
                    total_days = (end_date - requested_start_date).days
                    local_base_ms = round(requested_start_date.timestamp() * 1000)
                    file_presence = {}

                    def exists(path: str) -> bool:
                        if path not in file_presence:
                            file_presence[path] = self.file_exists(path)[0]
                        return file_presence[path]

                    for day_index in range(total_days):
                        day = requested_start_date + timedelta(days=day_index)
                        next_day = day + timedelta(days=1)
                        day_start_ms = base_ms + round(day.timestamp() * 1000) - local_base_ms
                        day_end_ms = min(
                            base_ms + round(next_day.timestamp() * 1000) - local_base_ms,
                            request_end_ms,
                        )
                        if day_start_ms >= day_end_ms:
                            continue
                        covered = has_complete_coverage(
                            day_start_ms,
                            day_end_ms,
                            manifest,
                            signature,
                            exists,
                        )
                        if covered:
                            current_dates[table_name].append(
                                requested_start_date + timedelta(days=day_index)
                            )
                    # Include tables with no complete day in the missing check.
                    current_dates[table_name]
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
                        if _start_date == _end_date:
                            current_dates[table_name].append(_start_date)
                            continue
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
            dates_with_no_data = (
                [] if table_name in manifest_tables else
                [datetime.strptime(date, "%d/%m/%Y") for date in
                 config_for_update_.get(ColumnToUpdate.DATE_NO_DATA.value, [])]
            )
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
        for _main_table_name, sub_tables in tables_to_map.items():
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
            self.storage_client.get_bucket(self.bucket_name)
            self.logger.info(f"Bucket {self.bucket_name} already exists.")
        except NotFound:
            self.storage_client.create_bucket(self.bucket_name, location="EU")
            self.logger.info(f"Bucket {self.bucket_name} created successfully.")


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
        self.table_load_min_interval_seconds = float(
            os.getenv("BIGQUERY_TABLE_LOAD_MIN_INTERVAL_SECONDS", "2.5")
        )
        if self.table_load_min_interval_seconds < 0:
            raise ValueError("BIGQUERY_TABLE_LOAD_MIN_INTERVAL_SECONDS doit être positif ou nul")
        self.table_load_max_attempts = int(
            os.getenv("BIGQUERY_TABLE_LOAD_MAX_ATTEMPTS", "5")
        )
        if self.table_load_max_attempts < 1:
            raise ValueError("BIGQUERY_TABLE_LOAD_MAX_ATTEMPTS doit être supérieur ou égal à 1")
        self._table_load_lock = threading.Lock()
        self._table_load_next_at: dict[str, float] = {}

    def _pace_table_load(self, table_name: str) -> None:
        """Reserve a per-table submission slot without blocking other tables."""
        interval = getattr(self, "table_load_min_interval_seconds", 0.0)
        if interval <= 0:
            return
        with self._table_load_lock:
            now = time.monotonic()
            slot = max(now, self._table_load_next_at.get(table_name, now))
            self._table_load_next_at[table_name] = slot + interval
        if slot > now:
            time.sleep(slot - now)

    def load_parquet_to_bigquery(
            self, uri: str | list[str], table_name: str, job_id: str | None = None
    ) -> tuple[str | list[str], bigquery.LoadJob] | tuple[str | list[str], Exception]:
        """Charger un ou plusieurs fichiers Parquet de GCS vers BigQuery."""
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
        # if len(uri) == 1 and uri[0].endswith("/*"):
        #     self.logger.info(
        #         f"Deleting existing table (if any): {table_id} before loading files from {uri[0]}"
        #     )
        #     self.bigquery_client.delete_table(table_id, not_found_ok=True)

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

        # A successful deterministic job is reusable; a failed 429 job is
        # immutable, so its retry needs a fresh deterministic attempt ID.
        self.logger.info(
            f"Loading files into BigQuery table '{table_name}' from URIs: {uri}"
        )
        max_attempts = getattr(self, "table_load_max_attempts", 5)
        new_attempts = 0
        last_error: Exception | None = None
        for retry_index in count():
            attempt_id = (
                job_id if retry_index == 0 or job_id is None
                else f"{job_id}_retry_{retry_index}"
            )
            load_job = None
            newly_submitted = False
            if attempt_id is not None:
                try:
                    load_job = self.bigquery_client.get_job(attempt_id)
                except NotFound:
                    pass
                except Exception as exc:
                    return (uri, exc)

            if load_job is None:
                if new_attempts >= max_attempts:
                    break
                self._pace_table_load(table_name)
                new_attempts += 1
                try:
                    load_job = self.bigquery_client.load_table_from_uri(
                        uri, table_id, job_config=job_config, job_id=attempt_id
                    )
                    newly_submitted = True
                except Conflict as exc:
                    if attempt_id is None:
                        return (uri, exc)
                    self.logger.info(
                        f"Le job BigQuery {attempt_id} existe déjà, récupération du résultat."
                    )
                    try:
                        load_job = self.bigquery_client.get_job(attempt_id)
                    except Exception as exc:
                        return (uri, exc)
                except Exception as exc:
                    last_error = exc
                    if not _is_retryable_table_rate_error(exc):
                        return (uri, exc)
                    if new_attempts < max_attempts:
                        time.sleep(min(30, 2 ** new_attempts) + random.uniform(0, 0.5))
                    continue
            else:
                self.logger.info(
                    f"Le job BigQuery {attempt_id} existe déjà, récupération du résultat."
                )

            try:
                result = load_job.result()
                if load_job.errors:
                    raise RuntimeError(
                        f"Erreurs BigQuery pour la table '{table_name}': {load_job.errors}"
                    )
                self.logger.info(
                    f"Table '{table_name}' mise à jour avec succès depuis {uri}."
                )
                return (uri, result)
            except Exception as exc:
                last_error = exc
                if not _is_retryable_table_rate_error(exc):
                    return (uri, exc)
                self.logger.warning(
                    f"Job BigQuery limité pour {table_name} ({attempt_id}): {exc}"
                )
                if newly_submitted and new_attempts < max_attempts:
                    time.sleep(min(30, 2 ** new_attempts) + random.uniform(0, 0.5))

        return (uri, last_error or RuntimeError(
            f"Trop de jobs BigQuery déjà échoués pour {table_name}"
        ))

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
        input_folder = metadata.iloc[0].get("family", "") + "/" + table_name
        bucket_manager: BucketManager = self.memory_manager.read("bucket_manager")
        all_file_paths = bucket_manager.list_parquet_files(input_folder=input_folder)
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
        self._config_lock = threading.RLock()

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
            raise

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
                "Le fichier de configuration est introuvable dans le bucket."
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
        def update(current: dict) -> dict:
            return data if erase else self._merge_config(current, data)

        self._mutate_config(filename, update)

    def _mutate_config(
        self, filename: str, mutator: Callable[[dict], dict]
    ) -> dict:
        """Atomically mutate a JSON config, retrying on generation conflicts."""
        with self._config_lock:
            blob_name = f"{self.bucket_manager.gcs_config_path}/{filename}.json"
            blob = self.bucket_manager.bucket.blob(blob_name)
            max_attempts = 8
            for attempt in range(max_attempts):
                generation = 0
                current = {}
                if blob.exists():
                    blob.reload()
                    generation = blob.generation
                    current = json.loads(
                        blob.download_as_bytes(if_generation_match=generation)
                    )

                updated = mutator(current)
                buffer = BytesIO(json.dumps(updated, indent=4).encode("utf-8"))
                try:
                    blob.upload_from_file(
                        buffer,
                        content_type="application/json",
                        if_generation_match=generation,
                    )
                    return updated
                except (PreconditionFailed, InvalidResponse) as exc:
                    if not _is_generation_conflict(exc):
                        raise
                    if attempt == max_attempts - 1:
                        raise RuntimeError(
                            f"Conflit persistant lors de la mise à jour de {blob_name}"
                        ) from exc
                    delay = min(
                        0.25 * (2**attempt) + random.uniform(0, 0.25),
                        4.0,
                    )
                    self.bucket_manager.logger.warning(
                        f"Conflit de génération pour {blob_name}; "
                        f"nouvelle tentative dans {delay:.2f}s"
                    )
                    time.sleep(delay)
        raise RuntimeError(f"Impossible de mettre à jour {blob_name}")

    @staticmethod
    def _merge_config(current: dict, updates: dict) -> dict:
        merged = current.copy()
        for key, value in updates.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = GCSClient._merge_config(merged[key], value)
            elif isinstance(value, list) and isinstance(merged.get(key), list):
                merged[key] = list(dict.fromkeys([*merged[key], *value]))
            else:
                merged[key] = value
        return merged

    def get_bigquery_load_manifest(self) -> dict:
        return self._get_config("bigquery_load_manifest")

    def update_bigquery_load_manifest(self, entries: dict) -> None:
        if entries:
            self._update_config(entries, "bigquery_load_manifest")

    @staticmethod
    def _extraction_manifest_name(table_name: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
            raise ValueError(f"Nom de table invalide pour le manifeste: {table_name}")
        return f"extraction_manifests/{table_name}"

    def get_extraction_manifest(self, table_name: str) -> dict:
        return self._get_config(self._extraction_manifest_name(table_name))

    def update_extraction_state(
        self, table_name: str, state_key: str, state: dict
    ) -> None:
        def replace_state(current: dict) -> dict:
            updated = current.copy()
            updated[state_key] = state
            return updated

        self._mutate_config(self._extraction_manifest_name(table_name), replace_state)

    def migrate_legacy_table_state(self, table_name: str) -> None:
        """Copy a table's legacy keys once, preserving non-temporal resumes."""
        filename = self._extraction_manifest_name(table_name)
        current = self._get_config(filename)
        if current.get("__meta__", {}).get("legacy_table_imported"):
            return
        legacy = self._get_config("extraction_manifest")
        imports = {}
        for key, state in legacy.items():
            if not isinstance(state, dict):
                continue
            identity = state.get("identity", "")
            file_path = identity.partition("|")[0]
            folder = file_path.rsplit("/", 1)[0].rsplit("/", 1)[-1]
            if folder == table_name:
                imports[key] = state

        def merge(existing: dict) -> dict:
            result = existing.copy()
            for key, state in imports.items():
                result.setdefault(key, state)
            meta = result.get("__meta__", {}).copy()
            meta["version"] = 2
            meta["legacy_table_imported"] = True
            result["__meta__"] = meta
            return result

        self._mutate_config(filename, merge)
        self.bucket_manager.logger.info(
            f"Manifeste de {table_name}: {len(imports)} ancien(s) état(s) importé(s)."
        )

    def migrate_legacy_extraction_state(
        self, table_name: str, endpoint: str, params: dict
    ) -> None:
        """Import provable UTC windows once; never delete the legacy manifest."""
        from .extraction_manifest import (
            legacy_utc_bounds,
            partition_key,
            request_signature,
        )

        # The old key did not encode static request parameters. Such entries
        # cannot safely be attributed to a new request with extra filters.
        if set(params) - {"startMs", "endMs", "after", "endpoints"}:
            return
        signature = request_signature(table_name, endpoint, params)
        filename = self._extraction_manifest_name(table_name)
        current = self._get_config(filename)
        if signature in current.get("__meta__", {}).get("legacy_signatures", []):
            return
        legacy = current
        imports = {}
        for state in legacy.values():
            if not isinstance(state, dict):
                continue
            identity = state.get("identity", "")
            if not identity.endswith(f"|{endpoint}"):
                continue
            bounds = legacy_utc_bounds(table_name, identity)
            if bounds is None or state.get("status") not in {"complete", "in_progress"}:
                continue
            files = state.get("uploaded_files", [])
            if not all(self.bucket_manager.file_exists(path)[0] for path in files):
                continue
            key = partition_key(signature, *bounds)
            imports[key] = {
                **state,
                "signature": signature,
                "start_ms": bounds[0],
                "end_exclusive_ms": bounds[1],
                "migrated_from_legacy": True,
            }

        def merge(existing: dict) -> dict:
            result = existing.copy()
            for key, state in imports.items():
                result.setdefault(key, state)
            meta = result.get("__meta__", {}).copy()
            meta["version"] = 2
            signatures = meta.get("legacy_signatures", [])
            meta["legacy_signatures"] = list(dict.fromkeys([*signatures, signature]))
            result["__meta__"] = meta
            return result

        self._mutate_config(filename, merge)
        self.bucket_manager.logger.info(
            f"Manifeste de {table_name}: {len(imports)} partition(s) UTC migrée(s)."
        )

    def cleanup_bigquery_load_manifest(
        self,
        configs_for_update: dict,
        retention_days: int = 30,
        dry_run: bool = True,
        now: datetime | None = None,
    ) -> dict:
        """Remove safe-to-forget manifest entries and return a cleanup report."""
        if retention_days < 0:
            raise ValueError("retention_days doit être positif ou nul")
        cutoff = (now or datetime.now()) - timedelta(days=retention_days)
        existence_cache = {}
        report = {}

        def clean(manifest: dict) -> dict:
            nonlocal report
            reasons = {"orphan": [], "checkpointed": [], "obsolete_oneshot": []}
            keep = {}
            oneshot_by_table = defaultdict(list)

            for fingerprint, entry in manifest.items():
                if not isinstance(entry, dict):
                    keep[fingerprint] = entry
                    continue
                uri = entry.get("uri", "")
                prefix = f"gs://{self.bucket_manager.bucket_name}/"
                if not uri.startswith(prefix):
                    keep[fingerprint] = entry
                    continue
                uris = entry.get("uris") or [uri]
                blob_names = [
                    item[len(prefix):] for item in uris if item.startswith(prefix)
                ]
                if len(blob_names) != len(uris):
                    keep[fingerprint] = entry
                    continue
                for blob_name in blob_names:
                    if blob_name not in existence_cache:
                        existence_cache[blob_name] = self.bucket_manager.file_exists(blob_name)[0]
                if not all(existence_cache[name] for name in blob_names):
                    reasons["orphan"].append(fingerprint)
                    continue

                blob_name = blob_names[0]

                source_table = entry.get("source_table_name") or entry.get("table_name")
                config = configs_for_update.get(source_table, {})
                download_type = config.get("download_type", DownloadType.TIME.value)
                if download_type == DownloadType.ONESHOT.value:
                    oneshot_by_table[(source_table, entry.get("table_name"))].append(
                        (fingerprint, entry)
                    )
                    continue

                loaded_at = self._parse_manifest_datetime(entry.get("loaded_at"))
                checkpoint = config.get(ColumnToUpdate.DATABASE.value)
                file_start = self.bucket_manager.get_start_date(blob_name)
                checkpoint_date = (
                    datetime.strptime(checkpoint, "%d/%m/%Y") if checkpoint else None
                )
                if (
                    loaded_at
                    and loaded_at < cutoff
                    and file_start
                    and checkpoint_date
                    and file_start < checkpoint_date
                ):
                    reasons["checkpointed"].append(fingerprint)
                    continue
                keep[fingerprint] = entry

            for uri_entries in oneshot_by_table.values():
                ordered = sorted(
                    uri_entries,
                    key=lambda item: self._parse_manifest_datetime(
                        item[1].get("loaded_at")
                    ) or datetime.min,
                    reverse=True,
                )
                latest_fingerprint, latest_entry = ordered[0]
                keep[latest_fingerprint] = latest_entry
                reasons["obsolete_oneshot"].extend(
                    fingerprint for fingerprint, _entry in ordered[1:]
                )

            report = {
                "dry_run": dry_run,
                "total": len(manifest),
                "kept": len(keep),
                "removed": sum(len(values) for values in reasons.values()),
                "reasons": {key: len(value) for key, value in reasons.items()},
            }
            return keep

        if dry_run:
            clean(self.get_bigquery_load_manifest())
        else:
            self._mutate_config("bigquery_load_manifest", clean)
        return report

    @staticmethod
    def _parse_manifest_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).replace(tzinfo=None)
        except (TypeError, ValueError):
            return None

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
        data = {}
        all_missing_dates: dict[str, list[datetime]] = kwargs.get("missing_dates", {})
        # Parcourt chaque ligne des métadonnées pour mettre à jour les configurations.
        for _, row in metadata.iterrows():
            # Récupère le nom de la table depuis les métadonnées.
            if table_name := row.get("table_name"):
                missing_dates = [date.strftime("%d/%m/%Y") for date in all_missing_dates.get(table_name, [])]
                data[table_name] = {}
                # Met à jour les configurations pour la table avec la date de fin et le type de téléchargement.
                data[table_name].update(
                    {
                        col_to_update.value: end_time if col_to_update != ColumnToUpdate.DATE_NO_DATA else missing_dates,
                        "download_type": row.get("download_type", "time"),
                        # Définit le type de téléchargement (par défaut: "time").
                    }
                )

        # Enregistre les configurations mises à jour dans le bucket GCS.
        self._update_config(data, "configs_for_update", erase=False)

    def transform_and_save_data(
            self,
            target_bucket_name: str,
            metadata: pd.DataFrame,
            configs_for_update: dict,
            start_date: datetime | None = None,
            end_date: datetime | None = None,
            skip_existing: bool = False,
    ) -> dict[str, int]:
        """
        Transforme les données et les télécharge dans un bucket GCS.

        Args:
            target_bucket_name (str): Nom du bucket cible pour le téléchargement.
            metadata (pd.DataFrame): DataFrame contenant les métadonnées des tables.
            configs_for_update (dict): Dictionnaire de configurations pour la mise à jour des tables.

        Returns:
            None: Cette méthode ne retourne rien, elle effectue des transformations et des téléchargements.
        """
        # Crée un client GCS pour le bucket cible
        self.target_bucket_manager = BucketManager(target_bucket_name)
        tasks = []
        for _index, row in metadata.iterrows():
            table_name = row.get("table_name")
            family = row.get("family")
            if not table_name or not family:
                continue
            input_folder = family + "/" + table_name
            files = self.bucket_manager.list_parquet_files(input_folder=input_folder)

            download_type = row.get("download_type", DownloadType.TIME.value)
            if download_type == DownloadType.ONESHOT.value:
                active_files = current_oneshot_files(
                    self.get_extraction_manifest(table_name)
                )
                if active_files is not None:
                    active_paths = set(active_files)
                    files = [path for path in files if path in active_paths]

            self.bucket_manager.logger.info(
                f"Found {len(files)} files for table {table_name} in bucket {self.bucket_manager.bucket_name}"
            )
            end_point_info = {
                "table_name": table_name,
                "family": family,
                "input_folder": input_folder,
                "folder_path": input_folder,
            }

            last_transform_date = configs_for_update.get(MAPPING_TABLES.get(table_name, table_name), {}).get(ColumnToUpdate.TRANSFORMATION.value, None)
            for file_path in files:
                file_start = self.bucket_manager.get_start_date(file_path)
                file_end = self.bucket_manager.get_end_date(file_path) or file_start
                # Keep files that overlap the requested half-open interval.
                # Timestamp endpoints may start on the previous UTC calendar
                # day when the CLI dates were interpreted in local time.
                if download_type != DownloadType.ONESHOT.value and start_date and file_end and file_end < start_date:
                    continue
                if download_type != DownloadType.ONESHOT.value and end_date and file_start and file_start >= end_date:
                    continue
                if download_type == DownloadType.TIME.value and last_transform_date:
                    if file_start and file_start < datetime.strptime(last_transform_date, "%d/%m/%Y"):
                        continue
                    if file_end and file_end < datetime.strptime(last_transform_date, "%d/%m/%Y"):
                        continue
                tasks.append(
                    {
                        "file_path": file_path,
                        "endpoint_info": end_point_info,
                        "skip_existing": skip_existing,
                    }
                )
        results = parallelize_execution(
            tasks=tasks,
            func=self._apply_transformations_and_save,
            logger=self.bucket_manager.logger,
        )
        oneshot_tables = {
            row["table_name"] for _, row in metadata.iterrows()
            if row.get("download_type") == DownloadType.ONESHOT.value
        }
        output_groups = defaultdict(list)
        for result in results:
            if not isinstance(result, dict) or result.get("source_table_name") not in oneshot_tables:
                continue
            for path in result.get("output_files", []):
                output_groups[self.target_bucket_manager.get_table_name(path)].append(path)
        for output_table, paths in output_groups.items():
            if output_table and len(paths) > 1:
                self._normalize_oneshot_parquet_schemas(paths)
        report = {
            "selected_files": len(tasks),
            "uploaded_files": sum(
                result.get("uploaded_files", 0)
                for result in results
                if isinstance(result, dict)
            ),
            "skipped_files": sum(
                result.get("skipped_files", 0)
                for result in results
                if isinstance(result, dict)
            ),
        }
        self.bucket_manager.logger.info(f"Bilan de transformation: {report}")
        return report

    def _normalize_oneshot_parquet_schemas(self, paths: list[str]) -> None:
        """Give every chunk the union schema before a BigQuery replace load."""
        schemas = []
        for path in paths:
            with CustomNamedTemporaryFile(dir=TMP_DIR) as temp_file:
                self.target_bucket_manager.bucket.blob(path).download_to_filename(
                    temp_file.name
                )
                schemas.append(pq.ParquetFile(temp_file.name).schema_arrow)
        try:
            union_schema = pa.unify_schemas(schemas, promote_options="permissive")
        except pa.ArrowInvalid as exc:
            raise RuntimeError(
                f"Schémas Parquet incompatibles pour {paths[0]}: {exc}"
            ) from exc

        for path, schema in zip(paths, schemas):
            if schema.equals(union_schema, check_metadata=False):
                continue
            with CustomNamedTemporaryFile(dir=TMP_DIR) as temp_file:
                self.target_bucket_manager.bucket.blob(path).download_to_filename(
                    temp_file.name
                )
                table = pq.read_table(temp_file.name)
                arrays = [
                    table[field.name].cast(field.type)
                    if field.name in table.column_names
                    else pa.nulls(table.num_rows, type=field.type)
                    for field in union_schema
                ]
                normalized = pa.Table.from_arrays(arrays, schema=union_schema)
                buffer = BytesIO()
                pq.write_table(normalized, buffer)
                buffer.seek(0)
                self.target_bucket_manager.upload_bytes(buffer, path)

    def _apply_transformations_and_save(
            self,
            file_path: str,
            endpoint_info: dict,
            skip_existing: bool = False,
    ) -> dict[str, int | str | list[str]]:
        uploaded_files = 0
        skipped_files = 0
        output_files = []
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
                    file_name = f'{table}_{suffixe}' if suffixe else table
                    # table_df.to_json(f'{TMP_DIR}/{file_name}.json', orient='records', lines=False)
                    destination_blob_name = f'{endpoint_info.get("folder_path")}/{file_name}.parquet'
                    if skip_existing and self.target_bucket_manager.file_exists(
                        destination_blob_name
                    )[0]:
                        self.bucket_manager.logger.info(
                            f"[skip] Fichier transformé déjà présent: {destination_blob_name}"
                        )
                        skipped_files += 1
                        output_files.append(destination_blob_name)
                        continue
                    buffer = parquet_buffer(table_df)
                    self.target_bucket_manager.upload_bytes(buffer, destination_blob_name)
                    uploaded_files += 1
                    output_files.append(destination_blob_name)
        return {
            "uploaded_files": uploaded_files,
            "skipped_files": skipped_files,
            "output_files": output_files,
            "source_table_name": endpoint_info.get("table_name"),
        }


class GCSBigQueryLoader:
    def __init__(self, bucket_name: str, dataset_id: str, **kwargs):
        # Initialiser les clients et les paramètres
        self.bucket_name: str = bucket_name
        self.dataset_id: str = dataset_id
        self.bucket_manager: BucketManager = BucketManager(bucket_name)
        self.gcs_client = GCSClient(bucket_name)
        self.bigquery_manager: BigQueryManager = BigQueryManager(dataset_id, **kwargs)
        # Configurer le logger
        self.logger: MyLogger = MyLogger("GCSBigQueryLoader", with_console=False)
        self._from: datetime | None = kwargs.get("_from", None)
        self._to: datetime | None = kwargs.get("_to", None)
        self.max_workers = 1
        self.memory_manager: MemoryAccess | None = kwargs.get("memory_manager")
        self.raw_gcs_client: GCSClient | None = kwargs.get("raw_gcs_client")

    def run(
            self, configs_for_update: dict, metadata: pd.DataFrame | None = None
    ) -> None:
        """Exécuter le processus de chargement pour tous les fichiers Parquet."""
        futures = []
        failures = []
        successful_manifest_entries = {}
        manifest = self.gcs_client.get_bigquery_load_manifest()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_entries = {}
            for _, row in metadata.iterrows():
                configured_table_name = row.get("table_name")
                if not configured_table_name:
                    continue
                family = row.get("family")
                if not family:
                    continue
                input_folder = f"{family}/{configured_table_name}"
                files = self.bucket_manager.list_parquet_file_metadata(input_folder=input_folder)
                download_type = row.get("download_type", DownloadType.TIME.value)
                if download_type == DownloadType.ONESHOT.value:
                    if self.raw_gcs_client is None:
                        raise RuntimeError("Le client GCS brut est requis pour charger les instantanés oneshot")
                    active_raw_files = current_oneshot_files(
                        self.raw_gcs_client.get_extraction_manifest(configured_table_name)
                    )
                    if active_raw_files is not None:
                        active_suffixes = {
                            extract_suffixe(path.rsplit("/", 1)[-1])
                            for path in active_raw_files
                        }
                        files = [
                            item for item in files
                            if extract_suffixe(item["name"].rsplit("/", 1)[-1])
                            in active_suffixes
                        ]
                    grouped_files = defaultdict(list)
                    for item in files:
                        table_name = self.bucket_manager.get_table_name(item["name"])
                        if table_name:
                            grouped_files[table_name].append(item)
                    if active_raw_files is not None and not grouped_files:
                        raise RuntimeError(
                            f"Aucun fichier transformé pour l'instantané oneshot "
                            f"{configured_table_name}; chargement interrompu pour éviter "
                            "de conserver silencieusement l'ancienne version."
                        )
                    for table_name, group in grouped_files.items():
                        group.sort(key=lambda item: item["name"])
                        uris = [f"gs://{self.bucket_name}/{item['name']}" for item in group]
                        identity = [
                            self.bucket_name,
                            table_name,
                            [
                                [item["name"], str(item.get("generation") or "")]
                                for item in group
                            ],
                        ]
                        fingerprint = hashlib.sha256(
                            json.dumps(identity, separators=(",", ":")).encode("utf-8")
                        ).hexdigest()
                        if fingerprint in manifest:
                            self.logger.info(f"[skip] Instantané déjà chargé: {table_name}")
                            continue
                        future = executor.submit(
                            self.bigquery_manager.load_parquet_to_bigquery,
                            uri=uris,
                            table_name=table_name,
                            job_id=f"samsara_load_{fingerprint}",
                        )
                        futures.append(future)
                        future_entries[future] = {
                            "fingerprint": fingerprint,
                            "entry": {
                                "uri": uris[0],
                                "uris": uris,
                                "table_name": table_name,
                                "source_table_name": configured_table_name,
                                "loaded_at": datetime.now().isoformat(),
                            },
                        }
                    continue
                files.reverse()  # priviégier le schema des fichiers les plus récents
                # Grouper les fichiers par table (en fonction du chemin)
                for file_metadata in files:
                    file_path = file_metadata["name"]
                    table_name = self.bucket_manager.get_table_name(file_path)
                    start_date = self.bucket_manager.get_start_date(file_path)
                    end_date = self.bucket_manager.get_end_date(file_path) or start_date
                    if self._from:
                        if end_date and end_date < self._from:
                            continue
                    if self._to:
                        if start_date and start_date >= self._to:
                            continue
                    if configs := configs_for_update.get(
                            MAPPING_TABLES.get(table_name, table_name), {}
                    ):
                        if last_update_date := configs.get(ColumnToUpdate.DATABASE.value, None):
                            if start_date and start_date < datetime.strptime(last_update_date, "%d/%m/%Y"):
                                continue

                    mapped_table_name = MAPPING_TABLES.get(table_name, table_name)
                    download_type = row.get("download_type", DownloadType.TIME.value)
                    fingerprint = build_load_fingerprint(
                        self.bucket_name,
                        file_path,
                        file_metadata.get("generation"),
                        download_type,
                    )
                    if fingerprint in manifest:
                        self.logger.info(f"[skip] Fichier déjà chargé: {file_path}")
                        continue

                    future = executor.submit(
                            self.bigquery_manager.load_parquet_to_bigquery,
                            **{
                                "uri": f"gs://{self.bucket_name}/{file_path}",
                                "table_name": table_name,
                                "job_id": f"samsara_load_{fingerprint}",
                            },
                        )
                    futures.append(future)
                    future_entries[future] = {
                        "fingerprint": fingerprint,
                        "entry": {
                            "uri": f"gs://{self.bucket_name}/{file_path}",
                            "table_name": table_name,
                            "source_table_name": mapped_table_name,
                            "generation": file_metadata.get("generation"),
                            "loaded_at": datetime.now().isoformat(),
                        },
                    }

            # Attendre que toutes les tâches soient terminées
            for future in as_completed(futures):
                uri, result = future.result()
                if isinstance(result, Exception):
                    self.logger.error(f"[x] Échec du job pour pour l'uri {uri} : {result}")
                    failures.append((uri, result))
                else:
                    self.logger.info(f"[ok] Job soumis à bigquery pour {uri}")
                    manifest_entry = future_entries[future]
                    successful_manifest_entries[manifest_entry["fingerprint"]] = manifest_entry["entry"]
        self.gcs_client.update_bigquery_load_manifest(successful_manifest_entries)
        if failures:
            first_uri, first_error = failures[0]
            raise RuntimeError(
                f"{len(failures)} chargement(s) BigQuery ont échoué. "
                f"Premier échec: {first_uri}: {first_error}"
            ) from first_error
