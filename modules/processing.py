import ast
import hashlib
import json
import os.path
import uuid
from datetime import datetime, time, timedelta, timezone
from typing import Literal

import pyarrow.parquet as pq
import pytz
import requests

from .gcp import GCSClient
from .interface import DownloadType
from .extraction_manifest import (
    partition_key,
    plan_timestamp_intervals,
    request_signature,
)
from .samsara import SamsaraClient
from .transformation import TransformData
from .utils import (
    DEFAULT_RATE_LIMIT_SECOND,
    date_to_iso_or_timestamp,
    flatten_data,
    get_start_end_date_config,
    parallelize_execution,
    parquet_buffer,
    process_params,
    split_list,
    timestamp_ms_to_timestamp,
)
from .utils_transformation import *


def _parse_exception_config(config: dict | str) -> dict:
    """Parse legacy string configs without executing arbitrary code."""
    if isinstance(config, dict):
        return config
    if isinstance(config, str):
        try:
            parsed = json.loads(config)
        except json.JSONDecodeError:
            parsed = ast.literal_eval(config)
        if not isinstance(parsed, dict):
            raise ValueError("exception_config doit contenir un dictionnaire")
        return parsed
    raise ValueError("exception_config doit être un dictionnaire")


def _read_dependency_values(
    file_names: list[str],
    column_name: str,
    column_aliases: list[str] | None = None,
    logger=None,
) -> list:
    """Read a routing key from Parquet files whose schemas may differ.

    Files without any usable key are ignored: their source data remains intact,
    but they cannot be used to build a dynamic API request.  A clear error is
    raised only when none of the files contains the requested key or an alias.
    """
    candidates = list(dict.fromkeys([column_name, *(column_aliases or [])]))
    values = []
    schemas = {}

    for file_name in file_names:
        available_columns = pq.ParquetFile(file_name).schema_arrow.names
        schemas[os.path.basename(file_name)] = available_columns
        source_column = next(
            (candidate for candidate in candidates if candidate in available_columns),
            None,
        )
        if source_column is None:
            if logger:
                logger.warning(
                    "Fichier Parquet ignoré pour la résolution de la dépendance "
                    f"'{column_name}': {file_name}. Colonnes disponibles: "
                    f"{available_columns}"
                )
            continue

        series = pd.read_parquet(file_name, columns=[source_column])[source_column]
        values.extend(series.dropna().tolist())

    if not values:
        raise RuntimeError(
            f"Aucune valeur trouvée pour la colonne '{column_name}' "
            f"(alias acceptés: {candidates[1:]}) dans les fichiers de dépendance. "
            f"Schémas détectés: {schemas}"
        )

    # dict preserves the source order and avoids pandas/Arrow type coercion
    # while combining chunks with heterogeneous schemas.
    return list(dict.fromkeys(values))


def _retryable_server_status(error: Exception) -> int | None:
    """Find a retryable HTTP status in the API client's exception chain."""
    while error is not None:
        response = getattr(error, "response", None)
        status = getattr(response, "status_code", None)
        if isinstance(error, requests.HTTPError) and status in (500, 502, 503, 504):
            return status
        error = error.__cause__
    return None


class DataFetcher:
    """
    Cette classe encapsule la récupération et le téléchargement des données pour un endpoint donné.
    """

    def __init__(
        self,
        samsara_client: SamsaraClient,
        gcs_client: GCSClient,
        endpoint_info: dict,
        **kwargs,
    ):
        self.transformer = TransformData()
        self.samsara_client: SamsaraClient = samsara_client
        self.gcs_client: GCSClient = gcs_client
        self.endpoint_info: dict = endpoint_info
        self.logger = self.samsara_client.logger
        self.max_workers = kwargs.get("max_workers")
        def chunk_setting(env_name: str, metadata_name: str, default: int) -> int:
            value = os.getenv(env_name)
            if value is None:
                value = endpoint_info.get(metadata_name)
            return int(default if value is None or pd.isna(value) else value)

        self.chunk_rows = chunk_setting("SAMSARA_CHUNK_ROWS", "chunk_rows", 50000)
        if self.chunk_rows < 1:
            raise ValueError("SAMSARA_CHUNK_ROWS doit être supérieur ou égal à 1")
        self.chunk_pages = chunk_setting("SAMSARA_CHUNK_PAGES", "chunk_pages", 25)
        if self.chunk_pages < 1:
            raise ValueError("SAMSARA_CHUNK_PAGES doit être supérieur ou égal à 1")

    def _split_policy(self) -> dict:
        """Effective policy may change between runs without changing coverage."""
        def setting(env_name: str, metadata_name: str, default):
            value = os.getenv(env_name)
            if value is None:
                value = self.endpoint_info.get(metadata_name)
            return default if value is None or pd.isna(value) else value

        delta_days = self.endpoint_info.get("delta_days")
        default_minutes = float(1 if pd.isna(delta_days) else delta_days) * 1440
        window_minutes = float(
            setting("SAMSARA_WINDOW_MINUTES", "window_minutes", default_minutes)
        )
        min_minutes = float(
            setting("SAMSARA_SPLIT_MIN_MINUTES", "split_min_minutes", 45)
        )
        max_depth = int(setting("SAMSARA_SPLIT_MAX_DEPTH", "split_max_depth", 3))
        if window_minutes < 1 or min_minutes < 1 or max_depth < 0:
            raise ValueError("Paramètres de découpage invalides")
        return {
            "window_minutes": window_minutes,
            "split_min_minutes": min_minutes,
            "split_max_depth": max_depth,
        }

    def fetch_and_upload(self, *args, **kwargs):
        """
        Récupère les données pour un endpoint donné et les télécharge dans Google Cloud Storage
        :return: None
        """
        # Récupération des informations de l'endpoint
        rate_limit_per_seconde: int | float = self.endpoint_info.get(
            "rate_limit_per_seconde"
        )
        rate_limit_per_seconde = (
            DEFAULT_RATE_LIMIT_SECOND
            if pd.isna(rate_limit_per_seconde)
            else rate_limit_per_seconde
        )
        self.endpoint_info["rate_limit_per_seconde"] = rate_limit_per_seconde
        self.endpoint_info["rate_limit_per_minute"] = rate_limit_per_seconde * 60

        family = self.endpoint_info.get("family")
        table = self.endpoint_info.get("table_name", "")
        # self.endpoint_info['table_name'] = table
        self.endpoint_info["folder_path"] = (
            table if pd.isna(family) else f"{family}/{table}"
        )
        self.gcs_client.migrate_legacy_table_state(table)
        endpoint_infos = []

        # Parse les paramètres de la requête
        params = self.endpoint_info.get("params")
        params = params if not pd.isna(params) else {}
        try:
            params = process_params(params)
        except Exception as e:
            self.logger.error(
                f"Erreur lors de la conversion des paramètres pour {self.endpoint_info.get('table_name')}: {e}"
            )
            raise

        is_list = None
        endpoints = []
        # Gestion des urls avec des paramètres dynamiques (endpoint/{id}) ou des urls qui dépendent des données d'autres endpoints
        exception_config = self.endpoint_info.get("exception_config", {})
        if self.endpoint_info.get("is_exception", False):
            exception_config = _parse_exception_config(exception_config)
            table_name_to_get = exception_config.get(
                "table_name"
            )  # table à partir de laquelle récupérer les données
            column_to_get = exception_config.get(
                "table_column_name"
            )  # colonne à récupérer
            column_aliases = exception_config.get("table_column_aliases", [])
            exception_param_name = exception_config.get(
                "exception_param_name"
            )  # element à modifier dans les paramètres
            key_to_apply_on = exception_config.get(
                "key_to_apply_on"
            )  # clé qui contient l'element à modifier
            is_list = exception_config.get(
                "is_list", True
            )  # si les données sont sous forme de liste ou non
            exception_type = exception_config.get("exception_type", None)
            constraint = exception_config.get("constraint", None)
            process_exception = False

            # Récupération des données à partir de la table spécifiée
            if exception_type == "table" and constraint == "dynamic_url":
                bucket_manager = self.gcs_client.bucket_manager
                all_file_paths = bucket_manager.list_parquet_files()
                downloaded_files = []
                for file_path in all_file_paths:
                    file_name = file_path.split("/")[-1]
                    __table_name = bucket_manager.get_table_name(file_path)
                    if table_name_to_get == __table_name:
                        bucket_manager.download_file(file_path, file_name)
                        downloaded_files.append(
                            os.path.join(bucket_manager.tmp_folder, file_name)
                        )

                if not downloaded_files:
                    message = (
                        f"Aucune donnée trouvée pour la table {table_name_to_get} pour l'endpoint {self.endpoint_info.get('endpoint')}"
                    )
                    self.logger.error(message)
                    raise RuntimeError(message)

                process_exception = True

                data = _read_dependency_values(
                    downloaded_files,
                    column_to_get,
                    column_aliases=column_aliases,
                    logger=self.logger,
                )

                if is_list:
                    # si c'est une liste, on met les données dans une seule chaine de caractères
                    # généralement utilisé pour les endpoints de type /endpoint avec des paramètres dynamiques

                    chunk_size = 50
                    param_to_alter = self.endpoint_info.get(key_to_apply_on)
                    for chunk in split_list(data, chunk_size):
                        if isinstance(param_to_alter, str):
                            temp_endpoint_info = self.endpoint_info.copy()
                            array_to_str_chunk = ",".join(map(str, chunk))
                            temp_endpoint_info.update(
                                {
                                    key_to_apply_on: param_to_alter.format(
                                        **{
                                            f"{exception_param_name}": array_to_str_chunk
                                        }
                                    )
                                }
                            )
                            endpoint_infos.append(temp_endpoint_info)
                else:
                    # sinon il s'agit d'une liste d'endpoints de la forme /endpoint/{id}
                    for index, value in enumerate(data):
                        endpoint_template = self.endpoint_info.get(key_to_apply_on)
                        if endpoint_template is None:
                            raise ValueError(
                                f"Key '{key_to_apply_on}' not found in 'endpoint_info'"
                            )
                        try:
                            url = endpoint_template.format(
                                **{f"{exception_param_name}": value}
                            )
                        except KeyError as e:
                            raise KeyError(f"Missing key in URL template: {e}") from e

                        endpoints.append({"endpoint": url, "index": int(index)})
                    # on ajoute les endpoints à la liste des paramètres
                    # params.update({"endpoints": endpoints})
            if exception_type == "date" and constraint == "is_data_but_datetime":
                process_exception = True

            if exception_type == "date" and constraint == "only_start_date":
                process_exception = True

            if not process_exception:
                raise ValueError(
                    f"Configuration d'exception non prise en charge: {exception_config}"
                )

        if not endpoint_infos:
            endpoint_infos.append(self.endpoint_info)

        for endpoint_info in endpoint_infos:
            self.endpoint_info = endpoint_info
            # Parse les paramètres de la requête. cette position est importante pour entre compte les modifications sur les endpoints de type /endpoint/{id}
            params = self.endpoint_info.get("params")
            params = params if not pd.isna(params) else {}
            try:
                params = process_params(params)
            except Exception as e:
                self.logger.error(
                    f"Erreur lors de la conversion des paramètres pour {self.endpoint_info.get('table_name')}: {e}"
                )
                raise

            # Par défaut, l'intervalle de recupération est de 1 jour
            # on peut le modifier dans les paramètres de l'endpoint et cette valeur qui fait fois si elle existe
            split_policy = self._split_policy()
            delta = timedelta(minutes=split_policy["window_minutes"])

            # cette variable uniformise la gestion des dates (startMs, startTime, startDate) en proposant un format standard qui gere les différents cas
            start_end_config = get_start_end_date_config(params)
            if start_end_config:
                start_end_type: Literal["datetime", "date", "timestamp_ms"] = (
                    start_end_config.get("type")
                )
                if start_end_type == "date" and delta < timedelta(days=1):
                    # Date-only APIs cannot represent subday windows.
                    delta = timedelta(days=1)
                start_date = params.get(start_end_config.get("start_str"))
                end_date = params.get(start_end_config.get("end_str"))

                if start_end_type == "timestamp_ms":
                    start_date = datetime.fromtimestamp(
                        timestamp_ms_to_timestamp(float(start_date)), tz=pytz.UTC
                    )
                    end_date = datetime.fromtimestamp(
                        timestamp_ms_to_timestamp(float(end_date)), tz=pytz.UTC
                    )
                else:
                    if start_end_type == "datetime":
                        start_date = datetime.strptime(start_date, "%d/%m/%Y")
                        end_date = datetime.strptime(end_date, "%d/%m/%Y")
                    else:
                        start_date = datetime.strptime(start_date, "%Y-%m-%d")
                        end_date = datetime.strptime(end_date, "%Y-%m-%d")
                    start_date = start_date.replace(tzinfo=pytz.UTC)
                    end_date = end_date.replace(tzinfo=pytz.UTC)

                # il est préférable de paralléliser l'exécution pour les endpoints de type /endpoint/{id} meme s'il contiennent des dates
                if (
                    self.samsara_client.shared_vars_manager
                    and self.samsara_client.shared_vars_manager.read("is_exception")
                ):
                    if (
                        self.endpoint_info.get("is_exception", False)
                        and not is_list
                        and is_list is not None
                    ):
                        parallelize_execution(
                            tasks=endpoints,
                            logger=self.logger,
                            func=self._download_flatten_and_upload_dynamic_url,
                            max_workers=self.max_workers,
                            **dict(
                                params=params,
                                date_str=self._date_str(
                                    start_date.isoformat(), end_date.isoformat()
                                ),
                            ),
                        )
                        return  # on sort de la fonction pour éviter de traiter les données normalement

                # endpoints marqués date mais qui sont datetime
                exception_config = _parse_exception_config(exception_config)
                if all(
                    [
                        self.endpoint_info.get("is_exception", False),
                        exception_config.get("exception_type", None) == "date",
                        exception_config.get("constraint", None)
                        == "is_data_but_datetime",
                    ]
                ):
                    start_end_type = "datetime"

                # Récupération des données pour les endpoints avec date dans les paramètres
                date_intervals = []
                if start_end_type == "timestamp_ms":
                    table_name = self.endpoint_info["table_name"]
                    endpoint = self.endpoint_info["endpoint"]
                    self.gcs_client.migrate_legacy_extraction_state(
                        table_name, endpoint, params
                    )
                    manifest = self.gcs_client.get_extraction_manifest(table_name)
                    signature = request_signature(table_name, endpoint, params)
                    start_ms = int(params[start_end_config["start_str"]])
                    end_exclusive_ms = int(params[start_end_config["end_str"]])
                    window_ms = round(split_policy["window_minutes"] * 60_000)
                    windows = plan_timestamp_intervals(
                        start_ms,
                        end_exclusive_ms,
                        window_ms,
                        manifest,
                        signature,
                        lambda path: self.gcs_client.bucket_manager.file_exists(path)[0],
                    )
                    self.logger.info(
                        f"{table_name}: {len(windows)} fenêtre(s) à télécharger ou reprendre "
                        f"avec une fenêtre cible de {split_policy['window_minutes']} minute(s)."
                    )
                    date_intervals = [
                        {"startMs": left, "endMs": right - 1}
                        for left, right in windows
                    ]
                else:
                    current_date = start_date
                    while current_date < end_date:
                        boundary_gap = (
                            timedelta(seconds=1)
                            if start_end_type == "date"
                            else timedelta(milliseconds=1)
                        )
                        day_end = min(current_date + delta - boundary_gap, end_date)
                        date_intervals.append(
                            {
                                start_end_config["start_str"]: date_to_iso_or_timestamp(
                                    current_date, start_end_type
                                ),
                                start_end_config["end_str"]: date_to_iso_or_timestamp(
                                    day_end, start_end_type
                                ),
                            }
                        )
                        current_date += delta

                # Exécution en parallèle
                parallelize_execution(
                    tasks=date_intervals,
                    func=self._fetch_data_for_interval,
                    logger=self.logger,
                    max_workers=self.max_workers,
                    **{"params": params},
                )
            else:
                # Récupération des données pour les endpoints sans date dans les paramètres
                file_name = f'{self.endpoint_info.get("table_name")}'
                self._stream_and_upload(
                    endpoint=self.endpoint_info.get("endpoint"),
                    params=params,
                    file_name=file_name,
                    date_str="",
                    max_calls_per_second=self.endpoint_info.get(
                        "rate_limit_per_seconde"
                    ),
                )

    def _fetch_data_for_interval(self, **kwargs):
        # params: dict, start_time: str, end_time: str
        """
        Récupère les données pour un jour donné et les télécharge dans google cloud storage
        :param params: paramètres de la requête
        :param start_time: date de début
        :param end_time: date de fin
        :return: None
        """
        params = kwargs.get("params", {}).copy()
        if not params:
            raise ValueError("Les paramètres de la requête sont requis")
        start_end_config = get_start_end_date_config(params)
        start_time = kwargs.get(start_end_config.get("start_str"))
        end_time = kwargs.get(start_end_config.get("end_str"))
        params.update(
            {
                start_end_config.get("start_str"): start_time,
                start_end_config.get("end_str"): end_time,
            }
        )

        if "after" in params:
            del params["after"]
        endpoints = params.get("endpoints", [])
        # on supprime les endpoints de la liste des paramètres pour éviter des requêtes avec des urls longues
        if "endpoints" in params:
            del params["endpoints"]

        if start_end_config.get("type") == "timestamp_ms":
            try:
                start_time = datetime.fromtimestamp(
                    timestamp_ms_to_timestamp(float(start_time)), tz=pytz.UTC
                ).isoformat()
                end_time = datetime.fromtimestamp(
                    timestamp_ms_to_timestamp(float(end_time)), tz=pytz.UTC
                ).isoformat()
            except Exception as e:
                self.logger.error(
                    f"Erreur lors de la conversion des dates en timestamp: {e}"
                )
                raise
        date_str = self._date_str(start_time, end_time)

        self.logger.info(
            f"Récupération des données pour le {date_str} de la table {self.endpoint_info.get('table_name')}"
        )
        try:
            # la boucle for gére des données pour les endpoints multiples de la forme /endpoint/{id}
            if endpoints:
                for index, endpoint in enumerate(endpoints):
                    file_name = f'{self.endpoint_info.get("table_name")}_{date_str}_{index}'
                    endpoint_params = params.copy()
                    data = self.samsara_client.get_all_data(
                        endpoint=endpoint,
                        params=endpoint_params,
                        max_calls_per_second=self.endpoint_info.get(
                            "rate_limit_per_seconde", DEFAULT_RATE_LIMIT_SECOND
                        ),
                        rate_limit_key=self.endpoint_info.get("endpoint"),
                    )
                    self._flatten_and_upload(data, file_name, date_str)
            else:
                file_name = f'{self.endpoint_info.get("table_name")}_{date_str}'
                self._stream_and_upload(
                    endpoint=self.endpoint_info.get("endpoint"),
                    params=params,
                    file_name=file_name,
                    date_str=date_str,
                    max_calls_per_second=self.endpoint_info.get(
                        "rate_limit_per_seconde", DEFAULT_RATE_LIMIT_SECOND
                    ),
                )

        except Exception as e:
            if self._split_server_error_interval(e, params, file_name, kwargs):
                return
            self.logger.error(
                f"Erreur lors de la récupération pour table : {self.endpoint_info.get('table_name')}, date : {date_str}, params : {params} exception : {e}"
            )
            raise

    def _split_server_error_interval(
        self, error: Exception, params: dict, file_name: str, kwargs: dict
    ) -> bool:
        """Split a failed legacy query without replaying any committed chunks."""
        split_enabled = self.endpoint_info.get("split_on_server_error")
        if not pd.notna(split_enabled):
            split_enabled = self.endpoint_info.get("split_on_gateway_timeout")
        status = _retryable_server_status(error)
        if not pd.notna(split_enabled) or not split_enabled or status is None:
            return False
        policy = self._split_policy()
        depth = int(kwargs.get("_split_depth", 0))
        start_ms = int(params["startMs"])
        end_ms = int(params["endMs"])
        duration_ms = end_ms - start_ms + 1
        if (depth >= policy["split_max_depth"] or
                duration_ms // 2 < policy["split_min_minutes"] * 60_000):
            return False

        endpoint = self.endpoint_info.get("endpoint")
        table_name = self.endpoint_info["table_name"]
        identity = f'{self.endpoint_info.get("folder_path")}/{file_name}|{endpoint}'
        signature = request_signature(table_name, endpoint, params)
        state_key = partition_key(signature, start_ms, end_ms + 1)
        state = self.gcs_client.get_extraction_manifest(table_name).get(state_key, {})
        if state.get("uploaded_files") or state.get("next_cursor"):
            # A committed chunk must be resumed with its cursor to avoid duplicates.
            return False
        # A crash can happen after uploading a chunk but before persisting its
        # cursor. Do not split the parent while an uncommitted parent file exists:
        # it would otherwise be loaded alongside the child partitions.
        prefix = f'{self.endpoint_info["folder_path"]}/{file_name}_'
        if any(self.gcs_client.bucket_manager.bucket.list_blobs(
            prefix=prefix, max_results=1
        )):
            self.logger.warning(
                f"Chunk non confirmé détecté pour {identity}; "
                "la fenêtre d'origine sera retentée sans découpage."
            )
            return False

        middle_ms = start_ms + duration_ms // 2
        if middle_ms <= start_ms or middle_ms > end_ms:
            return False
        self.gcs_client.update_extraction_state(
            table_name,
            state_key,
            {
                "identity": identity,
                "signature": signature,
                "start_ms": start_ms,
                "end_exclusive_ms": end_ms + 1,
                "status": "split",
                "children": [[start_ms, middle_ms], [middle_ms, end_ms + 1]],
                "split_policy": policy,
                "updated_at": datetime.now().isoformat(),
            },
        )
        self.logger.warning(
            f"Samsara a renvoyé HTTP {status} pour {self.endpoint_info.get('table_name')} "
            f"[{start_ms}, {end_ms}]. Nouvelle tentative en deux fenêtres: "
            f"[{start_ms}, {middle_ms - 1}] et [{middle_ms}, {end_ms}]."
        )
        base_params = kwargs["params"]
        for child_start, child_end in (
            (start_ms, middle_ms - 1),
            (middle_ms, end_ms),
        ):
            self._fetch_data_for_interval(
                params=base_params,
                startMs=child_start,
                endMs=child_end,
                _split_depth=depth + 1,
            )
        return True

    def _date_str(self, start_time: str, end_time: str):
        start = datetime.fromisoformat(start_time)
        end = datetime.fromisoformat(end_time)
        start_date = start.strftime("%Y_%m_%d")
        end_date = end.strftime("%Y_%m_%d")

        if len(start_time) == 10 and len(end_time) == 10:
            return start_date if start_date == end_date else f"{start_date}_to_{end_date}"

        if (
            end - start == timedelta(days=1)
            and start.time() == time.min
            and end.time() == time.min
        ):
            return start_date
        if (
            start.date() == end.date()
            and start.time() == time.min
            and end.time() >= time(23, 59, 59)
        ):
            return start_date
        if end - start < timedelta(days=1):
            return (
                f"{start_date}_{start.strftime('%H%M%S')}_to_"
                f"{end_date}_{end.strftime('%H%M%S')}"
            )
        return f"{start_date}_to_{end_date}"

    def _download_flatten_and_upload_dynamic_url(
        self, endpoint: str, params: dict, date_str: str, index: int, **kwargs
    ):
        file_name = f'{self.endpoint_info.get("table_name")}_{date_str}_{index}'
        data = self.samsara_client.get_all_data(
            endpoint=endpoint,
            params=params,
            max_calls_per_second=self.endpoint_info.get(
                "rate_limit_per_seconde", DEFAULT_RATE_LIMIT_SECOND
            ),
            rate_limit_key=self.endpoint_info.get("endpoint"),
        )
        self._flatten_and_upload(data, file_name, date_str)

    def _stream_and_upload(
        self,
        endpoint: str,
        params: dict,
        file_name: str,
        date_str: str,
        max_calls_per_second: int | float,
    ) -> None:
        table_name = self.endpoint_info["table_name"]
        state_identity = f'{self.endpoint_info.get("folder_path")}/{file_name}|{endpoint}'
        signature = request_signature(table_name, endpoint, params)
        start_ms = int(params["startMs"]) if "startMs" in params else None
        end_exclusive_ms = int(params["endMs"]) + 1 if "endMs" in params else None
        state_key = (
            partition_key(signature, start_ms, end_exclusive_ms)
            if start_ms is not None and end_exclusive_ms is not None
            else hashlib.sha256(f"{signature}:{state_identity}".encode()).hexdigest()
        )
        manifest = self.gcs_client.get_extraction_manifest(table_name)
        state = manifest.get(state_key, {})
        is_oneshot = self.endpoint_info.get("download_type") == DownloadType.ONESHOT.value
        if not (start_ms is not None and end_exclusive_ms is not None) and not state:
            legacy_key = hashlib.sha256(state_identity.encode()).hexdigest()
            legacy_state = manifest.get(legacy_key, {})
            if legacy_state.get("identity") == state_identity:
                state = legacy_state
        state_metadata = {
            "identity": state_identity,
            "signature": signature,
            "split_policy": self._split_policy(),
        }
        if start_ms is not None and end_exclusive_ms is not None:
            state_metadata.update(
                {"start_ms": start_ms, "end_exclusive_ms": end_exclusive_ms}
            )
        uploaded_files = state.get("uploaded_files", [])
        previous_complete = (
            state if state.get("status") == "complete"
            else state.get("previous_complete")
        ) if is_oneshot else None
        files_are_present = (
            is_oneshot and state.get("status") == "complete"
        ) or all(
            self.gcs_client.bucket_manager.file_exists(path)[0]
            for path in uploaded_files
        )
        if not is_oneshot and state.get("status") == "complete" and files_are_present:
            self.logger.info(f"Extraction déjà complète pour {state_identity}")
            return
        if state.get("status") == "complete" or not files_are_present:
            state = {}
            uploaded_files = []
        if is_oneshot and state and not state.get("snapshot_id"):
            # Legacy partial files have stable names; do not mix them with a
            # newly versioned refresh.
            state = {}
            uploaded_files = []

        # A completed oneshot is a snapshot, not a permanent extraction
        # checkpoint. Keep each refresh in distinct objects so a failed run
        # cannot overwrite the last complete snapshot.
        snapshot_id = state.get("snapshot_id") if is_oneshot else None
        if is_oneshot and not snapshot_id:
            snapshot_id = (
                f"{datetime.now(timezone.utc):%Y_%m_%d_%H%M%S}_"
                f"{uuid.uuid4().int % 100_000_000:08d}"
            )
        if is_oneshot:
            state_metadata["snapshot_id"] = snapshot_id

        def chunk_file_name(index: int) -> str:
            if is_oneshot:
                stamp, nonce = snapshot_id.rsplit("_", 1)
                return f"{file_name}_{stamp}_{nonce}{index:05d}"
            return f"{file_name}_{index:05d}"

        request_params = params.copy()
        if cursor := state.get("next_cursor"):
            request_params["after"] = cursor
        chunk_index = int(state.get("next_chunk_index", 1))
        chunk_policy = {
            "chunk_rows": self.chunk_rows,
            "chunk_pages": self.chunk_pages,
        }
        chunk_policy_history = list(state.get("chunk_policy_history") or [])
        if not chunk_policy_history or any(
            chunk_policy_history[-1].get(key) != value
            for key, value in chunk_policy.items()
        ):
            chunk_policy_history.append(
                {
                    **chunk_policy,
                    "from_chunk_index": chunk_index,
                    "recorded_at": datetime.now().isoformat(),
                }
            )
        state_metadata.update(
            {
                "chunk_policy": chunk_policy,
                "chunk_policy_history": chunk_policy_history,
            }
        )
        rows_written = int(state.get("rows_written", 0))
        buffered_rows = []
        next_cursor = state.get("next_cursor")
        pages_processed = 0
        pages_in_chunk = 0

        for page, pagination in self.samsara_client.iter_data_pages(
            endpoint=endpoint,
            params=request_params,
            max_calls_per_second=max_calls_per_second,
        ):
            pages_processed += 1
            pages_in_chunk += 1
            buffered_rows.extend(page)
            next_cursor = (
                pagination.get("endCursor")
                if pagination.get("hasNextPage", False)
                else None
            )
            print(
                f"\r[{self.endpoint_info.get('table_name')}] "
                f"pages={pages_processed} chunks={chunk_index - 1} "
                f"objets={rows_written + len(buffered_rows)}",
                end="",
                flush=True,
            )
            if (
                len(buffered_rows) >= self.chunk_rows
                or pages_in_chunk >= self.chunk_pages
            ):
                destination = self._flatten_and_upload(
                    buffered_rows,
                    chunk_file_name(chunk_index),
                    date_str,
                )
                if destination:
                    uploaded_files.append(destination)
                rows_written += len(buffered_rows)
                chunk_index += 1
                buffered_rows = []
                pages_in_chunk = 0
                if next_cursor:
                    self.gcs_client.update_extraction_state(
                        table_name,
                        state_key,
                        {
                            **state_metadata,
                            "status": "in_progress",
                            "next_cursor": next_cursor,
                            "next_chunk_index": chunk_index,
                            "rows_written": rows_written,
                            "uploaded_files": uploaded_files,
                            **({"previous_complete": previous_complete} if previous_complete else {}),
                            "updated_at": datetime.now().isoformat(),
                        },
                    )

        if buffered_rows:
            destination = self._flatten_and_upload(
                buffered_rows,
                chunk_file_name(chunk_index),
                date_str,
            )
            if destination:
                uploaded_files.append(destination)
            rows_written += len(buffered_rows)
            chunk_index += 1

        self.gcs_client.update_extraction_state(
            table_name,
            state_key,
            {
                **state_metadata,
                "status": "complete",
                "next_cursor": None,
                "next_chunk_index": chunk_index,
                "rows_written": rows_written,
                "uploaded_files": uploaded_files,
                "updated_at": datetime.now().isoformat(),
            },
        )
        print(
            f"\r[{self.endpoint_info.get('table_name')}] terminé: "
            f"pages={pages_processed} chunks={len(uploaded_files)} "
            f"objets={rows_written}"
        )

    def _flatten_and_upload(
        self, data: list[dict], file_name: str, date_str: str
    ) -> str | None:
        if data:
            df = flatten_data(data)
            if df.empty:
                self.logger.info(
                    f"Aucune donnée pour le {date_str} de la table {self.endpoint_info.get('table_name')}, endpoint: {self.endpoint_info.get('endpoint')}"
                )
                return None

            if self.endpoint_info.get("table_name") == "fleet_vehicles_fuel_energy":
                # On traite la table fleet_vehicles_fuel_energy de manière spécifique
                self.endpoint_info.update({"date_str": date_str})
                df = self.transformer.set_data(df, self.endpoint_info).transform()

            # df.to_json(f'{DATA_DIR}/{file_name}.json', orient='records', lines=False)
            destination_blob_name = (
                f'{self.endpoint_info.get("folder_path")}/{file_name}.parquet'
            )
            buffer = parquet_buffer(df)
            self.gcs_client.upload_bytes(buffer, destination_blob_name)
            return destination_blob_name

        else:
            self.logger.info(
                f"Aucune donnée pour le {date_str} de la table {self.endpoint_info.get('table_name')}"
            )
        return None

