import gc
import json
import os.path
from typing import Literal

import pytz

from .gcp import GCSClient
from .logs import MyLogger
from .samsara import SamsaraClient
from .transformation_configs import transformation_configs
from .utils import flatten_data, parquet_buffer, seconds_to_minutes, DATA_DIR, parallelize_execution, \
    get_start_end_date_config, date_to_iso_or_timestamp, process_params, timestamp_ms_to_timestamp, \
    DEFAULT_RATE_LIMIT_SECOND, split_list, extract_suffixe, TMP_DIR
import pandas as pd
from datetime import datetime, timedelta

from .utils_transformation import *


class DataFetcher:
    """
    Cette classe encapsule la récupération et le téléchargement des données pour un endpoint donné.
    """

    def __init__(self, samsara_client: SamsaraClient, gcs_client: GCSClient, endpoint_info: dict, **kwargs):
        self.samsara_client: SamsaraClient = samsara_client
        self.gcs_client: GCSClient = gcs_client
        self.endpoint_info: dict = endpoint_info
        self.transformer = TransformData()
        self.logger = self.samsara_client.logger
        self.max_workers = kwargs.get('max_workers')

    def fetch_and_upload(self, *args, **kwargs):
        """
        Récupère les données pour un endpoint donné et les télécharge dans Google Cloud Storage
        :return: None
        """
        # Récupération des informations de l'endpoint
        rate_limit_per_seconde: int | float = self.endpoint_info.get('rate_limit_per_seconde')
        rate_limit_per_seconde = DEFAULT_RATE_LIMIT_SECOND if pd.isna(rate_limit_per_seconde) else rate_limit_per_seconde
        self.endpoint_info['rate_limit_per_seconde'] = rate_limit_per_seconde
        self.endpoint_info['rate_limit_per_minute'] = rate_limit_per_seconde * 60

        family = self.endpoint_info.get('family')
        table = self.endpoint_info.get('table_name', "")
        # self.endpoint_info['table_name'] = table
        self.endpoint_info['folder_path'] = table if pd.isna(family) else f'{family}/{table}'
        endpoint_infos = []

        # Parse les paramètres de la requête
        params = self.endpoint_info.get('params')
        params = params if not pd.isna(params) else {}
        try:
            params = process_params(params)
        except Exception as e:
            self.logger.error(
                f"Erreur lors de la conversion des paramètres pour {self.endpoint_info.get('table_name')}: {e}")
            return

        is_list = None
        endpoints = []
        # Gestion des urls avec des paramètres dynamiques (endpoint/{id}) ou des urls qui dépendent des données d'autres endpoints
        exception_config = self.endpoint_info.get('exception_config', {})
        if self.endpoint_info.get('is_exception', False):
            if isinstance(exception_config, str):
                exception_config = eval(exception_config)
            table_name_to_get = exception_config.get('table_name')  # table à partir de laquelle récupérer les données
            column_to_get = exception_config.get('table_column_name')  # colonne à récupérer
            exception_param_name = exception_config.get('exception_param_name')  # element à modifier dans les paramètres
            key_to_apply_on = exception_config.get('key_to_apply_on')  # clé qui contient l'element à modifier
            is_list = exception_config.get('is_list', True)  # si les données sont sous forme de liste ou non
            exception_type = exception_config.get('exception_type', None)
            constraint = exception_config.get('constraint', None)
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
                        downloaded_files.append(os.path.join(bucket_manager.tmp_folder, file_name))

                if not downloaded_files:
                    self.logger.error(
                        f"Aucune donnée trouvée pour la table {table_name_to_get} pour l'endpoint {self.endpoint_info.get('endpoint')}")
                    return

                process_exception = True

                df = pd.concat([pd.read_parquet(file_name, columns=[column_to_get]) for file_name in downloaded_files])
                df = df.drop_duplicates(subset=[column_to_get])
                data = df[column_to_get].tolist()

                if is_list:
                    # si c'est une liste, on met les données dans une seule chaine de caractères
                    # généralement utilisé pour les endpoints de type /endpoint avec des paramètres dynamiques

                    chunk_size = 50
                    param_to_alter = self.endpoint_info.get(key_to_apply_on)
                    for chunk in split_list(data, chunk_size):
                        if isinstance(param_to_alter, str):
                            temp_endpoint_info = self.endpoint_info.copy()
                            array_to_str_chunk = ",".join(chunk)
                            temp_endpoint_info.update(
                                {key_to_apply_on: param_to_alter.format(**{f"{exception_param_name}": array_to_str_chunk})}
                            )
                            endpoint_infos.append(temp_endpoint_info)
                else:
                    # sinon il s'agit d'une liste d'endpoints de la forme /endpoint/{id}
                    for index, value in enumerate(data):
                        endpoint_template = self.endpoint_info.get(key_to_apply_on)
                        if endpoint_template is None:
                            raise ValueError(f"Key '{key_to_apply_on}' not found in 'endpoint_info'")
                        try:
                            url = endpoint_template.format(**{f"{exception_param_name}": value})
                        except KeyError as e:
                            raise KeyError(f"Missing key in URL template: {e}") from e

                        endpoints.append({
                            "endpoint": url,
                            "index": int(index)
                        })
                    # on ajoute les endpoints à la liste des paramètres
                    # params.update({"endpoints": endpoints})
            if exception_type == "date" and constraint == "is_data_but_datetime":
                process_exception = True

            if exception_type == "date" and constraint == "only_start_date":
                process_exception = True

            if not process_exception:
                return

        if not endpoint_infos:
            endpoint_infos.append(self.endpoint_info)

        for endpoint_info in endpoint_infos:
            self.endpoint_info = endpoint_info
            # Parse les paramètres de la requête. cette position est importante pour entre compte les modifications sur les endpoints de type /endpoint/{id}
            params = self.endpoint_info.get('params')
            params = params if not pd.isna(params) else {}
            try:
                params = process_params(params)
            except Exception as e:
                self.logger.error(
                    f"Erreur lors de la conversion des paramètres pour {self.endpoint_info.get('table_name')}: {e}")
                return

            # Par défaut, l'intervalle de recupération est de 1 jour
            # on peut le modifier dans les paramètres de l'endpoint et cette valeur qui fait fois si elle existe
            if pd.isnull(delta_days := self.endpoint_info.get('delta_days')):
                delta_days = 1
            else:
                self.samsara_client.delta_days = delta_days
            delta = timedelta(days=delta_days)

            # cette variable uniformise la gestion des dates (startMs, startTime, startDate) en proposant un format standard qui gere les différents cas
            start_end_config = get_start_end_date_config(params)
            if start_end_config:
                start_end_type: Literal["datetime", "date", "timestamp_ms"] = start_end_config.get("type")
                start_date = params.get(start_end_config.get("start_str"))
                end_date = params.get(start_end_config.get("end_str"))

                if start_end_type == "timestamp_ms":
                    start_date = datetime.fromtimestamp(timestamp_ms_to_timestamp(float(start_date)), tz=pytz.UTC)
                    end_date = datetime.fromtimestamp(timestamp_ms_to_timestamp(float(end_date)), tz=pytz.UTC)
                else:
                    if start_end_type == "datetime":
                        start_date = datetime.strptime(start_date, '%d/%m/%Y')
                        end_date = datetime.strptime(end_date, '%d/%m/%Y')
                    else:
                        start_date = datetime.strptime(start_date, '%Y-%m-%d')
                        end_date = datetime.strptime(end_date, '%Y-%m-%d')
                    start_date = start_date.replace(tzinfo=pytz.UTC)
                    end_date = end_date.replace(tzinfo=pytz.UTC)


                # il est préférable de paralléliser l'exécution pour les endpoints de type /endpoint/{id} meme s'il contiennent des dates
                if self.samsara_client.shared_vars_manager and self.samsara_client.shared_vars_manager.read("is_exception"):
                    if self.endpoint_info.get('is_exception', False) and not is_list and is_list is not None:
                        parallelize_execution(
                            tasks=endpoints,
                            logger=self.logger,
                            func=self._download_flatten_and_upload_dynamic_url,
                            max_workers=self.max_workers,
                            **dict(
                                params=params,
                                date_str=self._date_str(start_date.isoformat(), end_date.isoformat())
                            )
                        )
                        return  # on sort de la fonction pour éviter de traiter les données normalement

                # endpoints marqués date mais qui sont datetime
                if isinstance(exception_config, str):
                    exception_config = eval(exception_config)
                if all([self.endpoint_info.get('is_exception', False),
                        exception_config.get('exception_type', None) == 'date',
                        exception_config.get('constraint', None) == "is_data_but_datetime"
                ]):
                    start_end_type = "datetime"

                # Récupération des données pour les endpoints avec date dans les paramètres
                date_intervals = []
                current_date = start_date
                has_range = current_date < end_date
                while has_range and current_date < end_date:
                    day_start = current_date  #.isoformat()
                    if (end_date_temp := current_date + delta - timedelta(seconds=1)) > end_date:
                        day_end = end_date #.isoformat()
                        has_range = False
                    else:
                        day_end = end_date_temp  #.isoformat()

                    date_intervals.append({
                        start_end_config.get("start_str"): date_to_iso_or_timestamp(day_start, start_end_type),
                        start_end_config.get("end_str"): date_to_iso_or_timestamp(day_end, start_end_type),
                    })
                    current_date += delta

                # Exécution en parallèle
                parallelize_execution(
                    tasks=date_intervals,
                    func=self._fetch_data_for_interval,
                    logger=self.logger,
                    max_workers=self.max_workers,
                    **{"params": params}
                )
            else:
                # Récupération des données pour les endpoints sans date dans les paramètres
                data = self.samsara_client.get_all_data(
                    endpoint=self.endpoint_info.get("endpoint"),
                    params=params,
                    max_calls_per_second=self.endpoint_info.get("rate_limit_per_seconde")
                )

                date_str = datetime.now().strftime('%Y_%m_%d')
                file_name = f'{self.endpoint_info.get("table_name")}_{date_str}'
                self._flatten_and_upload(data, file_name, date_str)

    def _fetch_data_for_interval(self, **kwargs):
        # params: dict, start_time: str, end_time: str
        """
        Récupère les données pour un jour donné et les télécharge dans google cloud storage
        :param params: paramètres de la requête
        :param start_time: date de début
        :param end_time: date de fin
        :return: None
        """
        params = kwargs.get('params')
        if not params:
            raise ValueError("Les paramètres de la requête sont requis")
        start_end_config = get_start_end_date_config(params)
        start_time = kwargs.get(start_end_config.get("start_str"))
        end_time = kwargs.get(start_end_config.get("end_str"))
        params.update({
            start_end_config.get("start_str"): start_time,
            start_end_config.get("end_str"): end_time,
        })

        if 'after' in params:
            del params['after']
        endpoints = params.get('endpoints', [])
        # on supprime les endpoints de la liste des paramètres pour éviter des requêtes avec des urls longues
        if "endpoints" in params:
            del params['endpoints']

        if start_end_config.get("type") == "timestamp_ms":
            try:
                start_time = datetime.fromtimestamp(timestamp_ms_to_timestamp(float(start_time)),tz=pytz.UTC).isoformat()
                end_time = datetime.fromtimestamp(timestamp_ms_to_timestamp(float(end_time)), tz=pytz.UTC).isoformat()
            except Exception as e:
                self.logger.error(f"Erreur lors de la conversion des dates en timestamp: {e}")
                return
        date_str = self._date_str(start_time, end_time)

        self.logger.info(
            f"Récupération des données pour le {date_str} de la table {self.endpoint_info.get('table_name')}")
        try:
            # la boucle for gére des données pour les endpoints multiples de la forme /endpoint/{id}
            for index, endpoint in enumerate(endpoints):
                file_name = f'{self.endpoint_info.get("table_name")}_{date_str}_{index}'
                params.update({"endpoint": endpoint})
                data = self.samsara_client.get_all_data(
                    endpoint=endpoint,
                    params=params,
                    max_calls_per_second=self.endpoint_info.get("rate_limit_per_seconde", DEFAULT_RATE_LIMIT_SECOND)
                )
                self._flatten_and_upload(data, file_name, date_str)

            else:
                file_name = f'{self.endpoint_info.get("table_name")}_{date_str}'
                data = self.samsara_client.get_all_data(
                    endpoint=self.endpoint_info.get('endpoint'),
                    params=params,
                    max_calls_per_second=self.endpoint_info.get("rate_limit_per_seconde", DEFAULT_RATE_LIMIT_SECOND)
                )
                self._flatten_and_upload(data, file_name, date_str)

        except Exception as e:
            self.logger.error(
                f"Erreur lors de la récupération pour table : {self.endpoint_info.get('table_name')}, date : {date_str}, params : {params} exception : {e}")

    def _date_str(self, start_time: str, end_time: str):
        if self.samsara_client.delta_days == 1:
            date_str = start_time[:10].replace("-", "_")
        else:
            date_str = start_time[:10].replace("-", "_") + "_to_" + end_time[:10].replace("-", "_")

        return date_str

    def _download_flatten_and_upload_dynamic_url(self, endpoint: str, params: dict, date_str: str, index: int, **kwargs):
        file_name = f'{self.endpoint_info.get("table_name")}_{date_str}_{index}'
        data = self.samsara_client.get_all_data(
            endpoint=endpoint,
            params=params,
            max_calls_per_second=self.endpoint_info.get("rate_limit_per_seconde", DEFAULT_RATE_LIMIT_SECOND)
        )
        self._flatten_and_upload(data, file_name, date_str)

    def _flatten_and_upload(self, data: list[dict], file_name: str, date_str: str):
        if data:
            df = flatten_data(data)
            if df.empty:
                self.logger.info(f"Aucune donnée pour le {date_str} de la table {self.endpoint_info.get('table_name')}, endpoint: {self.endpoint_info.get('endpoint')}")
                return

            self.endpoint_info.update({"date_str": date_str})
            df = self.transformer.set_data(df, self.endpoint_info).transform()
            # df.to_json(f'{DATA_DIR}/{file_name}.json', orient='records', lines=False)
            table_name = self.endpoint_info.get('table_name')
            # allow_unique_name = self.endpoint_info.get('download_type', '') != "oneshot"
            # # Transformation de la table des codes défauts

            dfs = self.split_data(df, table_name)
            if (suffixe := extract_suffixe(file_name)) is None:
                suffixe = date_str
            for table, table_df in dfs.items():
                if not table_df.empty:
                    file_name = f'{table}_{suffixe}'
                    # table_df.to_json(f'{TMP_DIR}/{file_name}.json', orient='records', lines=False)
                    destination_blob_name = f'{self.endpoint_info.get("folder_path")}/{file_name}.parquet'
                    buffer = parquet_buffer(table_df)
                    self.gcs_client.upload_bytes(buffer, destination_blob_name)
        else:
            self.logger.info(f"Aucune donnée pour le {date_str} de la table {self.endpoint_info.get('table_name')}")

    def split_data(self, df: pd.DataFrame, table_name: str, **kwargs) -> dict[str, pd.DataFrame]:
        if table_name in self.transformer.tables_to_split:
            results: dict[str, pd.DataFrame] = self.transformer.set_data(df, {"table_name": f"{table_name}_split"}).transform()
            for table_name in results:
                if not results[table_name].empty:
                    results[table_name] = self.transformer.set_data(results[table_name], {"table_name": table_name}).transform()
        else:
            results = {table_name: df}
        return results

class TransformData:

    _tables_to_split = ["fleet_vehicle_stats_faultCodes", "fleet_assets_reefers", "fleet_tags", "fleet_devices"]

    @property
    def tables_to_split(self) -> list[str]:
        """
        Retourne la liste des tables à diviser
        :return: list[str]
        """
        return self._tables_to_split
    def __init__(self):
        self.df = None
        self.endpoint_info = None
        self.logger = MyLogger("TransformData", True)

    def set_data(self, data: pd.DataFrame, endpoint_info: dict) -> "TransformData":
        """
        Définit les données à transformer et les informations de l'endpoint
        :param data:
        :param endpoint_info:
        :return: self
        """
        self.df = data
        self.endpoint_info = endpoint_info
        return self
    def _get_transform_config(self) -> dict:
        """
        Récupère la configuration de transformation pour les données
        :return: dict
        """
        table_name = self.endpoint_info.get('table_name')
        value: str | None = self.endpoint_info.pop('date_str', None)
        if table_name == "fleet_vehicles_fuel_energy" and value:
            value = value.replace("_", "-")

        configs = transformation_configs(
            **{"fuel_energy_date": value}
        )
        return configs.get(table_name, {})

    def transform(self) -> pd.DataFrame| dict[str, pd.DataFrame]:
        """
        Applique les transformations définies dans la configuration sur le DataFrame
        :return:
        """
        config = self._get_transform_config()
        function_name = 'Unknown'
        step_config = {}
        try:
            # self.logger.info(f"Taille initiale du DataFrame: {self.df.shape if isinstance(self.df, pd.DataFrame) else {k: v.shape for k, v in self.df.items()}}")
            n_configs = len(config)
            for step, step_config in config.items():
                function_name = step_config.get("function")
                self.logger.info(f"Transformation étape: {step}/{n_configs} - Table: {self.endpoint_info.get('table_name')} - Fonction: {function_name}")
                if step_config.get("type_function") == "is_df_function":
                    function = getattr(self.df, function_name)
                    self.df = function(**step_config.get("kwargs", {}))
                elif step_config.get("type_function") == "is_pd_function":
                    function = getattr(pd, function_name)
                    self.df = function(self.df, **step_config.get("kwargs"))
                elif step_config.get("type_function") == "is_custom_function":
                    function = globals().get(function_name)
                    self.df = function(self.df, **step_config.get("kwargs", {}))

                # self.logger.info(f"Taille après transformation :{step}/{n_configs} - {self.df.shape if isinstance(self.df, pd.DataFrame) else {k: v.shape for k, v in self.df.items()}}")

            # self.logger.info(f"Taille finale du DataFrame: {self.df.shape if isinstance(self.df, pd.DataFrame) else {k: v.shape for k, v in self.df.items()}}")

        except Exception as e:
            self.logger.error(f"Erreur lors de la transformation des données: {e} pour la table {self.endpoint_info.get('table_name')} avec la fonction {function_name} et la configuration {step_config}")
        return self.df

