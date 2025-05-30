import io
import logging
import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Literal, Any

import pyarrow as pa
import pyarrow.parquet as pq

import pandas as pd
import numpy as np
import pytz
from google.cloud import bigquery


def pandas_to_bq_schema(df: pd.DataFrame, time_col: str | None = None) -> list[bigquery.SchemaField]:
    bq_types = {
        'object': 'STRING',
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64[ns, UTC]': 'TIMESTAMP',
        'datetime64[us, UTC]': 'TIMESTAMP',

    }
    schema = []
    for col, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        if col == time_col:
            # Si la colonne est le time_col, on la convertit en TIMESTAMP
            schema.append(bigquery.SchemaField(col, 'TIMESTAMP'))
        else:
            if dtype_str not in bq_types:
                raise ValueError(f"Type {dtype_str} non supporté pour la colonne {col}")
            schema.append(bigquery.SchemaField(col, bq_types[dtype_str]))
    return schema

def reverse_mapping(mapping: dict[str, str]) -> dict[str, list[str]]:
    reversed_mapping = defaultdict(list)
    for key, value in mapping.items():
        reversed_mapping[value].append(key)
    return dict(reversed_mapping)
def extract_suffixe(filename: str) -> str | None:
    """    Extrait le suffixe d'un nom de fichier au format
    - 'fleet_drivers_2024_12_22.parquet' -> '2024_12_22'
    - 'fleet_drivers_2024_12_22_to_2024_12_25.parquet' -> '2024_12_22_to_2024_12_25'
    - 'fleet_drivers_2024_12_22_to_2024_12_25_02.parquet' -> '2024_12_22_to_2024_12_25_02'
    :param filename: nom du fichier
    :return: suffixe du nom de fichier ou None si le format n'est pas respecté
    """
    match = re.match(r'^[a-zA-Z0-9_]+?_(\d{4}_\d{2}_\d{2}.*)', filename)
    return match.group(1) if match else None

def has_function(obj, function_name: str) -> bool:
    """
    Vérifie si un objet a une méthode donnée
    :param obj: objet à vérifier
    :param function_name: nom de la méthode à vérifier
    :return: booléen
    """
    return hasattr(obj, function_name) and callable(getattr(obj, function_name))


def parallelize_execution(tasks: list, func: Any, logger: logging, **kwargs):
    """
    Exécute des tâches en parallèle
    :param tasks: liste des tâches à exécuter
    :param func: nom de la méthode à exécuter
    :param logger: logger à utiliser
    :param kwargs:
    :return:
    """
    max_workers = kwargs.get('max_workers', None)
    if max_workers:
        kwargs.pop('max_workers')
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        if isinstance(func, str):
            futures = [executor.submit(getattr(task, func), **kwargs) for task in tasks if
                       has_function(task, func)]
        elif callable(func):
            for task in tasks:
                if isinstance(task, dict):
                    futures.append(executor.submit(func, **kwargs, **task))
                else:
                    futures.append(executor.submit(func, **kwargs))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Une tâche a généré une exception : {exc}")


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyer les noms de colonnes pour les rendre compatibles avec BigQuery
    :param df: dataframe à formater
    :return: dataframe avec des noms de colonnes formatées
    """
    df.columns = df.columns.str.replace(r"[^\w]", "_", regex=True)
    return df


def extract_date_range(filename)-> list[datetime]:
    """
    Extracts date and date range from filenames like 'fleet_drivers_2024_12_22.parquet',
    'fleet_drivers_2024_12_22_to_2024_12_30.parquet', etc.

    Args:
        filename: The name of the file.

    Returns:
        A pandas DataFrame with columns 'start_date' and 'end_date', or None if no date is found.
    """
    dates = []
    match = re.search(r"_(\d{4})_(\d{2})_(\d{2})", filename)
    if match:
        start_date_str = match.group(1) + "-" + match.group(2) + "-" + match.group(3)
        start_date = pd.to_datetime(start_date_str)
        dates.append(start_date)

        match_range = re.search(r"_(\d{4})_(\d{2})_(\d{2})_to_(\d{4})_(\d{2})_(\d{2})", filename)
        if match_range:
            end_date_str = match_range.group(4) + "-" + match_range.group(5) + "-" + match_range.group(6)
            end_date = pd.to_datetime(end_date_str)
            dates.append(end_date)

    return dates

def flatten_data(data_list: dict | list[dict]) -> pd.DataFrame:
    # Aplatir les données pour la conversion en DataFrame
    df = pd.json_normalize(data_list)
    df = clean_column_names(df)
    return df


def parquet_buffer(dataframe: pd.DataFrame) -> io.BytesIO:
    """
    Transforme le DataFrame en format Parquet et le place dans un buffer pour le stockage.
    """
    buffer = io.BytesIO()

    fields = []
    for col in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(dataframe[col]):
            fields.append(pa.field(col, pa.timestamp('us')))  # encodé comme TIMESTAMP(MICROS)
        else:
            fields.append(pa.field(col, pa.array(dataframe[col]).type))

    schema = pa.schema(fields)

    # Convertir le DataFrame en Table Arrow avec le schéma explicite
    table = pa.Table.from_pandas(dataframe, preserve_index=False)
    table = table.cast(schema)
    pq.write_table(table, buffer)
    buffer.seek(0)

    return buffer


def file_buffer(file)-> io.BytesIO:
    """
    Transforme un fichier en buffer pour le stockage.
    """
    buffer = io.BytesIO()
    buffer.write(file)
    buffer.seek(0)
    return buffer

def timestamp_to_timestamp_ms(timestamp: int | float) -> int:
    return int(timestamp * 1000)


def timestamp_ms_to_timestamp(timestamp_ms: int | float) -> int | float:
    return timestamp_ms / 1000


def date_to_timestamp(date: str, format="%d/%m/%Y"):
    return datetime.strptime(date, format).timestamp()


def seconds_to_minutes(seconds):
    return seconds * 60


def cast_date(date, source_del="/", target_del="-") -> str:
    return datetime.strptime(date, f"%d{source_del}%m{source_del}%Y").strftime(
        f"%Y{target_del}%m{target_del}%d")

def make_path(path: str) -> str:
    """
    Crée un chemin s'il n'existe pas déjà
    """
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_start_end_date_config(params: dict) -> dict[str, str] | None:
    if params.get('startTime') and params.get('endTime'):
        return {"start_str": "startTime", "end_str": "endTime", "type": "datetime"}
    if params.get('startDate') and params.get('endDate'):
        return {"start_str": "startDate", "end_str": "endDate", "type": "date"}
    if params.get('startMs') and params.get('endMs'):
        return {"start_str": "startMs", "end_str": "endMs", "type": "timestamp_ms"}
    return None


def date_to_iso_or_timestamp(date: datetime, date_type: Literal["datetime", "date", "timestamp_ms"]) -> str | float:
    if date_type == "datetime":
        return date.isoformat()
    if date_type == "date":
        return date.date().isoformat()
    if date_type == "timestamp_ms":
        return timestamp_to_timestamp_ms(date.timestamp())


def process_params(params: dict | str) -> dict:
    if params and isinstance(params, str):
        # Split the string by ',' and process key-value pairs
        parsed_data = {}
        current_key = None

        for pair in params.split(','):
            if '=' in pair:
                # Nouvelle clé-valeur
                key, value = pair.split('=', 1)  # Split on the first '='
                current_key = key  # Mémorise la clé courante
                if key in parsed_data:
                    parsed_data[key].append(value)
                else:
                    parsed_data[key] = [value]
            else:
                # Ajouter une valeur à la clé courante
                if current_key:
                    parsed_data[current_key].append(pair)
                else:
                    raise ValueError(f"Value '{pair}' found without a key!")

        # Flatten lists with a single value
        for key in parsed_data:
            if len(parsed_data[key]) == 1:
                parsed_data[key] = parsed_data[key][0]
            else:
                parsed_data[key] = ",".join(parsed_data[key])

        return parsed_data

    return {}

def split_list(data: list, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def set_metadata_is_processed(metadata: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    try:
        endpoint = kwargs.get('endpoint')
        is_processed = kwargs.get('is_processed')
        params = kwargs.get('params')
        if None in [endpoint, is_processed]:
            return metadata
        endpoint_indexes = metadata[metadata['endpoint'] == endpoint].index
        if len(endpoint_indexes) > 1:
            if params:
                for endpoint_index in endpoint_indexes:
                    current_params = process_params(metadata.loc[endpoint_index, 'params'])
                    if current_params.get("type") and current_params.get("type") == params.get("type"):
                        endpoint_indexes = endpoint_index
                        break
        metadata.loc[endpoint_indexes, 'is_processed'] = is_processed
    except Exception as e:
        logging.error(f"Erreur lors de la mise à jour des métadonnées: {e}")
    return metadata


__this_file__ = os.path.abspath(__file__)
ROOT_PATH = make_path(os.path.dirname(os.path.dirname(__this_file__)))
RESSOURCES_DIR = make_path(os.path.join(ROOT_PATH, "resources"))
TMP_DIR = make_path(os.path.join(RESSOURCES_DIR, "tmp"))
LOGS_DIR = make_path(os.path.join(RESSOURCES_DIR, "logs"))
DATA_DIR = make_path(os.path.join(RESSOURCES_DIR, "data"))
CREDENTIALS_DIR = make_path(os.path.join(ROOT_PATH, "credentials"))
DEFAULT_RATE_LIMIT_SECOND = 5