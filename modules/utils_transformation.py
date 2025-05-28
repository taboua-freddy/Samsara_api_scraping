from typing import Any

import pandas as pd

def json_normalize(df: pd.DataFrame, record_path: str, sep: str='_',prefix: str = '', meta_cols: list[str] = None, record_prefix=None)->pd.DataFrame:
    """
    Aplatie une colonne contenant des objets JSON (dicts ou listes de dicts).

    Params:
        df (pd.DataFrame): le DataFrame d'origine.
        record_path (str): nom de la colonne à aplatir.
        sep (str): séparateur pour les colonnes imbriquées.
        prefix (str): préfixe ajouté aux colonnes normalisées.
        meta_cols (list[str]): colonnes à conserver autour (si None, toutes sauf record_path).

    Returns:
        pd.DataFrame: un DataFrame aplati.
    """
    if meta_cols is None:
        meta_cols = [col for col in df.columns if col != record_path]

    # Liste pour stocker les morceaux aplatis
    all_rows = []

    for idx, row in df.iterrows():
        base_data = row[meta_cols].to_dict()
        raw_json = row[record_path]

        if isinstance(raw_json, list):  # plusieurs éléments à aplatir
            for item in raw_json:
                normalized = pd.json_normalize(item, sep=sep, record_prefix=record_prefix)
                normalized.rename(columns={col: prefix + col for col in normalized.columns}, inplace=True)
                combined = {**base_data, **normalized.iloc[0].to_dict()}
                all_rows.append(combined)
        elif isinstance(raw_json, dict):  # un seul objet JSON
            normalized = pd.json_normalize(raw_json, sep=sep, record_prefix=record_prefix)
            normalized.rename(columns={col: prefix + col for col in normalized.columns}, inplace=True)
            combined = {**base_data, **normalized.iloc[0].to_dict()}
            all_rows.append(combined)
        else:
            # cas vide ou malformé : on garde juste le contexte
            all_rows.append(base_data)

    return pd.DataFrame(all_rows)

def set_column(df: pd.DataFrame, col_name: str, value: str)->pd.DataFrame:
    """
    Ajoute une colonne avec une valeur constante à un DataFrame
    :param df: DataFrame d'entrée
    :param col_name: nom de la nouvelle colonne
    :param value: valeur constante à ajouter
    :return: DataFrame avec la nouvelle colonne
    """
    df[col_name] = value
    return df

def to_datetime(df: pd.DataFrame, columns: list[str], format=None, unit=None)->pd.DataFrame:
    for column in columns:
        df[column] = pd.to_datetime(df[column], format=format, unit=unit, errors='coerce')
    return df

def cast_column(df: pd.DataFrame, columns: list[str], dtype: Any)->pd.DataFrame:
    """
    Convertit le type de données d'une ou plusieurs colonnes d'un DataFrame.
    :param df: DataFrame d'entrée
    :param columns: liste des noms de colonnes à convertir
    :param dtype: type de données cible (par exemple, 'int', 'float', 'str')
    :return: DataFrame avec les colonnes converties
    """
    try:
        for column in columns:
            if column in df.columns:
                df[column] = df[column].astype(dtype)
    except ValueError as e:
        return df
    return df

type SplitDFConfig = dict[str, dict[str, Any]]

def split_dataframe(df: pd.DataFrame, shared_cols: list[str], split_configs: SplitDFConfig) -> dict[str, pd.DataFrame]:
    """
    Divise un DataFrame en plusieurs DataFrames basés sur des configurations de colonnes.
    :param df: DataFrame à diviser
    :param shared_cols: liste de colonnes partagées entre les DataFrames
    :param split_configs: dictionnaire de configurations pour chaque DataFrame à créer.
    :return: dict[str, pd.DataFrame]
    """
    result = {}
    for table_name, configs in split_configs.items():
        if prefix := configs.get("prefix"):
            columns = [col for col in df.columns if col.startswith(prefix)]
        else:
            columns = configs.get("columns", [])
        result[table_name] = df[shared_cols + columns].copy()
        if configs.get("drop_duplicates", False):
            result[table_name] = result[table_name].drop_duplicates(subset=configs.get("subset", None))
        if query := configs.get("query"):
            result[table_name] = filter_df(result[table_name], query)
    return result

def filter_df(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Filtre un DataFrame en fonction d'une requête.
    :param df: DataFrame à filtrer
    :param query: requête de filtrage
    :return: DataFrame filtré
    """
    return df.query(query)

def get_trans_to_filter_df(query: str) -> dict:
    """
    Crée une transformation pour filtrer un DataFrame en fonction d'une requête.
    :param query: requête de filtrage
    :return: dictionnaire de transformation
    """
    return {
        "type_function": "is_custom_function",
        "function": "filter_df",
        "kwargs": {"query": query},
    }

def get_standard_transformation_config(column_name: str, current_index=0) -> dict[int, dict]:
    """
    Fonction de configuration standard pour les transformations de données
    :param column_name: nom de la colonne à transformer
    :param record_path: chemin d'enregistrement pour json_normalize
    :param sep: séparateur pour json_normalize
    :param drop_column: si True, la colonne d'origine sera supprimée
    :return: dictionnaire de configuration
    """
    return index_transformations(
        get_trans_to_explode_df(column_name),
        get_trans_to_json_normalize_df(column_name),
        get_trans_to_drop_columns([column_name]),
        get_trans_to_rename_columns({"value": column_name}),
        current_index=current_index
    )

def get_trans_to_split_df(shared_cols: list[str], split_configs: SplitDFConfig, drop_duplicates: bool = False, subset = None):
    return {
        "type_function": "is_custom_function",
        "function": "split_dataframe",
        "kwargs": {
            "shared_cols": shared_cols,
            "split_configs": split_configs,
            "drop_duplicates": drop_duplicates,
            "subset": subset
        }
    }

def get_trans_to_drop_duplicates_df(subset=None) -> dict:
    return {
        "type_function": "is_df_function",
        "function": "drop_duplicates",
        "kwargs": {"subset": subset},
    }

def get_trans_to_default_trans() -> list[dict]:
    return [
        get_trans_to_rename_columns({"id": "vehicle_id", "name": "PARC_ID"}), # "id": "samsara_id",
        get_trans_to_drop_columns([
            "externalIds_samsara_serial",
            "externalIds_samsara_vin",
            "vehicle_externalIds_samsara_serial",
            "vehicle_externalIds_samsara_vin"
        ]),
        get_trans_to_drop_duplicates_df()
    ]

def get_trans_to_cast_column_type(columns:list[str], dtype) -> dict:
    return {
        "type_function": "is_custom_function",
        "function": "cast_column",
        "kwargs": {"columns": columns, "dtype": dtype},
    }
def get_trans_to_set_df_column(column_name:str, value:Any) -> dict:
    return {
        "type_function": "is_custom_function",
        "function": "set_column",
        "kwargs": {"col_name": column_name, "value": f"{value}"},
    }

def get_trans_to_drop_columns(columns: list) -> dict:
    return {
        "type_function": "is_df_function",
        "function": "drop",
        "kwargs":  {"columns": columns, "errors": "ignore"},
    }

def get_trans_to_rename_columns(new_columns: dict) -> dict:
    return {
        "type_function": "is_df_function",
        "function": "rename",
        "kwargs": {"columns": new_columns, "errors": "ignore"},
    }

def get_trans_to_explode_df(column_name: str) -> dict:
    return {
        "type_function": "is_df_function",
        "function": "explode",
        "kwargs": {"column": column_name},
    }

def get_trans_timestamp_to_datetime(column_name: str, unit=None) -> dict:
    return {
        "type_function": "is_custom_function",
        "function": "to_datetime",
        "kwargs": {"columns": [column_name], "unit": unit},
    }

def get_trans_to_json_normalize_df(column_name: str, record_path: str=None, sep: str = "_", prefix: str | None = "", record_prefix=None ):
    if record_path is None:
        record_path = column_name
    return {
        "column_name": column_name,
        "type_function": "is_custom_function",
        "function": "json_normalize",
        "kwargs": {"sep": sep, "record_path": record_path, "prefix": prefix, "record_prefix": record_prefix}, #
    }
def index_transformations(*transformations, current_index: int = 0, include_default_trans=True) -> dict[int, dict]:
    if include_default_trans:
        transformations = list(transformations) + get_trans_to_default_trans()
    return {index: trans for index, trans in enumerate(transformations, start=current_index + 1)}