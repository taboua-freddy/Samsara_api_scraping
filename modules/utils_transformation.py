import gc
import json
from multiprocessing import Pool, cpu_count
from typing import Any, Optional, Generator

import numpy as np
import pandas as pd

from .interface import SplitDFConfig


def _normalize_row(args):
    record, base, sep, prefix, record_prefix = args
    normalized_rows = []

    if isinstance(record, list):
        if record:
            norm = pd.json_normalize(record, sep=sep, record_prefix=record_prefix)
            rows = norm.to_dict(orient="records")
            for row in rows:
                normalized_rows.append(
                    {**base, **{f"{prefix}{k}": v for k, v in row.items()}}
                )
        else:
            normalized_rows.append(base)
    elif isinstance(record, dict):
        norm = pd.json_normalize(record, sep=sep, record_prefix=record_prefix)
        row = norm.to_dict(orient="records")[0] if not norm.empty else {}
        normalized_rows.append({**base, **{f"{prefix}{k}": v for k, v in row.items()}})

    return normalized_rows or [base]


def fast_json_normalize_parallel(
    df: pd.DataFrame,
    record_path: str,
    sep: str = "_",
    prefix: str = "",
    meta_cols: list[str] = None,
    record_prefix: str = None,
    n_processes: int = None,
) -> pd.DataFrame:
    if meta_cols is None:
        meta_cols = [col for col in df.columns if col != record_path]
    if n_processes is None:
        n_processes = max(1, cpu_count() - 1)

    valid_mask = df[record_path].apply(lambda x: isinstance(x, (list, dict)))
    df_valid = df[valid_mask].copy()
    meta_data = df_valid[meta_cols].to_dict(orient="records")

    args = [
        (record, base, sep, prefix, record_prefix)
        for record, base in zip(df_valid[record_path], meta_data)
    ]

    with Pool(processes=n_processes) as pool:
        results = pool.map(_normalize_row, args)

    normalized_parts = [item for sublist in results for item in sublist]

    df_invalid = df[~valid_mask]
    normalized_parts.extend(df_invalid[meta_cols].to_dict(orient="records"))

    return pd.DataFrame(normalized_parts)


def json_normalize(
    df: pd.DataFrame,
    record_path: str,
    sep: str = "_",
    prefix: str = "",
    meta_cols: Optional[list[str]] = None,
    record_prefix=None,
) -> pd.DataFrame:
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

        # On filtre les lignes valides (dict) pour éviter erreurs
        is_dict = df[record_path].apply(lambda x: isinstance(x, dict))
        df_valid = df[is_dict].copy()

        # On normalise la colonne json_col
        normalized = pd.json_normalize(df_valid[record_path], sep=sep)

        # On applique un préfixe si demandé
        if prefix:
            normalized.columns = [f"{prefix}{col}" for col in normalized.columns]

        # On récupère les colonnes meta
        meta = df_valid[meta_cols].reset_index(drop=True)

        # On concatène le tout
        result = pd.concat([meta, normalized], axis=1)

        # On gère les cas où json_col est vide ou invalide
        if not is_dict.all():
            fallback = df[~is_dict][meta_cols].copy()
            result = pd.concat([result, fallback], ignore_index=True)

        return result


def set_column(df: pd.DataFrame, col_name: str, value: str) -> pd.DataFrame:
    """
    Ajoute une colonne avec une valeur constante à un DataFrame
    :param df: DataFrame d'entrée
    :param col_name: nom de la nouvelle colonne
    :param value: valeur constante à ajouter
    :return: DataFrame avec la nouvelle colonne
    """
    df.loc[:, col_name] = value
    return df


def to_datetime(
    df: pd.DataFrame, columns: list[str], format=None, unit=None
) -> pd.DataFrame:
    for column in columns:
        df[column] = pd.to_datetime(
            df[column], format=format, unit=unit, errors="coerce"
        )
    return df


def cast_column(
    df: pd.DataFrame,
    columns: list[str],
    dtype: Any,
    format: str | None = None,
    utc: bool = False,
) -> pd.DataFrame:
    """
    Convertit le type de données d'une ou plusieurs colonnes d'un DataFrame.
    :param df: DataFrame d'entrée
    :param columns: liste des noms de colonnes à convertir
    :param dtype: type de données cible (par exemple, 'int', 'float', 'str')
    :param format: format de date à appliquer si dtype est 'datetime'
    :param utc: si True, convertit les dates en UTC
    :return: DataFrame avec les colonnes converties
    """
    try:
        for column in columns:
            if column in df.columns:
                if dtype == "datetime":
                    if not pd.api.types.is_datetime64_any_dtype(df[column]):
                        df[column] = pd.to_datetime(
                            df[column], errors="coerce", utc=utc
                        )
                    if format:
                        df[column] = df[column].dt.strftime(format)
                else:
                    df[column] = df[column].astype(dtype)
    except ValueError as e:
        print(
            f"Erreur lors de la conversion des colonnes {columns} en type {dtype}: {e}"
        )
        return df
    return df


def split_dataframe(
    df: pd.DataFrame, shared_cols: list[str], split_configs: SplitDFConfig
) -> dict[str, pd.DataFrame]:
    """
    Divise un DataFrame en plusieurs DataFrames basés sur des configurations de colonnes.
    :param df: DataFrame à diviser
    :param shared_cols: liste de colonnes partagées entre les DataFrames
    :param split_configs: dictionnaire de configurations pour chaque DataFrame à créer.
    :return: dict[str, pd.DataFrame]
    """
    result = {}
    for shared_col in shared_cols:
        if shared_col not in df.columns:
            shared_cols.remove(shared_col)
    for table_name, configs in split_configs.items():
        if prefix := configs.get("prefix"):
            columns = [col for col in df.columns if col.startswith(prefix)]
        else:
            columns = configs.get("columns", [])
            for col in columns:
                if col not in df.columns:
                    columns.remove(col)
        result[table_name] = df.loc[:, shared_cols + columns].copy()
        if configs.get("drop_duplicates", False):
            result[table_name] = result[table_name].drop_duplicates(
                subset=configs.get("subset", None)
            )
        if query := configs.get("query"):
            result[table_name] = filter_df(result[table_name], query)
    return result


def filter_df(df: pd.DataFrame, query: str, fields_to_check: list[str] | None = None) -> pd.DataFrame:
    """
    Filtre un DataFrame en fonction d'une requête.
    :param df: DataFrame à filtrer
    :param query: requête de filtrage
    :param fields_to_check: liste de champs à vérifier pour la requête
    :return: DataFrame filtré
    """
    if fields_to_check is None:
        fields_to_check = []
    for field in fields_to_check:
        if field not in df.columns:
            return df  # Si un champ requis n'existe pas, retourne le DataFrame original
    return df.query(query)


def dropna_df(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """
    Supprime les lignes contenant des valeurs manquantes dans un DataFrame.
    :param df: DataFrame à traiter
    :param columns: liste de colonnes à vérifier pour les valeurs manquantes (si None, vérifie toutes les colonnes)
    :return: DataFrame sans lignes avec des valeurs manquantes
    """
    if columns is None:
        return df.dropna().reset_index(drop=True)

    _columns = [col for col in columns if col in df.columns]

    return df[~df[_columns].isnull().all(axis=1)].reset_index(drop=True)

def _explode_in_n_chunks(
    df: pd.DataFrame, column: str, n: int
) -> Generator[pd.DataFrame, None, None]:
    """
    Divise un DataFrame en `n` parties, puis applique un `explode` sur une colonne contenant des listes.

    Args:
        df (pd.DataFrame): DataFrame à traiter
        column (str): colonne à exploser
        n (int): nombre de chunks souhaités

    Yields:
        pd.DataFrame: une portion explosée du DataFrame
    """
    splits = np.array_split(df, n)

    for chunk in splits:
        chunk = chunk.copy()

        # Convertir les valeurs non-listes en liste pour éviter explode sur types simples
        chunk[column] = chunk[column].apply(
            lambda x: x if isinstance(x, list) else [x] if pd.notnull(x) else []
        )

        exploded = chunk.explode(column, ignore_index=True)
        yield exploded
        gc.collect()


def explode_dataframe(df: pd.DataFrame, column: str, n_chunks: int = 1) -> pd.DataFrame:
    """
    Explose une colonne d'un DataFrame en plusieurs lignes, en divisant le DataFrame en `n_chunks` parties.

    Args:
        df (pd.DataFrame): DataFrame à traiter
        column (str): colonne à exploser
        n_chunks (int): nombre de chunks pour la division

    Returns:
        pd.DataFrame: DataFrame avec la colonne explosée
    """
    if n_chunks <= 1:
        return df.explode(column, ignore_index=True)

    exploded_dfs = list(_explode_in_n_chunks(df, column, n_chunks))
    return pd.concat(exploded_dfs, ignore_index=True)


def explode(df: pd.DataFrame, column: str, ignore_index: bool = True) -> pd.DataFrame:
    """
    Explose une colonne d'un DataFrame en plusieurs lignes.

    Args:
        df (pd.DataFrame): DataFrame à traiter
        column (str): colonne à exploser
        ignore_index (bool): si True, réinitialise l'index du DataFrame résultant

    Returns:
        pd.DataFrame: DataFrame avec la colonne explosée
    """
    if column not in df.columns:
        return df
    return df.explode(column, ignore_index=ignore_index)


def safe_serialize(x):
    if isinstance(x, (list, dict)):
        return json.dumps(x, sort_keys=True)
    if isinstance(x,  np.ndarray):
        return safe_serialize(x.tolist())
    return x


def drop_duplicates_df(
    df: pd.DataFrame, subset: Optional[list[str]] = None
) -> pd.DataFrame:
    """
    Supprime les doublons d'un DataFrame en fonction d'un sous-ensemble de colonnes.

    Args:
        df (pd.DataFrame): DataFrame à traiter
        subset (list[str], optional): colonnes à considérer pour la suppression des doublons

    Returns:
        pd.DataFrame: DataFrame sans doublons
    """

    try:
        return df.drop_duplicates(subset=subset).reset_index(drop=True)
    except TypeError:
        print("TypeError encountered, applying safe serialization")
        print(f"COlon names: {df.columns.tolist()}")
        df = df.map(safe_serialize)
        return df.drop_duplicates(subset=subset).reset_index(drop=True)
