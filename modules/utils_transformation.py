import pandas as pd

def json_normalize(df: pd.DataFrame, record_path: str, sep: str='_',prefix: str = '', meta_cols: list[str] = None)->pd.DataFrame:
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
                normalized = pd.json_normalize(item, sep=sep)
                for col in normalized.columns:
                    normalized.rename(columns={col: prefix + col}, inplace=True)
                combined = {**base_data, **normalized.iloc[0].to_dict()}
                all_rows.append(combined)
        elif isinstance(raw_json, dict):  # un seul objet JSON
            normalized = pd.json_normalize(raw_json, sep=sep)
            for col in normalized.columns:
                normalized.rename(columns={col: prefix + col}, inplace=True)
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

def to_datetime(df: pd.DataFrame, columns: list[str], format=None)->pd.DataFrame:
    for column in columns:
        df[column] = pd.to_datetime(df[column], format=format)
    return df

def get_standard_transformation_config(column_name: str, record_path: str=None, sep: str = "_", drop_column: bool = True):
    """
    Fonction de configuration standard pour les transformations de données
    :param column_name: nom de la colonne à transformer
    :param record_path: chemin d'enregistrement pour json_normalize
    :param sep: séparateur pour json_normalize
    :param drop_column: si True, la colonne d'origine sera supprimée
    :return: dictionnaire de configuration
    """
    if record_path is None:
        record_path = column_name
    return {
        1: {
            "type_function": "is_df_function",
            "function": "explode",
            "kwargs": {"column": column_name},
        },
        2: {
            "column_name": column_name,
            "type_function": "is_custom_function",
            "function": "json_normalize",
            "kwargs": {"sep": sep, "record_path": record_path},
        },
        3: {
            "type_function": "is_df_function",
            "function": "drop",
            "kwargs": {"columns": [column_name], "errors": "ignore"},
        },
        4: {
            "type_function": "is_df_function",
            "function": "rename",
            "kwargs": {"columns": {"value": column_name}},
        },
        # 5: {
        #     "type_function": "is_custom_function",
        #     "function": to_datetime,
        #     "kwargs": {"columns": ["time"],"format":"%Y-%m-%dT%H:%M:%S.%fZ"},
        # },
    }
