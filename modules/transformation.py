import pandas as pd

from .transformation_configs import transformation_configs
from .logs import MyLogger

from .utils_transformation import *


class TransformData:

    _tables_to_split = [
        "fleet_vehicle_stats_faultCodes",
        "fleet_assets_reefers",
        "fleet_tags",
        "fleet_devices",
    ]

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
        self.logger = MyLogger("TransformData")
        self.except_table_names: list[str] | None = None

    def set_data(self, data: pd.DataFrame, endpoint_info: dict, **kwargs) -> "TransformData":
        """
        Définit les données à transformer et les informations de l'endpoint
        :param data:
        :param endpoint_info:
        :return: self
        """
        self.df = data
        self.endpoint_info = endpoint_info
        if except_table_names := kwargs.get("except_table_names", None):
            self.except_table_names = except_table_names
        return self

    def _get_transform_config(self) -> dict:
        """
        Récupère la configuration de transformation pour les données
        :return: dict
        """
        table_name = self.endpoint_info.get("table_name")
        value: str | None = self.endpoint_info.pop("date_str", None)
        if table_name == "fleet_vehicles_fuel_energy" and value:
            value = value.replace("_", "-")

        configs = transformation_configs(**{"fuel_energy_date": value})
        return configs.get(table_name, {})

    def transform(self) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """
        Applique les transformations définies dans la configuration sur le DataFrame
        :param except_table_names: liste des noms de tables à exclure de la transformation
        :return:
        """
        if self.except_table_names and self.endpoint_info.get('table_name') in self.except_table_names:
            # self.logger.info(
            #     f"Transformation ignorée pour la table {self.endpoint_info.get('table_name')}"
            # )
            return self.df
        config = self._get_transform_config()
        function_name = "Unknown"
        step_config = {}
        try:
            # self.logger.info(f"Taille initiale du DataFrame: {self.df.shape if isinstance(self.df, pd.DataFrame) else {k: v.shape for k, v in self.df.items()}}")
            n_configs = len(config)
            for step, step_config in config.items():
                function_name = step_config.get("function")
                # self.logger.info(
                #     f"Transformation étape: {step}/{n_configs} - Table: {self.endpoint_info.get('table_name')} - Fonction: {function_name}"
                # )
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
            self.logger.error(
                f"Erreur lors de la transformation des données: {e} pour la table {self.endpoint_info.get('table_name')} avec la fonction {function_name} et la configuration {step_config}"
            )
        return self.df

    def split_data(self, df: pd.DataFrame, table_name: str, **kwargs) -> dict[str, pd.DataFrame]:
        """
        Divise les données en plusieurs DataFrames basés sur la configuration de transformation
        :param df: DataFrame à diviser
        :param table_name: nom de la table
        :return: dict[str, pd.DataFrame]
        """
        if table_name in self.tables_to_split:
            results: dict[str, pd.DataFrame] = self.set_data(df, {"table_name": f"{table_name}_split"}).transform()
            for table_name in results:
                if not results[table_name].empty:
                    results[table_name] = self.set_data(results[table_name], {"table_name": table_name}).transform()
        else:
            results = {table_name: df}

        return results