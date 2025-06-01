from typing import Any

from modules.interface import JSONNormalizeType
from modules.utils import reverse_mapping
from modules.utils_transformation import SplitDFConfig

MAPPING_TABLES = {
    "fleet_vehicle_stats_faultCodes_obdii": "fleet_vehicle_stats_faultCodes",
    "fleet_vehicle_stats_faultCodes_j1939": "fleet_vehicle_stats_faultCodes",
    "ReeferStats_engineHours": "fleet_assets_reefers",
    "ReeferStats_fuelPercentage": "fleet_assets_reefers",
    "ReeferStats_returnAirTemperature": "fleet_assets_reefers",
    "ReeferStats_ambientAirTemperature": "fleet_assets_reefers",
    "ReeferStats_dischargeAirTemperature": "fleet_assets_reefers",
    "ReeferStats_setPoint": "fleet_assets_reefers",
    "ReeferStats_powerStatus": "fleet_assets_reefers",
    "ReeferStats_reeferAlarms": "fleet_assets_reefers",
    "fleet_tags_vehicles": "fleet_tags",
    "fleet_tags_assets": "fleet_tags",
    "fleet_tags_sensors": "fleet_tags",
    "fleet_tags_addresses": "fleet_tags",
    "fleet_tags_drivers": "fleet_tags",
    "fleet_devices_healthReasons": "fleet_devices",
    "fleet_devices_health": "fleet_devices",
}

REVERSED_MAPPING_TABLES = reverse_mapping(MAPPING_TABLES)

def transformation_configs(**kwargs) -> dict[str, dict[int, dict]]:
    return {
        # EV
        "fleet_vehicle_stats_evAverageBatteryTemperatureMilliCelsius": get_standard_transformation_config(
            "evAverageBatteryTemperatureMilliCelsius"
        ),
        "fleet_vehicle_stats_evBatteryStateOfHealthMilliPercent": get_standard_transformation_config(
            "evBatteryStateOfHealthMilliPercent"
        ),
        "fleet_vehicle_stats_evChargingCurrentMilliAmp": get_standard_transformation_config(
            "evChargingCurrentMilliAmp"
        ),
        "fleet_vehicle_stats_evChargingEnergyMicroWh": get_standard_transformation_config("evChargingEnergyMicroWh"),
        "fleet_vehicle_stats_evChargingStatus": get_standard_transformation_config("evChargingStatus"),
        "fleet_vehicle_stats_evChargingVoltageMilliVolt": get_standard_transformation_config(
            "evChargingVoltageMilliVolt"
        ),
        "fleet_vehicle_stats_evConsumedEnergyMicroWh": get_standard_transformation_config("evConsumedEnergyMicroWh"),
        "fleet_vehicle_stats_evDistanceDrivenMeters": get_standard_transformation_config("evDistanceDrivenMeters"),
        "fleet_vehicle_stats_evRegeneratedEnergyMicroWh": get_standard_transformation_config(
            "evRegeneratedEnergyMicroWh"
        ),
        "fleet_vehicle_stats_evStateOfChargeMilliPercent": get_standard_transformation_config(
            "evStateOfChargeMilliPercent"
        ),

        # Thermique
        "fleet_vehicle_stats_obdEngineSeconds": get_standard_transformation_config("obdEngineSeconds"),
        "fleet_vehicle_stats_engineStates": get_standard_transformation_config("engineStates"),
        "fleet_vehicle_stats_gpsOdometerMeters": get_standard_transformation_config("gpsOdometerMeters"),
        "fleet_vehicle_stats_ecuSpeedMph": get_standard_transformation_config("ecuSpeedMph"),
        "fleet_vehicles_fuel_energy": index_transformations(
            get_trans_to_set_df_column("date", value=kwargs.get("fuel_energy_date", None)),
            get_trans_to_cast_column_type(
                ["efficiencyMpge", "energyUsedKwh", "estFuelEnergyCost_amount", "estCarbonEmissionsKg"],
                dtype=float
            ),
            get_trans_to_rename_columns({"vehicle_name": "parc_id"}),
            get_trans_to_cast_column_type(["date"], dtype="datetime", utc=True),
        ),
        "fleet_vehicle_stats_faultCodes": index_transformations(
            get_trans_to_explode_df("faultCodes"),
            get_trans_to_json_normalize_df("faultCodes"),
            get_trans_to_cast_column_type(["time"], dtype="datetime", utc=True),
        ),
        "fleet_vehicle_stats_faultCodes_obdii": index_transformations(
            get_trans_to_rename_columns({
                "obdii_checkEngineLightIsOn": "obdii_engine_light_on"
            }),
            get_trans_to_explode_df("obdii_diagnosticTroubleCodes"),
            get_trans_to_json_normalize_df("obdii_diagnosticTroubleCodes", prefix="obdii_"),
            get_trans_to_explode_df("obdii_pendingDtcs"),
            get_trans_to_explode_df("obdii_permanentDtcs"),
            get_trans_to_explode_df("obdii_confirmedDtcs"),
            get_trans_to_json_normalize_df("obdii_confirmedDtcs", prefix="obdii_confirmed_"),
            get_trans_to_json_normalize_df("obdii_pendingDtcs", prefix="obdii_pending_"),
            get_trans_to_json_normalize_df("obdii_permanentDtcs", prefix="obdii_permanent_"),
            get_trans_to_filter_df("obdii_engine_light_on.notna() or obdii_txId.notna() or obdii_confirmed_dtcId.notna() or obdii_pending_dtcId.notna() or obdii_permanent_dtcId.notna()"),
        ),
        "fleet_vehicle_stats_faultCodes_j1939": index_transformations(
            get_trans_to_rename_columns({
                "j1939_checkEngineLights_protectIsOn": "j1939_protect_engine_light_On",
                "j1939_checkEngineLights_warningIsOn": "j1939_warning_engine_light_On",
                "j1939_checkEngineLights_stopIsOn": "j1939_stop_engine_light_On",
                "j1939_checkEngineLights_emissionsIsOn": "j1939_emissions_engine_light_On",
            }),
            get_trans_to_explode_df("j1939_diagnosticTroubleCodes"),
            get_trans_to_json_normalize_df("j1939_diagnosticTroubleCodes", prefix="j1939_diag_trouble_codes_"),
            get_trans_to_filter_df("j1939_emissions_engine_light_On.notna() or j1939_protect_engine_light_On.notna() or j1939_stop_engine_light_On.notna() or j1939_warning_engine_light_On.notna() or j1939_diag_trouble_codes_spnId.notna() or j1939_diag_trouble_codes_fmiId.notna()"),
        ),
        # split des tables
        "fleet_vehicle_stats_faultCodes_split": index_transformations(
            get_trans_to_split_df(
                shared_cols=["vehicle_id", "parc_id", "time", "canBusType"],
                split_configs={
                    "fleet_vehicle_stats_faultCodes_obdii": {
                        "prefix": "obdii_",
                    },
                    "fleet_vehicle_stats_faultCodes_j1939": {
                        "prefix": "j1939_",
                    },
                },
                drop_duplicates=True,
            ),
            include_default_trans=False
        ),
        "fleet_assets_reefers_split": index_transformations(
            get_trans_to_split_df(
                shared_cols=["assetType", "id", "name"],
                split_configs={
                    "ReeferStats_engineHours": {
                        "columns": ["ReeferStats_engineHours"],
                    },
                    "ReeferStats_fuelPercentage": {
                        "columns": ["ReeferStats_fuelPercentage"],
                    },
                    "ReeferStats_returnAirTemperature": {
                        "columns": ["ReeferStats_returnAirTemperature"],
                    },
                    "ReeferStats_ambientAirTemperature": {
                        "columns": ["ReeferStats_ambientAirTemperature"],
                    },
                    "ReeferStats_dischargeAirTemperature": {
                        "columns": ["ReeferStats_dischargeAirTemperature"],
                    },
                    "ReeferStats_setPoint": {
                        "columns": ["ReeferStats_setPoint"],
                    },
                    "ReeferStats_powerStatus": {
                        "columns": ["ReeferStats_powerStatus"],
                    },
                    "ReeferStats_reeferAlarms": {
                        "columns": ["ReeferStats_reeferAlarms"],
                    }
                },
            ),
            include_default_trans=False
        ),
        "fleet_tags_split": index_transformations(
            get_trans_to_split_df(
                shared_cols=["tag_id", "tag_name", "externalIds_samsara_name", "parentTagId", "parentTag_id",
                             "parentTag_name"],
                split_configs={
                    "fleet_tags_vehicles": {
                        "columns": ["vehicles"]
                    },
                    "fleet_tags_assets": {
                        "columns": ["assets"]
                    },
                    "fleet_tags_sensors": {
                        "columns": ["sensors"]
                    },
                    "fleet_tags_addresses": {
                        "columns": ["addresses"]
                    },
                    "fleet_tags_drivers": {
                        "columns": ["drivers"]
                    }
                },
            ),
            include_default_trans=False
        ),
        "fleet_devices_split": index_transformations(
            get_trans_to_split_df(
                shared_cols=["asset_id", "asset_name", ],
                split_configs={
                    "fleet_devices_healthReasons": {
                        "columns": ["health_healthReasons"],
                    },
                    "fleet_devices": {
                        "columns": [
                            "serial",
                            "model",
                            "lastConnectedTime",
                            "lastKnownLocation_latitude",
                            "lastKnownLocation_longitude",
                            "lastKnownLocation_id",
                        ]
                    },
                    "fleet_devices_health": {
                        "prefix": "health_",
                    }
                }
            )
        ),
        # "": get_standard_transformation_config(),
        "fleet_safety_events": index_transformations(
            get_trans_to_cast_column_type(["time"], dtype="datetime", utc=True),
            get_trans_to_explode_df("behaviorLabels"),
            get_trans_to_json_normalize_df("behaviorLabels", prefix="behavior_"),
            get_trans_to_rename_columns({"id": "event_id"})
        ),
        # Groupe froid
        "ReeferStats_engineHours": index_transformations(
            get_trans_to_explode_df("ReeferStats_engineHours"),
            get_trans_to_json_normalize_df("ReeferStats_engineHours"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({"changedAtMs": "engineHoursChangedAt"}),
        ),
        "ReeferStats_fuelPercentage": index_transformations(
            get_trans_to_explode_df("ReeferStats_fuelPercentage"),
            get_trans_to_json_normalize_df("ReeferStats_fuelPercentage"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({"changedAtMs": "fuelPercentageChangedAt"}),
        ),
        "ReeferStats_returnAirTemperature": index_transformations(
            get_trans_to_explode_df("ReeferStats_returnAirTemperature"),
            get_trans_to_json_normalize_df("ReeferStats_returnAirTemperature"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({
                "changedAtMs": "returnAirTemperatureChangedAt",
                "tempInMilliC": "returnAirTemperatureMilliCelsius"
            }),
        ),
        "ReeferStats_ambientAirTemperature": index_transformations(
            get_trans_to_explode_df("ReeferStats_ambientAirTemperature"),
            get_trans_to_json_normalize_df("ReeferStats_ambientAirTemperature"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({
                "changedAtMs": "ambientAirTemperatureChangedAt",
                "tempInMilliC": "ambientAirTemperatureMilliCelsius"
            }),
        ),
        "ReeferStats_dischargeAirTemperature": index_transformations(
            get_trans_to_explode_df("ReeferStats_dischargeAirTemperature"),
            get_trans_to_json_normalize_df("ReeferStats_dischargeAirTemperature"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({
                "changedAtMs": "dischargeAirTemperatureChangedAt",
                "tempInMilliC": "dischargeAirTemperatureMilliCelsius"
            }),
        ),
        "ReeferStats_setPoint": index_transformations(
            get_trans_to_explode_df("ReeferStats_setPoint"),
            get_trans_to_json_normalize_df("ReeferStats_setPoint"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({
                "changedAtMs": "setPointChangedAt",
                "tempInMilliC": "setPointMilliCelsius"
            }),
        ),
        "ReeferStats_powerStatus": index_transformations(
            get_trans_to_explode_df("ReeferStats_powerStatus"),
            get_trans_to_json_normalize_df("ReeferStats_powerStatus"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_rename_columns({
                "changedAtMs": "powerStatusChangedAt",
                "status": "powerStatus"
            }),
        ),
        "ReeferStats_reeferAlarms": index_transformations(
            get_trans_to_explode_df("ReeferStats_reeferAlarms"),
            get_trans_to_json_normalize_df("ReeferStats_reeferAlarms"),
            get_trans_timestamp_to_datetime("changedAtMs", unit="ms"),
            get_trans_to_explode_df("alarms"),
            get_trans_to_json_normalize_df("alarms", prefix="alarm_"),
            get_trans_to_rename_columns({"changedAtMs": "reeferAlarmsChangedAt"}),
        ),
        # Non temporel
        "fleet_vehicles": index_transformations(
            get_trans_to_drop_columns(["tags", "sensorConfiguration_areas"]),
            get_trans_to_rename_columns({"id": "vehicle_id", "name": "parc_id"}),
            get_trans_to_drop_columns(["sensorConfiguration_doors", "attributes"]),
            include_default_trans=False
        ),
        "fleet_assets": index_transformations(
            get_trans_to_explode_df("assets"),
            get_trans_to_set_df_column("test", value="test_value"),
            get_trans_to_json_normalize_df("assets"),
            get_trans_to_rename_columns({"id": "asset_id", "name": "asset_name"}),
            get_trans_to_drop_columns(["test"]),
            include_default_trans=False
        ),
        "fleet_trailers": index_transformations(
            get_trans_to_drop_columns(["tags"]),
            get_trans_to_rename_columns({"id": "vehicle_id", "name": "parc_id"}),
            include_default_trans=False
        ),
        "fleet_tags": index_transformations(
            get_trans_to_rename_columns({"id": "tag_id", "name": "tag_name"}),
            get_trans_to_drop_columns(["machines"]),
            include_default_trans=False
        ),
        "fleet_tags_vehicles": index_transformations(
            get_trans_to_explode_df("vehicles"),
            get_trans_to_json_normalize_df("vehicles", prefix="vehicle_"),
        ),
        "fleet_tags_assets": index_transformations(
            get_trans_to_explode_df("assets"),
            get_trans_to_json_normalize_df("assets", prefix="asset_"),
        ),
        "fleet_tags_sensors": index_transformations(
            get_trans_to_explode_df("sensors"),
            get_trans_to_json_normalize_df("sensors", prefix="sensor_"),
        ),
        "fleet_tags_addresses": index_transformations(
            get_trans_to_explode_df("addresses"),
            get_trans_to_json_normalize_df("addresses", prefix="address_"),
        ),
        "fleet_tags_drivers": index_transformations(
            get_trans_to_explode_df("drivers"),
            get_trans_to_json_normalize_df("drivers", prefix="driver_"),
        ),
        "fleet_devices": index_transformations(),
        "fleet_devices_health": index_transformations(
            get_trans_to_drop_columns(["health_healthReasons"])
        ),
        "fleet_devices_healthReasons": index_transformations(
            get_trans_to_explode_df("health_healthReasons"),
            get_trans_to_json_normalize_df("health_healthReasons")
        ),
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
            "split_configs": split_configs
        }
    }

def get_trans_to_drop_duplicates_df(subset=None) -> dict:
    return {
        "type_function": "is_custom_function",
        "function": "drop_duplicates",
        "kwargs": {"subset": subset},
    }

def get_trans_to_default_trans() -> list[dict]:
    return [
        get_trans_to_rename_columns({"id": "vehicle_id", "name": "parc_id"}), # "id": "samsara_id",
        get_trans_to_drop_columns([
            "externalIds_samsara_serial",
            "externalIds_samsara_vin",
            "vehicle_externalIds_samsara_serial",
            "vehicle_externalIds_samsara_vin"
        ]),
        get_trans_to_cast_column_type(["time"], dtype="datetime", utc=True),
        get_trans_to_drop_duplicates_df()
    ]

def get_trans_to_cast_column_type(columns:list[str], dtype, format:str | None = None, utc=False) -> dict:
    return {
        "type_function": "is_custom_function",
        "function": "cast_column",
        "kwargs": {"columns": columns, "dtype": dtype, "format": format, "utc": utc},
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

def get_trans_to_explode_df(column_name: str, ignore_index: bool = True) -> dict:
    return {
        "type_function": "is_df_function",
        "function": "explode",
        "kwargs": {"column": column_name, "ignore_index": ignore_index},
    }

def get_trans_timestamp_to_datetime(column_name: str, unit=None) -> dict:
    return {
        "type_function": "is_custom_function",
        "function": "to_datetime",
        "kwargs": {"columns": [column_name], "unit": unit},
    }

def get_trans_to_json_normalize_df(column_name: str, record_path: str=None, sep: str = "_", prefix: str | None = "", record_prefix=None, function: JSONNormalizeType = JSONNormalizeType.JSON_NORMALIZE) -> dict:
    if record_path is None:
        record_path = column_name
    return {
        "column_name": column_name,
        "type_function": "is_custom_function",
        "function": f"{function.value if isinstance(function, JSONNormalizeType) else function}",
        "kwargs": {"sep": sep, "record_path": record_path, "prefix": prefix, "record_prefix": record_prefix}, #
    }
def index_transformations(*transformations, current_index: int = 0, include_default_trans=True) -> dict[int, dict]:
    if include_default_trans:
        transformations = list(transformations) + get_trans_to_default_trans()
    return {index: trans for index, trans in enumerate(transformations, start=current_index + 1)}

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