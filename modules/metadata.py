from datetime import datetime
from typing import Optional

import pandas as pd

from .interface import ColumnToUpdate, DownloadType, TypeFilter
from .metadata_catalog import load_metadata_catalog
from .utils import (
    DEFAULT_RATE_LIMIT_SECOND,
    DEFAULT_START_DATE,
)

pd.set_option('display.max_columns', None)

REQUIRED_METADATA_COLUMNS = {
    "family",
    "table_name",
    "endpoint",
    "params",
    "rate_limit_per_seconde",
    "download_type",
}


def validate_metadata(metadata: pd.DataFrame) -> pd.DataFrame:
    """Fail fast when an endpoint definition is ambiguous or incomplete."""
    missing_columns = REQUIRED_METADATA_COLUMNS.difference(metadata.columns)
    if missing_columns:
        raise ValueError(
            "Colonnes de métadonnées manquantes: " + ", ".join(sorted(missing_columns))
        )

    required_non_null = ["family", "table_name", "endpoint", "download_type"]
    invalid_rows = metadata[required_non_null].isna().any(axis=1)
    if invalid_rows.any():
        rows = metadata.index[invalid_rows].tolist()
        raise ValueError(f"Métadonnées incomplètes aux lignes: {rows}")

    duplicates = metadata.loc[
        metadata["table_name"].duplicated(keep=False), "table_name"
    ].unique()
    if len(duplicates):
        raise ValueError(
            "Noms de tables dupliqués: " + ", ".join(sorted(duplicates.tolist()))
        )

    allowed_download_types = {item.value for item in DownloadType}
    invalid_types = set(metadata["download_type"].dropna()) - allowed_download_types
    if invalid_types:
        raise ValueError(
            "Types de téléchargement invalides: " + ", ".join(sorted(invalid_types))
        )

    rates = pd.to_numeric(metadata["rate_limit_per_seconde"], errors="coerce")
    rates = rates.fillna(DEFAULT_RATE_LIMIT_SECOND)
    if (rates <= 0).any():
        raise ValueError("rate_limit_per_seconde doit être un nombre strictement positif")
    metadata = metadata.copy()
    metadata["rate_limit_per_seconde"] = rates
    return metadata


def is_exception_date(row: pd.Series) -> bool:
    if row.get("is_exception") == 1:
        if row.get("exception_config", {}).get("exception_type") == "date":
            if row.get("exception_config").get("constraint") == "is_data_but_datetime":
                return True
    return False


def is_exception_date_only_start_date(row: pd.Series) -> bool:
    if row.get("is_exception") == 1:
        if row.get("exception_config", {}).get("exception_type") == "date":
            if row.get("exception_config").get("constraint") == "only_start_date":
                return True
    return False


def is_exception_table(row: pd.Series) -> bool:
    if row.get("is_exception") == 1:
        if row.get("exception_config", {}).get("exception_type") == "table":
            return True
    return False


def is_exception(row: pd.Series) -> bool:
    if row.get("is_exception") == 1:
        return True
    return False


def is_date_range(row: pd.Series) -> bool:
    if not is_exception(row):
        params = row.get("params")
        if not pd.isna(params) and any(
            ["endDate" in params, "endTime" in params, "endMs" in params]
        ):
            return True
    return False


def is_vehicle_stats(row: pd.Series) -> bool:
    return row.get("family") == "vehicle_stats"


def is_downloadable_oneshot(row: pd.Series) -> bool:
    return (
        not is_vehicle_stats(row) and not is_exception(row) and not is_date_range(row)
    )


def get_metadata_by_table_names(
    metadata: pd.DataFrame, table_names: list[str]
) -> pd.DataFrame:
    # metadata = make_meta_data("metadata.xlsx", start_time, end_time)
    return metadata[metadata["table_name"].isin(table_names)]


def get_metadata(
    metadata: pd.DataFrame,
    table_names: list[str] = None,
    filter_: Optional[TypeFilter] = TypeFilter.ALL,
) -> pd.DataFrame:
    if table_names is not None:
        metadata = get_metadata_by_table_names(metadata, table_names)
    if filter_ == TypeFilter.EXCEPTION_DATE:
        return metadata[metadata.apply(is_exception_date, axis=1)]
    elif filter_ == TypeFilter.EXCEPTION_TABLE:
        return metadata[metadata.apply(is_exception_table, axis=1)]
    elif filter_ == TypeFilter.DATE_RANGE:
        return metadata[metadata.apply(is_date_range, axis=1)]
    elif filter_ == TypeFilter.VEHICLE_STATS:
        return metadata[metadata.apply(is_vehicle_stats, axis=1)]
    elif filter_ == TypeFilter.DOWNLOADABLE_ONESHOT:
        return metadata[metadata.apply(is_downloadable_oneshot, axis=1)]
    elif filter_ == TypeFilter.EXCEPTION:
        return metadata[metadata.apply(is_exception, axis=1)]
    elif filter_ == TypeFilter.EXCEPTION_DATE_ONLY_START_DATE:
        return metadata[metadata.apply(is_exception_date_only_start_date, axis=1)]

    return metadata


def build_metadata(
    configs_for_update: dict,
    table_names: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    use_configured_start: bool = True,
) -> pd.DataFrame:
    if start_date is None:
        start_date = DEFAULT_START_DATE
    if end_date is None:
        end_date = datetime.now().strftime("%d/%m/%Y")
    end_points = []
    for table_name in table_names:
        table_start_date = start_date
        if table_configs := configs_for_update.get(table_name, {}):
            if table_configs.get("download_type") == DownloadType.TIME.value:
                last_update_time = table_configs.get(ColumnToUpdate.DOWNLOAD.value, None)
                if use_configured_start and last_update_time is not None:
                    table_start_date = last_update_time
        end_point = make_meta_data(start_time=table_start_date, end_time=end_date).query(
            "table_name == @table_name"
        )
        if end_point.empty:
            continue

        if pd.isnull(download_type := end_point.iloc[0].get("download_type")):
            if (download_type := table_configs.get("download_type")) is None:
                download_type = DownloadType.TIME.value

        end_point = end_point.assign(download_type=download_type)
        end_point = end_point.assign(**{ColumnToUpdate.DOWNLOAD.value: table_start_date})
        end_points.append(end_point)
    return pd.concat(end_points, ignore_index=True) if end_points else pd.DataFrame()


def make_meta_data(
    start_time: str, end_time: str, metadata_filename: str | None = None
) -> pd.DataFrame:
    """Build validated endpoint metadata from the versioned JSON catalog."""
    data = load_metadata_catalog(start_time, end_time)
    df = validate_metadata(pd.DataFrame(data))
    if metadata_filename:
        df.to_excel(metadata_filename, index=False, header=True)
    return df


def get_tables_default_table_names() -> list[str]:
    table_names: list[str] = []
    for table in get_table_name_by_category().values():
        table_names.extend(table)
    return table_names


def get_table_name_by_category() -> dict[str, list[str]]:
    """
    Returns a list of table names for a given category.
    """
    return {
        "ev": [
            "fleet_vehicle_stats_evAverageBatteryTemperatureMilliCelsius",
            "fleet_vehicle_stats_evBatteryStateOfHealthMilliPercent",
            "fleet_vehicle_stats_evChargingCurrentMilliAmp",
            "fleet_vehicle_stats_evChargingEnergyMicroWh",
            "fleet_vehicle_stats_evChargingStatus",
            "fleet_vehicle_stats_evChargingVoltageMilliVolt",
            "fleet_vehicle_stats_evConsumedEnergyMicroWh",
            "fleet_vehicle_stats_evDistanceDrivenMeters",
            "fleet_vehicle_stats_evRegeneratedEnergyMicroWh",
            "fleet_vehicle_stats_evStateOfChargeMilliPercent",
        ],
        "time": [
            "fleet_vehicle_stats_obdEngineSeconds",
            "fleet_vehicle_stats_engineStates",
            "fleet_vehicle_stats_gpsOdometerMeters",
            "fleet_vehicle_stats_obdOdometerMeters",
            "fleet_vehicle_stats_ecuSpeedMph",
            "fleet_vehicles_fuel_energy",
            "fleet_vehicle_stats_faultCodes",
            "fleet_safety_events",
            "fleet_assets_reefers",
            "fleet_vehicle_idling",
        ],
        "stats": [
            "fleet_vehicle_stats_intakeManifoldTemperatureMilliC",
            "fleet_vehicle_stats_engineRpm",
            "fleet_vehicle_stats_engineOilPressureKPa",
            "fleet_vehicle_stats_engineLoadPercent",
            "fleet_vehicle_stats_engineImmobilizer",
            "fleet_vehicle_stats_engineCoolantTemperatureMilliC",
            "fleet_vehicle_stats_defLevelMilliPercent",
            "fleet_vehicle_stats_batteryMilliVolts",
            "fleet_vehicle_stats_barometricPressurePa",
            "fleet_vehicle_stats_ambientAirTemperatureMilliC",
        ],
        "core": [
            "fleet_vehicles",  # ok
            "fleet_assets",  # ok
            "fleet_trailers",  # ok
            "fleet_tags",  # ok
            "fleet_devices",  # ok
        ],
    }
