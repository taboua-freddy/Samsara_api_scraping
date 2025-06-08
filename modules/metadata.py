from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from .interface import TypeFilter, DownloadType, ColumnToUpdate
from .utils import (
    timestamp_to_timestamp_ms,
    date_to_timestamp,
    date_to_iso_or_timestamp,
    cast_date, DEFAULT_START_DATE,
)


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
) -> pd.DataFrame:
    if start_date is None:
        start_date = DEFAULT_START_DATE
    if end_date is None:
        end_date = datetime.now().strftime("%d/%m/%Y")
    end_points = []
    for table_name in table_names:
        if table_configs := configs_for_update.get(table_name, {}):
            if table_configs.get("download_type") == DownloadType.TIME.value:
                last_update_time = table_configs.get("last_update_time", None)
                if last_update_time is not None:
                    start_date = last_update_time
        end_point = make_meta_data(start_time=start_date, end_time=end_date).query(
            "table_name == @table_name"
        )
        if end_point.empty:
            continue

        if pd.isnull(download_type := end_point.iloc[0].get("download_type")):
            if (download_type := table_configs.get("download_type")) is None:
                download_type = DownloadType.TIME.value

        end_point = end_point.assign(download_type=download_type)
        end_point = end_point.assign(**{ColumnToUpdate.DOWNLOAD.value: start_date})
        end_points.append(end_point)
    return pd.concat(end_points, ignore_index=True) if end_points else pd.DataFrame()


def make_meta_data(
    start_time: str, end_time: str, metadata_filename: str | None = None
) -> pd.DataFrame:
    """
    Crée un fichier de métadonnées pour les endpoints
    :param metadata_filename:
    :param start_time:
    :param end_time:
    :param make_file:
    :return:
    """
    samsara_vehicle_id = "vehicle_id"
    data = [
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evStateOfChargeMilliPercent",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evStateOfChargeMilliPercent,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Milli percent state of charge for electric and hybrid vehicles. Not all EV and HEVs may report this field.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evChargingStatus",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evChargingStatus,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Charging status of the battery (e.g. charging, discharging, idle).",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evChargingEnergyMicroWh",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evChargingEnergyMicroWh,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Energy consumed by the battery during charging or discharging. Positive value indicates charging, negative value indicates discharging.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evChargingVoltageMilliVolt",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evChargingVoltageMilliVolt,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Voltage of the battery during charging. Millivolt value.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evChargingCurrentMilliAmp",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evChargingCurrentMilliAmp,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Current of the battery during charging. Milliamp value.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evConsumedEnergyMicroWh",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evConsumedEnergyMicroWh,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Energy consumed by the battery during driving. ",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evRegeneratedEnergyMicroWh",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evRegeneratedEnergyMicroWh,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Energy regenerated by the battery during driving.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evBatteryStateOfHealthMilliPercent",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evBatteryStateOfHealthMilliPercent,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Milli percent battery state of health for electric and hybrid vehicles. ",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evAverageBatteryTemperatureMilliCelsius",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evAverageBatteryTemperatureMilliCelsius,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Average battery temperature for electric and hybrid vehicles.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_evDistanceDrivenMeters",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=evDistanceDrivenMeters,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 60,
            "description": "Distance driven by electric and hybrid vehicles.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "core",
            "table_name": "addresses",
            "endpoint": "addresses",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Liste des adresses de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "core",
            "table_name": "alerts_configurations",
            "endpoint": "alerts/configurations",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Liste des configurations d'alertes.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "alerts",
            "table_name": "alerts_incidents",
            "endpoint": "alerts/incidents/stream",
            "params": "configurationIds={configurationIds}"
            + ",startTime={startTime},endTime={endTime}".format(
                startTime=start_time, endTime=end_time
            ),
            "is_processed": 0,
            "rate_limit_per_seconde": 10,
            "description": "Liste des incidents d'alertes pour des configurations données par configurationIds.",
            "is_exception": 1,
            "exception_config": {
                "exception_type": "table",
                "constraint": "dynamic_url",
                "table_name": "alerts_configurations",
                "table_column_name": f"{samsara_vehicle_id}",
                "exception_param_name": "configurationIds",
                "key_to_apply_on": "params",
                "is_list": 1,
            },
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "assets",
            "table_name": "fleet_assets",
            "endpoint": "v1/fleet/assets",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Liste des actifs de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "assets",
            "table_name": "fleet_assets_reefers",
            "endpoint": "v1/fleet/assets/reefers",
            "params": f"startMs={timestamp_to_timestamp_ms(date_to_timestamp(start_time))},endMs={timestamp_to_timestamp_ms(date_to_timestamp(end_time))}",
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Assets frigorifiques et les statistiques spécifiques aux assets frigorifiques.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "core",
            "table_name": "contacts",
            "endpoint": "contacts",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Liste des contacts de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "documents",
            "table_name": "fleet_document_types",
            "endpoint": "fleet/document-types",
            "params": f"startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Liste des types de documents de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "documents",
            "table_name": "fleet_documents",
            "endpoint": "fleet/documents",
            "params": f"startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Liste de tous des documents de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "driver_vehicle_assignments",
            "table_name": "driver_vehicle_assignments",
            "endpoint": "fleet/driver-vehicle-assignments",
            "params": f"filterBy=drivers,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Affectations conducteur-véhicule pour les conducteurs ou véhicules demandés dans la plage horaire demandée. Pour récupérer les affectations conducteur-véhicule en dehors des plages horaires des trajets du véhicule, assignationType doit être spécifié. Remarque : ce point de terminaison remplace les points de terminaison précédents pour récupérer les affectations par conducteur ou par véhicule.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "drivers",
            "table_name": "fleet_drivers",
            "endpoint": "fleet/drivers",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Liste des conducteurs de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "equipment",
            "table_name": "fleet_equipment",
            "endpoint": "fleet/equipment",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Liste de tous les équipements d'une organisation. Les objets d'équipement représentent des actifs alimentés connectés à un Samsara AG26 via un câble APWR, CAT ou J1939. Ils sont automatiquement créés avec un identifiant d'équipement Samsara unique chaque fois qu'un AG26 est activé dans votre organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        # {
        #     "family": "fuel_energy",
        #     "table_name": "fleet_drivers_fuel_energy",
        #     "endpoint": "fleet/reports/drivers/fuel-energy",
        #     "params": f"startDate={cast_date(start_time)},endDate={cast_date(end_time)}",
        #     "is_processed": 0,
        #     "rate_limit_per_seconde": 5,
        #     "description": "Données de consommation de carburant et d'énergie pour tous les conducteurs dans la plage horaire spécifiée. Les données sont disponibles dans la plage horaire locale du conducteur.",
        #     "is_exception": 1,
        #     "exception_config": {"exception_type": "date", "constraint": "is_data_but_datetime"},
        #     'delta_days': 1
        #     "download_type": "time"
        # },
        {
            "family": "fuel_energy",
            "table_name": "fleet_vehicles_fuel_energy",
            "endpoint": "fleet/reports/vehicles/fuel-energy",
            "params": f"startDate={cast_date(start_time)},endDate={cast_date(end_time)}",
            "is_processed": 0,
            "rate_limit_per_seconde": 25,
            "description": "Données de consommation de carburant et d'énergie pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 1,
            "exception_config": {
                "exception_type": "date",
                "constraint": "is_data_but_datetime",
            },
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "date",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "hours_of_service",
            "table_name": "fleet_hos_clocks",
            "endpoint": "fleet/hos/clocks",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Horaires de service des conducteurs pour une organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "hours_of_service",
            "table_name": "fleet_hos_daily_logs",
            "endpoint": "fleet/hos/daily-logs",
            "params": f"startDate={cast_date(start_time)},endDate={cast_date(end_time)}",
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Journaux quotidiens de service des conducteurs pour une organisation.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "hours_of_service",
            "table_name": "fleet_hos_violations",
            "endpoint": "fleet/hos/violations",
            "params": f"startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Données des violations de service des conducteurs pour une organisation.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "fuel_energy",
            "table_name": "fleet_vehicle_idling",
            "endpoint": "fleet/reports/vehicle/idling",
            "params": f"limit=512,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 25,
            "description": "Données de ralenti pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            'delta_days': 1,
            "download_type": "time",
            "time_partitioning_field": "startTime",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "safety",
            "table_name": "fleet_safety_events",
            "endpoint": "fleet/safety-events",
            "params": f"startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": np.nan,
            "description": "Evénements de sécurité pour une organisation.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "safety",
            "table_name": "fleet_safety_events_audit_logs",
            "endpoint": "fleet/safety-events/audit-logs/feed",
            "params": f"startTime={date_to_iso_or_timestamp(datetime.strptime(start_time, '%d/%m/%Y').replace(tzinfo=pytz.UTC), 'datetime')}",
            "is_processed": 0,
            "rate_limit_per_seconde": 3,
            "description": "Journaux d'audit des événements de sécurité pour une organisation.",
            "is_exception": 1,
            "exception_config": {
                "exception_type": "date",
                "constraint": "only_start_date",
            },
            "delta_days": 1,
            "download_type": "time",
        },
        # {
        #     "family": "safety",
        #     "table_name": "fleet_safety_driver_efficiency",
        #     "endpoint": "beta/fleet/drivers/efficiency",
        #     "params": f"startTime={start_time},endTime={end_time}",
        #     "is_processed": 0,
        #     "rate_limit_per_seconde": 50,
        #     "description": "Données d'efficacité des conducteurs.",
        #     "is_exception": 0,
        #     "exception_config": {},
        #     'delta_days': 1,
        #     "download_type": "time"
        # },
        {
            "family": "safety",
            "table_name": "fleet_vehicle_safety_score",
            "endpoint": "v1/fleet/vehicles/{vehicleId}/safety/score",
            "params": f"startMs={timestamp_to_timestamp_ms(date_to_timestamp(start_time))},endMs={timestamp_to_timestamp_ms(date_to_timestamp(end_time))}",
            "is_processed": 0,
            "rate_limit_per_seconde": 3,
            "description": "Récupère le score de sécurité pour un véhicule ou un conducteur dans une plage horaire donnée.",
            "is_exception": 1,
            "exception_config": {
                "exception_type": "table",
                "constraint": "dynamic_url",
                "table_name": "fleet_vehicles",
                "table_column_name": f"{samsara_vehicle_id}",
                "exception_param_name": "vehicleId",
                "key_to_apply_on": "endpoint",
                "is_list": 0,
            },
            "delta_days": 1,
            "download_type": "time",
        },
        {
            "family": "trailers",
            "table_name": "fleet_trailers",
            "endpoint": "fleet/trailers",
            "params": np.nan,
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Récupère la liste des remorques de l'organisation.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_ambientAirTemperatureMilliC",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=ambientAirTemperatureMilliC,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de température ambiante en millidégré celsius pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_barometricPressurePa",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=barometricPressurePa,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de pression barométrique pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_batteryMilliVolts",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=batteryMilliVolts,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de tension de la batterie pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_defLevelMilliPercent",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=defLevelMilliPercent,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de niveau de The Diesel Exhaust Fluid (DEF) pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_ecuSpeedMph",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=ecuSpeedMph,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de vitesse du moteur pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_engineCoolantTemperatureMilliC",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=engineCoolantTemperatureMilliC,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de température du liquide de refroidissement du moteur pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_engineImmobilizer",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=engineImmobilizer,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_engineLoadPercent",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=engineLoadPercent,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de charge du moteur pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_engineOilPressureKPa",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=engineOilPressureKPa,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de pression d'huile du moteur pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_engineRpm",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=engineRpm,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de régime moteur pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_engineStates",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=engineStates,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données d'état du moteur (Off, On, Idle) pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_faultCodes",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=faultCodes,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Codes de défaut pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_fuelPercents",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=fuelPercents,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Pourcentages de carburant pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_gpsDistanceMeters",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=gpsDistanceMeters,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de distance parcourue pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_gpsOdometerMeters",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=gpsOdometerMeters,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de compteur kilométrique GPS pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_intakeManifoldTemperatureMilliC",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=intakeManifoldTemperatureMilliC,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Données de température du collecteur d'admission pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicle_stats",
            "table_name": "fleet_vehicle_stats_obdEngineSeconds",
            "endpoint": "fleet/vehicles/stats/history",
            "params": f"Types=obdEngineSeconds,startTime={start_time},endTime={end_time}",
            "is_processed": 0,
            "rate_limit_per_seconde": 50,
            "description": "Cumule en seconde du temps de fonctionnement du moteur pour tous les véhicules dans la plage horaire spécifiée.",
            "is_exception": 0,
            "exception_config": {},
            "delta_days": 1,
            "download_type": "time",
            "time_partitioning_field": "time",
            "clustering_fields": [f"{samsara_vehicle_id}"],
        },
        {
            "family": "vehicles",
            "table_name": "fleet_vehicles",
            "endpoint": "fleet/vehicles",
            "params": f'limit=512, createdAfterTime={datetime.strptime("01/05/2025", "%d/%m/%Y").replace(tzinfo=pytz.UTC).isoformat()}',
            "is_processed": 0,
            "rate_limit_per_seconde": 25,  # createdAfterTime={datetime.strptime(start_time, "%d/%m/%Y").replace(tzinfo=pytz.UTC).isoformat()}
            "description": "Liste des véhicules.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "core",
            "table_name": "fleet_tags",
            "endpoint": "tags",
            "params": f"limit=512",
            "is_processed": 0,
            "rate_limit_per_seconde": 25,
            "description": "Liste des tags.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        {
            "family": "core",
            "table_name": "fleet_devices",
            "endpoint": "devices",
            "params": f"limit=100,includeHealth=true",
            "is_processed": 0,
            "rate_limit_per_seconde": 5,
            "description": "Liste des boitiers.",
            "is_exception": 0,
            "exception_config": {},
            "download_type": "oneshot",
        },
        # {
        #     "family": "safety",
        #     "table_name": "fleet_drivers_efficiency",
        #     "endpoint": "beta/fleet/drivers/efficiency",
        #     "params": "driverIds={driverIds},"+f"startTime={start_time},endTime={end_time}",
        #     "is_processed": 0,
        #     "rate_limit_per_seconde": 50,
        #     "description": "Données d'efficacité des conducteurs.",
        #     "is_exception": 1,
        #     "exception_config": {"table_name": "fleet_drivers", "table_column_name": f"{samsara_vehicle_id}",
        #                          "exception_param_name": "driverIds", "key_to_apply_on": "params", "is_list": 1}
        # }
    ]
    df = pd.DataFrame(data)
    if metadata_filename:
        df.to_excel(metadata_filename, index=False, header=True)
    return df
