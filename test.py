import json
import os
import re

import pandas as pd

from modules.gcp import BucketManager
from modules.interface import SearchRetrieveType
from modules.processing import TransformData
from modules.utils import CREDENTIALS_DIR
from modules.utils_transformation import json_normalize, set_column, to_datetime, split_dataframe, \
    fast_json_normalize_parallel

pd.set_option('display.max_columns', None)

samsara_api_token = os.getenv('SAMSARA_API_TOKEN')
# gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
# database_id = os.getenv('DATABASE_ID')
gcs_bucket_name = os.getenv('GCS_BUCKET_FLATTENED_NAME')
database_id = os.getenv('DWH_ID')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/maintenance-predictive-445011-fb98a59d6aa3.json"

if __name__ == "__main__":
    file_name = "resources/data/fleet_vehicle_stats_faultCodes_2024_02_01.json"
    with open(file_name, 'r') as file:
        data = json.load(file)
    data = pd.DataFrame(data)
    # print(data.columns)
    # transfomer = fast_json_normalize_parallel(data, "assets")
    # data = data.iloc[:int(len(data) * 0.1)]  # Limit to 10% of the data for testing
    transfomer = TransformData().set_data(data, {"table_name": "fleet_vehicle_stats_faultCodes","date_str":"2024_05_22"}).transform()
    # print(transfomer.dtypes)
    table_name = "fleet_vehicle_stats_faultCodes"
    transformer = TransformData()
    data = transformer.set_data(data, {"table_name": table_name, "date_str": "2024_02_01"}).transform()
    results: dict[str, pd.DataFrame] = {}
    if table_name in transformer.tables_to_split:
        results: dict[str, pd.DataFrame] = transformer.set_data(data, {"table_name": f"{table_name}_split"}).transform()
        print(results.keys())
        for table_name in results:
            # print(f"Processing table: {table_name} with {len(results[table_name])} rows")
            if not results[table_name].empty:
                results[table_name] = transformer.set_data(results[table_name],
                                                                {"table_name": table_name}).transform()
            # print(f'Transformed table: {table_name} with {len(results[table_name])} rows')

    # print(results)
    # df = pd.read_parquet("resources/tmp/fleet_vehicles_fuel_energy_2024_02_01.parquet")
    # print(df.dtypes)
    # "vehicle_stats/fleet_vehicle_stats_faultCodes/fleet_vehicle_stats_faultCodes_j1939_2024_02_01.parquet"

    # "vehicle_stats/fleet_vehicle_stats_ecuSpeedMph/fleet_vehicle_stats_ecuSpeedMph_2024_02_01.parquet"
    # regex = re.compile(
    #     r"^(?P<family>[^/]+)/"
    #     r"(?P<main_family>[^/]+)/"
    #     r"(?P<table_name>.+?)_"
    #     r"(?P<start_date>\d{4}_\d{2}_\d{2})"
    #     r"(?:_to_(?P<end_date>\d{4}_\d{2}_\d{2}))?"
    #     r"(?:_(?P<index>\d+))?"
    #     r"\.parquet$"
    # )
    # paths = [
    #     "vehicle_stats/fleet_vehicle_stats_faultCodes/fleet_vehicle_stats_faultCodes_obdii_2024_02_01.parquet",
    #     "vehicle_stats/fleet_vehicle_stats_faultCodes/fleet_vehicle_stats_faultCodes_j1939_2024_02_01_to_2024_03_01.parquet",
    #     "vehicle_stats/fleet_vehicle_stats_faultCodes/fleet_vehicle_stats_faultCodes_j1939_2024_02_01_to_2024_03_01_12.parquet",
    #     "assets/fleet_assets_reefers/ReeferStats_ambientAirTemperature_2024_01_31.parquet",
    #     "assets/fleet_assets/fleet_assets_2025_05_30.parquet",
    # ]
    #
    # for path in paths:
    #     match = regex.match(path)
    #     if match:
    #         print(match.groupdict())
    #     else:
    #         print(f"❌ No match for: {path}")

    # print(bkm.parse_file_path("assets/fleet_assets_reefers/ReeferStats_ambientAirTemperature_2024_01_31.parquet", SearchRetrieveType.TABLE_NAME))