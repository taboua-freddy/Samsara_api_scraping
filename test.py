import json
import pandas as pd

from modules.processing import TransformData
from modules.utils_transformation import json_normalize, set_column, to_datetime, split_dataframe

pd.set_option('display.max_columns', None)

if __name__ == "__main__":
    file_name = "resources/data/fleet_assets_reefers_2023_12_31_to_2024_01_01.json"
    with open(file_name, 'r') as file:
        data = json.load(file)
    data = pd.DataFrame(data)
    # data = data.iloc[:int(len(data) * 0.1)]  # Limit to 10% of the data for testing
    transfomer = TransformData().set_data(data, {"table_name": "fleet_assets_reefers"}).transform()
    print(transfomer.head())