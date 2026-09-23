import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

from modules.transformation import TransformData
from modules.utils_transformation import split_dataframe


class VariableChunkSchemaTests(unittest.TestCase):
    def test_split_does_not_mutate_config_or_create_rows_for_missing_metric(self):
        shared = ["id", "name", "optional_shared"]
        config = {
            "engine": {"columns": ["ReeferStats_engineHours"]},
            "fuel": {"columns": ["ReeferStats_fuelPercentage"]},
        }
        frame = pd.DataFrame({
            "id": ["1"], "name": ["A"],
            "ReeferStats_fuelPercentage": [[{"changedAtMs": 1000, "value": 50}]],
        })

        result = split_dataframe(frame, shared, config)

        self.assertTrue(result["engine"].empty)
        self.assertEqual(len(result["fuel"]), 1)
        self.assertEqual(shared, ["id", "name", "optional_shared"])
        self.assertEqual(config["engine"]["columns"], ["ReeferStats_engineHours"])

    def test_reefer_metric_present_only_in_second_chunk_is_preserved(self):
        transformer = TransformData.__new__(TransformData)
        transformer.df = None
        transformer.endpoint_info = None
        transformer.except_table_names = None
        transformer.logger = Mock()
        first_chunk = pd.DataFrame({
            "assetType": ["Vehicle"], "id": ["1"], "name": ["A"],
            "reeferStats_fuelPercentage": [[
                {"changedAtMs": 1789689600000, "value": 50}
            ]],
        })
        second_chunk = pd.DataFrame({
            "assetType": ["Vehicle"], "id": ["1"], "name": ["A"],
            "reeferStats_engineHours": [[
                {"changedAtMs": 1789689600000, "value": 123, "extraField": "kept"}
            ]],
        })

        first = transformer.split_data(first_chunk, "fleet_assets_reefers")
        second = transformer.split_data(second_chunk, "fleet_assets_reefers")

        self.assertTrue(first["ReeferStats_engineHours"].empty)
        self.assertEqual(len(first["ReeferStats_fuelPercentage"]), 1)
        engine_hours = second["ReeferStats_engineHours"]
        self.assertEqual(len(engine_hours), 1)
        self.assertEqual(engine_hours.iloc[0]["extraField"], "kept")
        self.assertEqual(engine_hours.iloc[0]["asset_id"], "1")

    def test_null_metric_values_do_not_create_empty_child_rows(self):
        frame = pd.DataFrame({
            "id": ["1", "2", "3"], "name": ["A", "B", "C"],
            "ReeferStats_engineHours": [None, [], [{}]],
        })

        result = split_dataframe(
            frame,
            ["id", "name"],
            {"engine": {"columns": ["ReeferStats_engineHours"]}},
        )

        self.assertTrue(result["engine"].empty)

    def test_parquet_chunks_with_different_columns_keep_later_metric(self):
        transformer = TransformData.__new__(TransformData)
        transformer.df = None
        transformer.endpoint_info = None
        transformer.except_table_names = None
        transformer.logger = Mock()
        with tempfile.TemporaryDirectory() as directory:
            first_path = Path(directory) / "first.parquet"
            second_path = Path(directory) / "second.parquet"
            pd.DataFrame({
                "assetType": ["Vehicle"], "id": ["1"], "name": ["A"],
                "reeferStats_fuelPercentage": [[
                    {"changedAtMs": 1789689600000, "value": 50}
                ]],
            }).to_parquet(first_path)
            pd.DataFrame({
                "assetType": ["Vehicle"], "id": ["1"], "name": ["A"],
                "reeferStats_engineHours": [[
                    {"changedAtMs": 1789689600000, "value": 123}
                ]],
            }).to_parquet(second_path)

            first = transformer.split_data(
                pd.read_parquet(first_path), "fleet_assets_reefers"
            )
            second = transformer.split_data(
                pd.read_parquet(second_path), "fleet_assets_reefers"
            )

        self.assertTrue(first["ReeferStats_engineHours"].empty)
        self.assertEqual(len(second["ReeferStats_engineHours"]), 1)


if __name__ == "__main__":
    unittest.main()
