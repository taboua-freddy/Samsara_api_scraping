import unittest
from unittest.mock import patch

import pandas as pd

from modules import metadata as metadata_module
from modules.interface import ColumnToUpdate, DownloadType


class BuildMetadataTests(unittest.TestCase):
    def test_keeps_a_distinct_start_date_per_table(self):
        def fake_metadata(start_time, end_time, metadata_filename=None):
            return pd.DataFrame(
                [
                    {"table_name": "first", "download_type": DownloadType.TIME.value},
                    {"table_name": "second", "download_type": DownloadType.TIME.value},
                ]
            )

        with patch.object(metadata_module, "make_meta_data", fake_metadata):
            result = metadata_module.build_metadata(
                configs_for_update={
                    "first": {
                        "download_type": DownloadType.TIME.value,
                        ColumnToUpdate.DOWNLOAD.value: "10/01/2025",
                    }
                },
                table_names=["first", "second"],
                start_date="01/01/2025",
                end_date="20/01/2025",
            )

        starts = result.set_index("table_name")[ColumnToUpdate.DOWNLOAD.value].to_dict()
        self.assertEqual(
            starts, {"first": "10/01/2025", "second": "01/01/2025"}
        )


class MetadataValidationTests(unittest.TestCase):
    def test_rejects_duplicate_table_names(self):
        frame = pd.DataFrame(
            [
                {
                    "family": "fleet",
                    "table_name": "duplicate",
                    "endpoint": "endpoint",
                    "params": "limit=1",
                    "rate_limit_per_seconde": 1,
                    "download_type": DownloadType.TIME.value,
                },
                {
                    "family": "fleet",
                    "table_name": "duplicate",
                    "endpoint": "other",
                    "params": "limit=1",
                    "rate_limit_per_seconde": 1,
                    "download_type": DownloadType.TIME.value,
                },
            ]
        )
        with self.assertRaisesRegex(ValueError, "dupliqués"):
            metadata_module.validate_metadata(frame)

    def test_generated_metadata_is_valid(self):
        frame = metadata_module.make_meta_data("01/01/2025", "02/01/2025")
        validated = metadata_module.validate_metadata(frame)
        self.assertFalse(validated["rate_limit_per_seconde"].isna().any())

    def test_catalog_resolves_date_variables(self):
        frame = metadata_module.make_meta_data("18/09/2026", "19/09/2026")
        by_name = frame.set_index("table_name")
        self.assertEqual(
            by_name.loc["fleet_vehicle_stats_engineRpm", "params"],
            "Types=engineRpm,startTime=18/09/2026,endTime=19/09/2026",
        )
        rendered = frame["params"].dropna().astype(str)
        self.assertFalse(rendered.str.contains(r"\$\{", regex=True).any())

    def test_dynamic_dependencies_use_raw_ids_with_transformed_aliases(self):
        frame = metadata_module.make_meta_data("18/09/2026", "19/09/2026")
        by_name = frame.set_index("table_name")

        safety_config = by_name.loc[
            "fleet_vehicle_safety_score", "exception_config"
        ]
        self.assertEqual(safety_config["table_column_name"], "id")
        self.assertIn("vehicle_id", safety_config["table_column_aliases"])

        alerts_config = by_name.loc["alerts_incidents", "exception_config"]
        self.assertEqual(alerts_config["table_column_name"], "id")
        self.assertIn("configurationId", alerts_config["table_column_aliases"])
