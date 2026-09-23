import unittest
from unittest.mock import Mock, patch

import pandas as pd

import main


class HistoricalPipelineTests(unittest.TestCase):
    def setUp(self):
        self.metadata = pd.DataFrame(
            [{"table_name": "fleet_assets_reefers", "download_type": "time"}]
        )

    @patch("main.scrape_samsara_to_gcs")
    def test_historical_download_does_not_move_incremental_checkpoints(self, scrape):
        gcs_client = Mock()

        main.run_download_stage(
            gcs_client,
            self.metadata,
            configs_for_update={"fleet_assets_reefers": {}},
            start_date="18/09/2026",
            end_date="20/09/2026",
            max_workers=2,
            historical=True,
        )

        self.assertEqual(scrape.call_count, 2)
        gcs_client.update_configs_for_update.assert_not_called()
        gcs_client.bucket_manager.missing_dates.assert_not_called()

    def test_historical_transform_is_bounded_and_ignores_checkpoint(self):
        gcs_client = Mock()
        gcs_client.transform_and_save_data.return_value = {
            "selected_files": 1,
            "uploaded_files": 1,
            "skipped_files": 0,
        }

        main.run_transform_stage(
            gcs_client,
            self.metadata,
            configs_for_update={"fleet_assets_reefers": {"checkpoint": "later"}},
            start_date="18/09/2026",
            end_date="20/09/2026",
            historical=True,
        )

        call = gcs_client.transform_and_save_data.call_args.kwargs
        self.assertEqual(call["configs_for_update"], {})
        self.assertEqual(call["start_date"].strftime("%d/%m/%Y"), "18/09/2026")
        self.assertEqual(call["end_date"].strftime("%d/%m/%Y"), "20/09/2026")
        self.assertTrue(call["skip_existing"])
        gcs_client.update_configs_for_update.assert_not_called()

    def test_historical_transform_rejects_silent_empty_output(self):
        gcs_client = Mock()
        gcs_client.transform_and_save_data.return_value = {
            "selected_files": 2,
            "uploaded_files": 0,
            "skipped_files": 0,
        }

        with self.assertRaisesRegex(RuntimeError, "aucun fichier aplati"):
            main.run_transform_stage(
                gcs_client,
                self.metadata,
                configs_for_update={},
                start_date="18/09/2026",
                end_date="19/09/2026",
                historical=True,
            )

    @patch("main.load_to_bigquery")
    def test_historical_load_is_bounded_and_ignores_checkpoint(self, load):
        gcs_client = Mock()

        main.run_load_stage(
            gcs_client,
            self.metadata,
            configs_for_update={"fleet_assets_reefers": {"checkpoint": "later"}},
            start_date="18/09/2026",
            end_date="20/09/2026",
            historical=True,
        )

        call = load.call_args.kwargs
        self.assertEqual(call["configs_for_update"], {})
        self.assertEqual(call["start_date"].strftime("%d/%m/%Y"), "18/09/2026")
        self.assertEqual(call["end_date"].strftime("%d/%m/%Y"), "20/09/2026")
        gcs_client.update_configs_for_update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
