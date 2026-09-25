import os
import hashlib
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

from modules.processing import (
    DataFetcher,
    _parse_exception_config,
    _read_dependency_values,
)
from modules.extraction_manifest import partition_key, request_signature


class ExceptionConfigTests(unittest.TestCase):
    def test_accepts_json_and_legacy_literal(self):
        self.assertEqual(
            _parse_exception_config('{"exception_type": "date"}'),
            {"exception_type": "date"},
        )
        self.assertEqual(
            _parse_exception_config("{'exception_type': 'table'}"),
            {"exception_type": "table"},
        )

    def test_does_not_execute_code(self):
        with self.assertRaises((ValueError, SyntaxError)):
            _parse_exception_config("__import__('os').getcwd()")


class DynamicEndpointTests(unittest.TestCase):
    def test_dependency_values_accept_variable_parquet_schemas_and_aliases(self):
        logger = Mock()
        with tempfile.TemporaryDirectory() as temp_dir:
            without_key = os.path.join(temp_dir, "without_key.parquet")
            with_key = os.path.join(temp_dir, "with_key.parquet")
            with_alias = os.path.join(temp_dir, "with_alias.parquet")
            pd.DataFrame({"name": ["A"]}).to_parquet(without_key)
            pd.DataFrame({"id": ["1", "2"]}).to_parquet(with_key)
            pd.DataFrame({"configurationId": ["2", "3"]}).to_parquet(with_alias)

            values = _read_dependency_values(
                [without_key, with_key, with_alias],
                "id",
                ["configurationId"],
                logger,
            )

        self.assertEqual(values, ["1", "2", "3"])
        logger.warning.assert_called_once()

    def test_dependency_values_report_all_schemas_when_key_is_absent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_name = os.path.join(temp_dir, "without_key.parquet")
            pd.DataFrame({"name": ["A"]}).to_parquet(file_name)

            with self.assertRaisesRegex(RuntimeError, "Schémas détectés"):
                _read_dependency_values([file_name], "id", ["configurationId"])

    def test_does_not_call_base_endpoint_after_dynamic_endpoints(self):
        client = Mock(delta_days=1)
        client.get_all_data.return_value = [{"id": 1}]
        fetcher = DataFetcher.__new__(DataFetcher)
        fetcher.samsara_client = client
        fetcher.endpoint_info = {
            "table_name": "example",
            "endpoint": "base-endpoint",
            "rate_limit_per_seconde": 5,
        }
        fetcher.logger = Mock()
        fetcher._flatten_and_upload = Mock()

        fetcher._fetch_data_for_interval(
            params={
                "startTime": "unused",
                "endTime": "unused",
                "endpoints": ["endpoint/1", "endpoint/2"],
            },
            startTime="2025-01-01T00:00:00+00:00",
            endTime="2025-01-01T23:59:59+00:00",
        )

        called_endpoints = [
            call.kwargs["endpoint"] for call in client.get_all_data.call_args_list
        ]
        self.assertEqual(called_endpoints, ["endpoint/1", "endpoint/2"])
        self.assertNotIn("base-endpoint", called_endpoints)


class StreamingExtractionTests(unittest.TestCase):
    def make_fetcher(self, manifest=None):
        client = Mock()
        bucket_manager = Mock()
        bucket_manager.file_exists.return_value = (True, Mock())
        bucket_manager.bucket.list_blobs.return_value = []
        gcs_client = Mock(bucket_manager=bucket_manager)
        gcs_client.get_extraction_manifest.return_value = manifest or {}
        fetcher = DataFetcher.__new__(DataFetcher)
        fetcher.samsara_client = client
        fetcher.gcs_client = gcs_client
        fetcher.endpoint_info = {
            "folder_path": "vehicle_stats/example",
            "table_name": "example",
        }
        fetcher.logger = Mock()
        fetcher.chunk_rows = 3
        fetcher.chunk_pages = 25
        fetcher._flatten_and_upload = Mock(
            side_effect=lambda data, file_name, date_str: (
                f"vehicle_stats/example/{file_name}.parquet"
            )
        )
        return fetcher, client, gcs_client

    def test_subday_intervals_have_distinct_file_names(self):
        fetcher, _, _ = self.make_fetcher()

        self.assertEqual(
            fetcher._date_str(
                "2026-09-21T00:00:00+00:00", "2026-09-21T05:59:59+00:00"
            ),
            "2026_09_21_000000_to_2026_09_21_055959",
        )
        self.assertEqual(
            fetcher._date_str(
                "2026-09-21T06:00:00+00:00", "2026-09-21T11:59:59+00:00"
            ),
            "2026_09_21_060000_to_2026_09_21_115959",
        )
        self.assertEqual(
            fetcher._date_str(
                "2026-09-21T00:00:00+00:00", "2026-09-21T23:59:59+00:00"
            ),
            "2026_09_21",
        )
        self.assertEqual(
            fetcher._date_str(
                "2026-09-21T00:00:00+00:00", "2026-09-21T23:59:59.999000+00:00"
            ),
            "2026_09_21",
        )
        self.assertEqual(
            fetcher._date_str("2026-09-21", "2026-09-21"),
            "2026_09_21",
        )
        self.assertEqual(
            fetcher._date_str(
                "2026-09-21T00:00:00+00:00", "2026-09-22T00:00:00+00:00"
            ),
            "2026_09_21",
        )

    def test_reefer_day_is_covered_by_four_contiguous_six_hour_windows(self):
        fetcher, client, _ = self.make_fetcher()
        client.shared_vars_manager = None
        fetcher.max_workers = 4
        fetcher.endpoint_info.update(
            {
                "family": "assets",
                "table_name": "fleet_assets_reefers",
                "endpoint": "v1/fleet/assets/reefers",
                "params": "startMs=1789948800000,endMs=1790035200000",
                "is_exception": False,
                "exception_config": {},
                "delta_days": 0.25,
                "rate_limit_per_seconde": 5,
            }
        )

        with patch("modules.processing.parallelize_execution") as run_parallel:
            fetcher.fetch_and_upload()

        windows = run_parallel.call_args.kwargs["tasks"]
        self.assertEqual(len(windows), 4)
        self.assertEqual(windows[0]["startMs"], 1789948800000)
        self.assertEqual(windows[-1]["endMs"], 1790035199999)
        for previous, following in zip(windows, windows[1:], strict=False):
            self.assertEqual(previous["endMs"] + 1, following["startMs"])

    def test_empty_timestamp_window_after_checkpoint_is_skipped(self):
        fetcher, client, gcs_client = self.make_fetcher()
        client.shared_vars_manager = None
        fetcher.endpoint_info.update({
            "family": "assets",
            "table_name": "fleet_assets_reefers",
            "endpoint": "v1/fleet/assets/reefers",
            "params": "startMs=1790114400000,endMs=1790114400000",
            "is_exception": False,
            "exception_config": {},
            "delta_days": 0.25,
            "rate_limit_per_seconde": 5,
        })

        with patch("modules.processing.parallelize_execution") as parallel:
            fetcher.fetch_and_upload()

        parallel.assert_not_called()
        client.iter_data_pages.assert_not_called()
        gcs_client.get_extraction_manifest.assert_not_called()

    def test_changed_window_resumes_original_partial_partition(self):
        fetcher, client, gcs_client = self.make_fetcher()
        client.shared_vars_manager = None
        fetcher.max_workers = 1
        endpoint = "v1/fleet/assets/reefers"
        start_ms = 1789948800000
        end_ms = start_ms + 12 * 3_600_000
        fetcher.endpoint_info.update({
            "family": "assets", "table_name": "fleet_assets_reefers",
            "endpoint": endpoint,
            "params": f"startMs={start_ms},endMs={end_ms}",
            "is_exception": False, "exception_config": {},
            "delta_days": 0.25, "rate_limit_per_seconde": 5,
        })
        signature = request_signature(
            "fleet_assets_reefers", endpoint, {"startMs": start_ms, "endMs": end_ms}
        )
        gcs_client.get_extraction_manifest.return_value = {
            "complete": {
                "signature": signature, "start_ms": start_ms,
                "end_exclusive_ms": start_ms + 6 * 3_600_000,
                "status": "complete", "uploaded_files": ["existing.parquet"],
            },
            "partial": {
                "signature": signature,
                "start_ms": start_ms + 6 * 3_600_000,
                "end_exclusive_ms": start_ms + 9 * 3_600_000,
                "status": "in_progress", "next_cursor": "confirmed",
                "uploaded_files": ["partial.parquet"],
            },
        }

        with (
            patch.dict(os.environ, {"SAMSARA_WINDOW_MINUTES": "60"}),
            patch("modules.processing.parallelize_execution") as run_parallel,
        ):
            fetcher.fetch_and_upload()

        windows = run_parallel.call_args.kwargs["tasks"]
        self.assertEqual(windows[0], {
            "startMs": start_ms + 6 * 3_600_000,
            "endMs": start_ms + 9 * 3_600_000 - 1,
        })
        self.assertEqual(len(windows), 4)
        self.assertEqual(windows[-1]["endMs"], end_ms - 1)

    def test_504_splits_uncommitted_reefer_window_without_overlap(self):
        fetcher, _, _ = self.make_fetcher()
        fetcher.endpoint_info.update(
            {
                "table_name": "fleet_assets_reefers",
                "endpoint": "v1/fleet/assets/reefers",
                "folder_path": "assets/fleet_assets_reefers",
                "split_on_server_error": True,
                "rate_limit_per_seconde": 5,
            }
        )
        http_error = requests.HTTPError("504", response=Mock(status_code=504))
        failure = RuntimeError("Échec après 5 tentatives")
        failure.__cause__ = http_error
        fetcher._stream_and_upload = Mock(side_effect=[failure, None, None])
        start_ms = 1789948800000
        end_ms = 1789970399999

        fetcher._fetch_data_for_interval(
            params={"startMs": start_ms, "endMs": end_ms},
            startMs=start_ms,
            endMs=end_ms,
        )

        calls = fetcher._stream_and_upload.call_args_list
        self.assertEqual(len(calls), 3)
        first_child = calls[1].kwargs["params"]
        second_child = calls[2].kwargs["params"]
        self.assertEqual(first_child["startMs"], start_ms)
        self.assertEqual(first_child["endMs"] + 1, second_child["startMs"])
        self.assertEqual(second_child["endMs"], end_ms)
        self.assertNotEqual(calls[1].kwargs["file_name"], calls[2].kwargs["file_name"])

    def test_500_after_504_splits_again_and_covers_original_window(self):
        fetcher, _, _ = self.make_fetcher()
        fetcher.endpoint_info.update(
            {
                "table_name": "fleet_assets_reefers",
                "endpoint": "v1/fleet/assets/reefers",
                "folder_path": "assets/fleet_assets_reefers",
                "split_on_server_error": True,
                "rate_limit_per_seconde": 5,
            }
        )

        def failure(status):
            error = RuntimeError("Échec après 5 tentatives")
            error.__cause__ = requests.HTTPError(
                str(status), response=Mock(status_code=status)
            )
            return error

        fetcher._stream_and_upload = Mock(
            side_effect=[failure(504), failure(500), None, None, None]
        )
        start_ms = 1789704000000
        end_ms = 1789725599999

        fetcher._fetch_data_for_interval(
            params={"startMs": start_ms, "endMs": end_ms},
            startMs=start_ms,
            endMs=end_ms,
        )

        calls = fetcher._stream_and_upload.call_args_list
        self.assertEqual(len(calls), 5)
        grandchildren = [calls[2].kwargs["params"], calls[3].kwargs["params"]]
        first_child = calls[1].kwargs["params"]
        second_child = calls[4].kwargs["params"]
        self.assertEqual(grandchildren[0]["startMs"], start_ms)
        self.assertEqual(
            grandchildren[0]["endMs"] + 1, grandchildren[1]["startMs"]
        )
        self.assertEqual(grandchildren[1]["endMs"], first_child["endMs"])
        self.assertEqual(first_child["endMs"] + 1, second_child["startMs"])
        self.assertEqual(second_child["endMs"], end_ms)

    def test_persistent_500_stops_at_the_bounded_split_depth(self):
        fetcher, _, _ = self.make_fetcher()
        fetcher.endpoint_info.update(
            {
                "table_name": "fleet_assets_reefers",
                "endpoint": "v1/fleet/assets/reefers",
                "folder_path": "assets/fleet_assets_reefers",
                "split_on_server_error": True,
                "rate_limit_per_seconde": 5,
            }
        )
        error = RuntimeError("Échec après 5 tentatives")
        error.__cause__ = requests.HTTPError(
            "500", response=Mock(status_code=500)
        )
        fetcher._stream_and_upload = Mock(side_effect=error)

        with self.assertRaisesRegex(RuntimeError, "Échec après 5 tentatives"):
            fetcher._fetch_data_for_interval(
                params={"startMs": 1789704000000, "endMs": 1789706699999},
                startMs=1789704000000,
                endMs=1789706699999,
                _split_depth=3,
            )

        fetcher._stream_and_upload.assert_called_once()

    def test_504_keeps_committed_chunks_on_original_cursor(self):
        fetcher, _, gcs_client = self.make_fetcher()
        fetcher.endpoint_info.update(
            {
                "table_name": "fleet_assets_reefers",
                "endpoint": "v1/fleet/assets/reefers",
                "folder_path": "assets/fleet_assets_reefers",
                "split_on_server_error": True,
                "rate_limit_per_seconde": 5,
            }
        )
        signature = request_signature(
            "fleet_assets_reefers", "v1/fleet/assets/reefers",
            {"startMs": 1789948800000, "endMs": 1789970399000},
        )
        key = partition_key(signature, 1789948800000, 1789970399001)
        gcs_client.get_extraction_manifest.return_value = {
            key: {"uploaded_files": ["already-committed.parquet"]}
        }
        http_error = requests.HTTPError("504", response=Mock(status_code=504))
        failure = RuntimeError("Échec après 5 tentatives")
        failure.__cause__ = http_error
        fetcher._stream_and_upload = Mock(side_effect=failure)

        with self.assertRaisesRegex(RuntimeError, "Échec après 5 tentatives"):
            fetcher._fetch_data_for_interval(
                params={"startMs": 1789948800000, "endMs": 1789970399000},
                startMs=1789948800000,
                endMs=1789970399000,
            )

        fetcher._stream_and_upload.assert_called_once()

    def test_writes_bounded_chunks_and_marks_interval_complete(self):
        fetcher, client, gcs_client = self.make_fetcher()
        client.iter_data_pages.return_value = iter(
            [
                ([{"id": 1}, {"id": 2}], {"hasNextPage": True, "endCursor": "a"}),
                ([{"id": 3}, {"id": 4}], {"hasNextPage": True, "endCursor": "b"}),
                ([{"id": 5}], {"hasNextPage": False}),
            ]
        )

        fetcher._stream_and_upload("endpoint", {}, "example_2026_09_18", "date", 5)

        self.assertEqual(fetcher._flatten_and_upload.call_count, 2)
        first_call = fetcher._flatten_and_upload.call_args_list[0]
        second_call = fetcher._flatten_and_upload.call_args_list[1]
        self.assertEqual(len(first_call.args[0]), 4)
        self.assertEqual(first_call.args[1], "example_2026_09_18_00001")
        self.assertEqual(len(second_call.args[0]), 1)
        final_state = gcs_client.update_extraction_state.call_args_list[-1].args[2]
        self.assertEqual(final_state["status"], "complete")
        self.assertEqual(final_state["rows_written"], 5)
        self.assertEqual(final_state["next_chunk_index"], 3)
        self.assertEqual(
            final_state["chunk_policy"], {"chunk_rows": 3, "chunk_pages": 25}
        )
        self.assertEqual(final_state["chunk_policy_history"][0]["from_chunk_index"], 1)

    def test_resumes_from_last_confirmed_cursor(self):
        state = {
            "status": "in_progress",
            "next_cursor": "confirmed-cursor",
            "next_chunk_index": 2,
            "rows_written": 3,
            "uploaded_files": ["vehicle_stats/example/example_2026_09_18_00001.parquet"],
            "chunk_policy_history": [{
                "chunk_rows": 2,
                "chunk_pages": 10,
                "from_chunk_index": 1,
                "recorded_at": "2026-09-18T00:00:00",
            }],
        }
        fetcher, client, gcs_client = self.make_fetcher()
        signature = request_signature("example", "endpoint", {})
        key = hashlib.sha256(
            f"{signature}:vehicle_stats/example/example_2026_09_18|endpoint".encode()
        ).hexdigest()
        gcs_client.get_extraction_manifest.return_value = {key: state}
        client.iter_data_pages.return_value = iter(
            [([{"id": 4}], {"hasNextPage": False})]
        )
        fetcher._stream_and_upload(
            "endpoint", {}, "example_2026_09_18", "date", 5
        )

        request_params = client.iter_data_pages.call_args.kwargs["params"]
        self.assertEqual(request_params["after"], "confirmed-cursor")
        self.assertEqual(
            fetcher._flatten_and_upload.call_args.args[1],
            "example_2026_09_18_00002",
        )
        final_state = gcs_client.update_extraction_state.call_args.args[2]
        self.assertEqual(final_state["next_chunk_index"], 3)
        self.assertEqual(len(final_state["chunk_policy_history"]), 2)
        self.assertEqual(final_state["chunk_policy_history"][1]["from_chunk_index"], 2)
        self.assertEqual(
            final_state["chunk_policy_history"][1]["chunk_rows"], 3
        )

    def test_non_temporal_endpoint_uses_imported_legacy_state(self):
        fetcher, client, gcs_client = self.make_fetcher()
        identity = "vehicle_stats/example/example|endpoint"
        old_key = hashlib.sha256(identity.encode()).hexdigest()
        gcs_client.get_extraction_manifest.return_value = {
            old_key: {
                "identity": identity,
                "status": "complete",
                "uploaded_files": ["vehicle_stats/example/example_00001.parquet"],
            }
        }

        fetcher._stream_and_upload("endpoint", {}, "example", "", 5)

        client.iter_data_pages.assert_not_called()

    def test_completed_oneshot_is_refreshed_without_reusing_old_chunks(self):
        fetcher, client, gcs_client = self.make_fetcher()
        fetcher.endpoint_info["download_type"] = "oneshot"
        identity = "vehicle_stats/example/example|endpoint"
        old_key = hashlib.sha256(identity.encode()).hexdigest()
        old_file = "vehicle_stats/example/example_00001.parquet"
        gcs_client.get_extraction_manifest.return_value = {
            old_key: {
                "identity": identity,
                "status": "complete",
                "uploaded_files": [old_file],
            }
        }
        client.iter_data_pages.return_value = iter(
            [([{"id": 1}, {"id": 2}, {"id": 3}], {"hasNextPage": True, "endCursor": "next"}),
             ([{"id": 4}], {"hasNextPage": False})]
        )

        fetcher._stream_and_upload("endpoint", {}, "example", "", 5)

        self.assertNotIn("after", client.iter_data_pages.call_args.kwargs["params"])
        states = [call.args[2] for call in gcs_client.update_extraction_state.call_args_list]
        self.assertEqual(states[0]["status"], "in_progress")
        self.assertEqual(states[0]["previous_complete"]["uploaded_files"], [old_file])
        self.assertEqual(states[-1]["status"], "complete")
        self.assertNotIn(old_file, states[-1]["uploaded_files"])
        self.assertEqual(len(states[-1]["uploaded_files"]), 2)
        self.assertEqual(len({path.split("_")[-1] for path in states[-1]["uploaded_files"]}), 2)
        self.assertIn("snapshot_id", states[-1])

    def test_partial_oneshot_resumes_same_snapshot(self):
        fetcher, client, gcs_client = self.make_fetcher()
        fetcher.endpoint_info["download_type"] = "oneshot"
        identity = "vehicle_stats/example/example|endpoint"
        key = hashlib.sha256(identity.encode()).hexdigest()
        gcs_client.get_extraction_manifest.return_value = {
            key: {
                "identity": identity,
                "status": "in_progress",
                "snapshot_id": "2026_09_23_154512_12345678",
                "next_cursor": "cursor",
                "next_chunk_index": 2,
                "uploaded_files": [
                    "vehicle_stats/example/example_2026_09_23_154512_1234567800001.parquet"
                ],
                "rows_written": 3,
            }
        }
        client.iter_data_pages.return_value = iter(
            [([{"id": 4}], {"hasNextPage": False})]
        )

        fetcher._stream_and_upload("endpoint", {}, "example", "", 5)

        self.assertEqual(client.iter_data_pages.call_args.kwargs["params"]["after"], "cursor")
        self.assertEqual(
            fetcher._flatten_and_upload.call_args.args[1],
            "example_2026_09_23_154512_1234567800002",
        )
        self.assertEqual(gcs_client.update_extraction_state.call_args.args[2]["rows_written"], 4)
