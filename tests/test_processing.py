import unittest
from unittest.mock import Mock, patch

from modules.processing import DataFetcher, _parse_exception_config


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
        final_state = gcs_client.update_extraction_state.call_args_list[-1].args[1]
        self.assertEqual(final_state["status"], "complete")
        self.assertEqual(final_state["rows_written"], 5)
        self.assertEqual(final_state["next_chunk_index"], 3)

    def test_resumes_from_last_confirmed_cursor(self):
        state = {
            "status": "in_progress",
            "next_cursor": "confirmed-cursor",
            "next_chunk_index": 2,
            "rows_written": 3,
            "uploaded_files": ["vehicle_stats/example/example_2026_09_18_00001.parquet"],
        }
        fetcher, client, gcs_client = self.make_fetcher()
        gcs_client.get_extraction_manifest.return_value = {"state-key": state}
        client.iter_data_pages.return_value = iter(
            [([{"id": 4}], {"hasNextPage": False})]
        )
        with patch("modules.processing.hashlib.sha256") as sha:
            sha.return_value.hexdigest.return_value = "state-key"
            fetcher._stream_and_upload(
                "endpoint", {}, "example_2026_09_18", "date", 5
            )

        request_params = client.iter_data_pages.call_args.kwargs["params"]
        self.assertEqual(request_params["after"], "confirmed-cursor")
        self.assertEqual(
            fetcher._flatten_and_upload.call_args.args[1],
            "example_2026_09_18_00002",
        )
