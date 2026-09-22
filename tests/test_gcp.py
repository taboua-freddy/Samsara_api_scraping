import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pandas as pd
from google.api_core.exceptions import Conflict
from google.resumable_media.common import InvalidResponse

from modules.gcp import (
    BigQueryManager,
    BucketManager,
    ExecutionLock,
    GCSClient,
    build_load_fingerprint,
)
from modules.interface import SearchRetrieveType


class BigQueryManagerTests(unittest.TestCase):
    def test_waits_for_load_job_completion(self):
        load_job = Mock(errors=None)
        load_job.result.return_value = load_job
        client = Mock(project="project")
        client.load_table_from_uri.return_value = load_job

        manager = BigQueryManager.__new__(BigQueryManager)
        manager.bigquery_client = client
        manager.dataset_id = "dataset"
        manager.logger = Mock()
        manager.memory_manager = None

        uri, result = manager.load_parquet_to_bigquery(
            "gs://bucket/file.parquet", "table"
        )

        self.assertEqual(uri, "gs://bucket/file.parquet")
        self.assertIs(result, load_job)
        load_job.result.assert_called_once_with()

    def test_recovers_an_existing_deterministic_job(self):
        load_job = Mock(errors=None)
        load_job.result.return_value = load_job
        client = Mock(project="project")
        client.load_table_from_uri.side_effect = Conflict("job already exists")
        client.get_job.return_value = load_job

        manager = BigQueryManager.__new__(BigQueryManager)
        manager.bigquery_client = client
        manager.dataset_id = "dataset"
        manager.logger = Mock()
        manager.memory_manager = None

        _, result = manager.load_parquet_to_bigquery(
            "gs://bucket/file.parquet", "table", job_id="stable-job"
        )

        self.assertIs(result, load_job)
        client.get_job.assert_called_once_with("stable-job")
        load_job.result.assert_called_once_with()


class LoadManifestTests(unittest.TestCase):
    def test_time_file_fingerprint_ignores_generation(self):
        first = build_load_fingerprint("bucket", "path/file.parquet", "1", "time")
        second = build_load_fingerprint("bucket", "path/file.parquet", "2", "time")
        self.assertEqual(first, second)

    def test_oneshot_fingerprint_changes_with_generation(self):
        first = build_load_fingerprint("bucket", "path/file.parquet", "1", "oneshot")
        second = build_load_fingerprint("bucket", "path/file.parquet", "2", "oneshot")
        self.assertNotEqual(first, second)

    def test_config_merge_is_recursive_and_preserves_lists(self):
        merged = GCSClient._merge_config(
            {"table": {"checkpoint": "old", "missing": ["01/01/2025"]}},
            {"table": {"checkpoint": "new", "missing": ["02/01/2025"]}},
        )
        self.assertEqual(merged["table"]["checkpoint"], "new")
        self.assertEqual(
            merged["table"]["missing"], ["01/01/2025", "02/01/2025"]
        )

    def test_retries_resumable_412_generation_conflict(self):
        response = Mock(status_code=412)
        conflict = InvalidResponse(response, "precondition failed")
        blob = Mock(generation=7)
        blob.exists.return_value = True
        blob.download_as_bytes.return_value = b'{"existing": true}'
        blob.upload_from_file.side_effect = [conflict, None]
        manager = Mock()
        manager.gcs_config_path = "resources/configs"
        manager.bucket.blob.return_value = blob
        client = GCSClient.__new__(GCSClient)
        client.bucket_manager = manager
        client._config_lock = threading.RLock()

        with (
            patch("modules.gcp.random.uniform", return_value=0),
            patch("modules.gcp.time.sleep") as sleep,
        ):
            result = client._mutate_config(
                "manifest", lambda current: {**current, "updated": True}
            )

        self.assertEqual(result, {"existing": True, "updated": True})
        self.assertEqual(blob.upload_from_file.call_count, 2)
        sleep.assert_called_once_with(0.25)

    def test_cleanup_removes_only_safe_manifest_entries(self):
        manifest = {
            "orphan": {
                "uri": "gs://test-bucket/path/orphan_2026_01_01.parquet",
                "source_table_name": "time_table",
                "loaded_at": "2026-01-02T00:00:00",
            },
            "checkpointed": {
                "uri": "gs://test-bucket/path/time_table_2026_01_01.parquet",
                "source_table_name": "time_table",
                "loaded_at": "2026-01-02T00:00:00",
            },
            "recent": {
                "uri": "gs://test-bucket/path/time_table_2026_09_18.parquet",
                "source_table_name": "time_table",
                "loaded_at": "2026-09-19T00:00:00",
            },
            "oneshot-old": {
                "uri": "gs://test-bucket/path/oneshot.parquet",
                "source_table_name": "oneshot_table",
                "loaded_at": "2026-01-01T00:00:00",
            },
            "oneshot-new": {
                "uri": "gs://test-bucket/path/oneshot.parquet",
                "source_table_name": "oneshot_table",
                "loaded_at": "2026-09-18T00:00:00",
            },
        }
        manager = Mock(bucket_name="test-bucket")
        manager.file_exists.side_effect = lambda name: ("orphan" not in name, Mock())
        manager.get_start_date.side_effect = lambda name: (
            datetime(2026, 1, 1)
            if "2026_01_01" in name
            else datetime(2026, 9, 18)
        )
        client = GCSClient.__new__(GCSClient)
        client.bucket_manager = manager
        client.get_bigquery_load_manifest = Mock(return_value=manifest)

        report = client.cleanup_bigquery_load_manifest(
            configs_for_update={
                "time_table": {
                    "download_type": "time",
                    "last_db_migration_time": "19/09/2026",
                },
                "oneshot_table": {"download_type": "oneshot"},
            },
            retention_days=30,
            dry_run=True,
            now=datetime(2026, 9, 19),
        )

        self.assertEqual(report["total"], 5)
        self.assertEqual(report["kept"], 2)
        self.assertEqual(
            report["reasons"],
            {"orphan": 1, "checkpointed": 1, "obsolete_oneshot": 1},
        )


class ChunkPathTests(unittest.TestCase):
    def test_chunk_path_preserves_table_date_and_index(self):
        manager = BucketManager.__new__(BucketManager)
        manager.file_path_regex = BucketManager.FILE_PATH_REGEX
        path = (
            "vehicle_stats/fleet_vehicle_stats_faultCodes/"
            "fleet_vehicle_stats_faultCodes_2026_09_18_00001.parquet"
        )
        self.assertEqual(
            manager.parse_file_path(path, SearchRetrieveType.TABLE_NAME),
            "fleet_vehicle_stats_faultCodes",
        )
        self.assertEqual(
            manager.parse_file_path(path, SearchRetrieveType.DATE_START), "2026_09_18"
        )
        self.assertEqual(manager.parse_file_path(path, SearchRetrieveType.INDEX), "00001")

    def test_subday_chunk_path_preserves_table_and_dates(self):
        manager = BucketManager.__new__(BucketManager)
        manager.file_path_regex = BucketManager.FILE_PATH_REGEX
        path = (
            "assets/fleet_assets_reefers/"
            "fleet_assets_reefers_2026_09_21_000000_to_"
            "2026_09_21_055959_00001.parquet"
        )
        self.assertEqual(manager.get_table_name(path), "fleet_assets_reefers")
        self.assertEqual(
            manager.parse_file_path(path, SearchRetrieveType.DATE_START),
            "2026_09_21",
        )
        self.assertEqual(
            manager.parse_file_path(path, SearchRetrieveType.DATE_END),
            "2026_09_21",
        )
        self.assertEqual(manager.parse_file_path(path, SearchRetrieveType.INDEX), "00001")

    def test_subday_file_counts_as_present_for_its_calendar_day(self):
        manager = BucketManager.__new__(BucketManager)
        manager.list_parquet_files = Mock(
            return_value=[
                "assets/fleet_assets_reefers/"
                "fleet_assets_reefers_2026_09_21_000000_to_"
                "2026_09_21_055959_00001.parquet"
            ]
        )
        manager.logger = Mock()
        manager.bucket_name = "test-bucket"
        metadata = pd.DataFrame(
            [{"family": "assets", "table_name": "fleet_assets_reefers", "download_type": "time"}]
        )

        missing = manager.missing_dates(
            metadata=metadata,
            configs_for_update={},
            start_date=datetime(2026, 9, 21),
            end_date=datetime(2026, 9, 22),
        )

        self.assertEqual(dict(missing), {})


class LogCleanupTests(unittest.TestCase):
    def test_deletes_only_expired_log_blobs(self):
        now = datetime.now(timezone.utc)
        old_blob = Mock(updated=now - timedelta(days=31))
        recent_blob = Mock(updated=now - timedelta(days=2))
        manager = BucketManager.__new__(BucketManager)
        manager.gcs_log_path = "resources/logs"
        manager.bucket = Mock()
        manager.bucket.list_blobs.return_value = [old_blob, recent_blob]
        manager.logger = Mock()

        report = manager.cleanup_logs(retention_days=30)

        old_blob.delete.assert_called_once_with()
        recent_blob.delete.assert_not_called()
        self.assertEqual(report["deleted"], 1)


class ExecutionLockTests(unittest.TestCase):
    def test_uses_generation_preconditions_for_acquire_and_release(self):
        blob = Mock(generation=42)
        bucket = Mock()
        bucket.blob.return_value = blob
        lock = ExecutionLock(bucket, "resources/configs/pipeline.lock", 60)

        lock.acquire()
        lock.release()

        blob.upload_from_string.assert_called_once()
        self.assertEqual(
            blob.upload_from_string.call_args.kwargs["if_generation_match"], 0
        )
        blob.delete.assert_called_once_with(if_generation_match=42)
