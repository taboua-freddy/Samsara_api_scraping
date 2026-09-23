import threading
import json
import io
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.resumable_media.common import InvalidResponse

from modules.gcp import (
    BigQueryManager,
    BucketManager,
    ExecutionLock,
    GCSClient,
    GCSBigQueryLoader,
    build_load_fingerprint,
    current_oneshot_files,
)
from modules.extraction_manifest import partition_key, request_signature
from modules.interface import SearchRetrieveType


class BigQueryManagerTests(unittest.TestCase):
    def test_oneshot_batch_uses_single_truncate_job(self):
        load_job = Mock(errors=None)
        load_job.result.return_value = load_job
        client = Mock(project="project")
        client.load_table_from_uri.return_value = load_job
        manager = BigQueryManager.__new__(BigQueryManager)
        manager.bigquery_client = client
        manager.dataset_id = "dataset"
        manager.logger = Mock()
        manager.memory_manager = Mock()
        manager.memory_manager.read.return_value = pd.DataFrame([{
            "table_name": "fleet_tags",
            "download_type": "oneshot",
            "time_partitioning_field": None,
            "clustering_fields": None,
        }])
        uris = ["gs://bucket/part1.parquet", "gs://bucket/part2.parquet"]

        manager.load_parquet_to_bigquery(uris, "fleet_tags")

        call = client.load_table_from_uri.call_args
        self.assertEqual(call.args[0], uris)
        self.assertEqual(
            call.kwargs["job_config"].write_disposition,
            bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

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
        job_config = client.load_table_from_uri.call_args.kwargs["job_config"]
        self.assertIn(
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            job_config.schema_update_options,
        )

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
    def test_oneshot_snapshot_selection_keeps_previous_complete_during_retry(self):
        previous = {"status": "complete", "uploaded_files": ["old.parquet"]}
        self.assertEqual(
            current_oneshot_files({
                "snapshot": {
                    "status": "in_progress",
                    "previous_complete": previous,
                    "uploaded_files": ["partial.parquet"],
                }
            }),
            ["old.parquet"],
        )

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

    def test_cleanup_keeps_only_latest_oneshot_snapshot_across_different_uris(self):
        client = GCSClient.__new__(GCSClient)
        client.bucket_manager = Mock(bucket_name="test-bucket")
        client.bucket_manager.file_exists.return_value = (True, Mock())
        client.get_bigquery_load_manifest = Mock(return_value={
            "old": {
                "uri": "gs://test-bucket/old.parquet",
                "table_name": "fleet_tags_vehicles",
                "source_table_name": "fleet_tags",
                "loaded_at": "2026-09-22T00:00:00",
            },
            "new": {
                "uri": "gs://test-bucket/new1.parquet",
                "uris": ["gs://test-bucket/new1.parquet", "gs://test-bucket/new2.parquet"],
                "table_name": "fleet_tags_vehicles",
                "source_table_name": "fleet_tags",
                "loaded_at": "2026-09-23T00:00:00",
            },
        })

        report = client.cleanup_bigquery_load_manifest(
            configs_for_update={"fleet_tags": {"download_type": "oneshot"}},
            dry_run=True,
        )

        self.assertEqual(report["kept"], 1)
        self.assertEqual(report["reasons"]["obsolete_oneshot"], 1)


class OneshotLoadTests(unittest.TestCase):
    def test_normalizes_optional_columns_across_oneshot_chunks(self):
        paths = ["assets/fleet_tags/first.parquet", "assets/fleet_tags/second.parquet"]
        objects = {}
        for path, table in zip(paths, [
            pa.table({"id": [1], "first_only": ["a"]}),
            pa.table({"id": [2], "second_only": ["b"]}),
        ]):
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            objects[path] = buffer.getvalue()

        def download(path):
            blob = Mock()
            blob.download_to_filename.side_effect = (
                lambda filename: Path(filename).write_bytes(objects[path])
            )
            return blob

        target = BucketManager.__new__(BucketManager)
        target.bucket = Mock()
        target.bucket.blob.side_effect = download
        target.upload_bytes = Mock(side_effect=(
            lambda buffer, path: objects.__setitem__(path, buffer.getvalue())
        ))
        client = GCSClient.__new__(GCSClient)
        client.target_bucket_manager = target

        client._normalize_oneshot_parquet_schemas(paths)

        first, second = [pq.read_table(io.BytesIO(objects[path])) for path in paths]
        self.assertEqual(first.schema.names, second.schema.names)
        self.assertEqual(first.schema.names, ["id", "first_only", "second_only"])
        self.assertEqual(first["second_only"].to_pylist(), [None])
        self.assertEqual(second["first_only"].to_pylist(), [None])

    def test_transform_reads_only_latest_completed_snapshot(self):
        prefix = "assets/fleet_tags/"
        old = f"{prefix}fleet_tags_2026_09_22_100000_1111111100001.parquet"
        current = f"{prefix}fleet_tags_2026_09_23_100000_2222222200001.parquet"
        raw_client = GCSClient.__new__(GCSClient)
        raw_client.bucket_manager = Mock()
        raw_client.bucket_manager.list_parquet_files.return_value = [old, current]
        raw_client.bucket_manager.get_start_date.return_value = None
        raw_client.bucket_manager.get_end_date.return_value = None
        raw_client.get_extraction_manifest = Mock(return_value={
            "latest": {"status": "complete", "uploaded_files": [current]}
        })
        with (
            patch.object(BucketManager, "__init__", return_value=None),
            patch("modules.gcp.parallelize_execution", return_value=[]) as parallel,
        ):
            report = raw_client.transform_and_save_data(
                "flat-bucket",
                pd.DataFrame([{
                    "family": "assets", "table_name": "fleet_tags",
                    "download_type": "oneshot",
                }]),
                configs_for_update={},
            )

        self.assertEqual(report["selected_files"], 1)
        self.assertEqual(parallel.call_args.kwargs["tasks"][0]["file_path"], current)

    def test_loads_only_active_chunks_in_one_replace_job(self):
        prefix = "assets/fleet_tags/"
        names = [
            f"{prefix}fleet_tags_vehicles_2026_09_22_100000_1111111100001.parquet",
            f"{prefix}fleet_tags_vehicles_2026_09_23_100000_2222222200001.parquet",
            f"{prefix}fleet_tags_vehicles_2026_09_23_100000_2222222200002.parquet",
        ]
        active_raw = [
            f"{prefix}fleet_tags_2026_09_23_100000_2222222200001.parquet",
            f"{prefix}fleet_tags_2026_09_23_100000_2222222200002.parquet",
        ]
        loader = GCSBigQueryLoader.__new__(GCSBigQueryLoader)
        loader.bucket_name = "flat-bucket"
        loader.bucket_manager = Mock()
        loader.bucket_manager.list_parquet_file_metadata.return_value = [
            {"name": name, "generation": str(index + 1)}
            for index, name in enumerate(names)
        ]
        loader.bucket_manager.get_table_name.return_value = "fleet_tags_vehicles"
        loader.gcs_client = Mock()
        loader.gcs_client.get_bigquery_load_manifest.return_value = {}
        loader.raw_gcs_client = Mock()
        loader.raw_gcs_client.get_extraction_manifest.return_value = {
            "latest": {"status": "complete", "uploaded_files": active_raw}
        }
        loader.bigquery_manager = Mock()
        loader.bigquery_manager.load_parquet_to_bigquery.side_effect = (
            lambda uri, table_name, job_id: (uri, Mock())
        )
        loader.logger = Mock()
        loader._from = None
        loader._to = None
        loader.max_workers = 1

        loader.run(
            configs_for_update={},
            metadata=pd.DataFrame([{
                "family": "assets",
                "table_name": "fleet_tags",
                "download_type": "oneshot",
            }]),
        )

        loader.bigquery_manager.load_parquet_to_bigquery.assert_called_once()
        uris = loader.bigquery_manager.load_parquet_to_bigquery.call_args.kwargs["uri"]
        self.assertEqual(uris, [f"gs://flat-bucket/{name}" for name in names[1:]])
        manifest_entry = next(iter(loader.gcs_client.update_bigquery_load_manifest.call_args.args[0].values()))
        self.assertEqual(manifest_entry["uris"], uris)


class ExtractionManifestStorageTests(unittest.TestCase):
    def test_uses_a_distinct_manifest_path_per_table(self):
        client = GCSClient.__new__(GCSClient)
        client._get_config = Mock(return_value={})
        client._mutate_config = Mock()

        client.get_extraction_manifest("fleet_assets_reefers")
        client.update_extraction_state("fleet_assets_reefers", "key", {"status": "complete"})

        client._get_config.assert_called_once_with(
            "extraction_manifests/fleet_assets_reefers"
        )
        self.assertEqual(
            client._mutate_config.call_args.args[0],
            "extraction_manifests/fleet_assets_reefers",
        )

    def test_migrates_only_matching_legacy_utc_intervals(self):
        client = GCSClient.__new__(GCSClient)
        endpoint = "v1/fleet/assets/reefers"
        table = "fleet_assets_reefers"
        identity = (
            "assets/fleet_assets_reefers/"
            "fleet_assets_reefers_2026_09_21_000000_to_2026_09_21_055959"
            f"|{endpoint}"
        )
        client._get_config = Mock(return_value={
            "old": {
                "identity": identity,
                "status": "complete",
                "uploaded_files": ["existing.parquet"],
                "next_cursor": None,
            }
        })
        client._mutate_config = Mock()
        client.bucket_manager = Mock()
        client.bucket_manager.file_exists.return_value = (True, Mock())

        client.migrate_legacy_extraction_state(
            table, endpoint, {"startMs": 0, "endMs": 1}
        )

        name, mutator = client._mutate_config.call_args.args
        updated = mutator({})
        signature = request_signature(table, endpoint, {"startMs": 0, "endMs": 1})
        imported = [value for value in updated.values() if value.get("status") == "complete"]
        self.assertEqual(name, "extraction_manifests/fleet_assets_reefers")
        self.assertEqual(len(imported), 1)
        self.assertEqual(imported[0]["signature"], signature)
        self.assertEqual(
            partition_key(signature, imported[0]["start_ms"], imported[0]["end_exclusive_ms"]),
            next(key for key, value in updated.items() if value is imported[0]),
        )

    def test_imports_legacy_keys_by_table_without_deleting_global_manifest(self):
        client = GCSClient.__new__(GCSClient)
        client.bucket_manager = Mock()
        client._get_config = Mock(side_effect=[{}, {
            "first": {"identity": "assets/reefers/reefers_2026_09_21|endpoint"},
            "other": {"identity": "assets/other/other_2026_09_21|endpoint"},
        }])
        client._mutate_config = Mock()

        client.migrate_legacy_table_state("reefers")

        name, mutator = client._mutate_config.call_args.args
        result = mutator({})
        self.assertEqual(name, "extraction_manifests/reefers")
        self.assertIn("first", result)
        self.assertNotIn("other", result)
        self.assertTrue(result["__meta__"]["legacy_table_imported"])


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

    def test_manifest_does_not_count_partial_day_as_complete(self):
        manager = BucketManager.__new__(BucketManager)
        manager.logger = Mock()
        manager.bucket_name = "test-bucket"
        manager.gcs_config_path = "resources/configs"
        manager.list_parquet_files = Mock()
        manager.file_exists = Mock(return_value=(True, Mock()))
        table = "fleet_assets_reefers"
        endpoint = "v1/fleet/assets/reefers"
        signature = request_signature(table, endpoint, {"startMs": 1, "endMs": 86_400_001})
        manifest = {
            "first-window": {
                "signature": signature,
                "start_ms": 1,
                "end_exclusive_ms": 21_600_001,
                "status": "complete",
                "uploaded_files": ["first.parquet"],
            }
        }
        blob = Mock()
        blob.exists.return_value = True
        blob.download_as_bytes.return_value = json.dumps(manifest).encode()
        manager.bucket = Mock()
        manager.bucket.blob.return_value = blob
        metadata = pd.DataFrame([{
            "family": "assets", "table_name": table, "endpoint": endpoint,
            "params": "startMs=1,endMs=86400001", "download_type": "time",
        }])

        missing = manager.missing_dates(
            metadata=metadata,
            configs_for_update={},
            start_date=datetime(2026, 9, 21),
            end_date=datetime(2026, 9, 22),
        )

        self.assertEqual(missing[table], [datetime(2026, 9, 21)])
        manager.list_parquet_files.assert_not_called()


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
