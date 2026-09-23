import unittest

from modules.extraction_manifest import (
    has_complete_coverage,
    legacy_utc_bounds,
    partition_key,
    plan_timestamp_intervals,
    request_signature,
)


class ExtractionManifestTests(unittest.TestCase):
    def setUp(self):
        self.signature = request_signature(
            "fleet_assets_reefers", "v1/fleet/assets/reefers",
            {"startMs": 0, "endMs": 86_400_000},
        )

    def state(self, start, end, status="complete", files=None, **extra):
        return {
            "signature": self.signature,
            "start_ms": start,
            "end_exclusive_ms": end,
            "status": status,
            "uploaded_files": files if files is not None else [f"{start}.parquet"],
            **extra,
        }

    def test_policy_change_does_not_redownload_complete_coverage(self):
        manifest = {
            "six-hours": self.state(0, 6_000),
            "three-hours": self.state(6_000, 9_000),
            "failed": self.state(9_000, 12_000, status="failed"),
        }
        self.assertEqual(
            plan_timestamp_intervals(
                0, 12_000, 1_000, manifest, self.signature, lambda _: True
            ),
            [(9_000, 10_000), (10_000, 11_000), (11_000, 12_000)],
        )

    def test_in_progress_partition_keeps_original_bounds_and_cursor(self):
        manifest = {
            "partial": self.state(
                3_000, 6_000, status="in_progress", next_cursor="abc"
            ),
        }
        self.assertEqual(
            plan_timestamp_intervals(
                0, 9_000, 1_000, manifest, self.signature, lambda _: True
            ),
            [(0, 1_000), (1_000, 2_000), (2_000, 3_000),
             (3_000, 6_000), (6_000, 7_000), (7_000, 8_000), (8_000, 9_000)],
        )

    def test_split_children_remain_boundaries_after_restart(self):
        manifest = {
            "parent": self.state(
                0, 6_000, status="split", children=[[0, 3_000], [3_000, 6_000]]
            )
        }
        self.assertEqual(
            plan_timestamp_intervals(
                0, 6_000, 6_000, manifest, self.signature, lambda _: True
            ),
            [(0, 3_000), (3_000, 6_000)],
        )

    def test_missing_blob_is_not_complete_coverage(self):
        manifest = {"one": self.state(0, 6_000, files=["missing.parquet"])}
        self.assertFalse(
            has_complete_coverage(
                0, 6_000, manifest, self.signature, lambda _: False
            )
        )
        self.assertEqual(
            plan_timestamp_intervals(
                0, 6_000, 6_000, manifest, self.signature, lambda _: False
            ),
            [(0, 6_000)],
        )

    def test_empty_completed_interval_is_covered(self):
        manifest = {"empty": self.state(0, 6_000, files=[])}
        self.assertTrue(
            has_complete_coverage(
                0, 6_000, manifest, self.signature, lambda _: False
            )
        )

    def test_signature_ignores_window_but_not_data_filters(self):
        base = request_signature("t", "endpoint", {"startMs": 1, "endMs": 2})
        changed_window = request_signature("t", "endpoint", {"startMs": 3, "endMs": 4})
        filtered = request_signature(
            "t", "endpoint", {"startMs": 3, "endMs": 4, "type": "A"}
        )
        self.assertEqual(base, changed_window)
        self.assertNotEqual(base, filtered)
        self.assertEqual(partition_key(base, 0, 1), partition_key(changed_window, 0, 1))

    def test_legacy_subday_name_yields_exact_utc_interval(self):
        bounds = legacy_utc_bounds(
            "fleet_assets_reefers",
            "assets/fleet_assets_reefers/"
            "fleet_assets_reefers_2026_09_21_000000_to_2026_09_21_055959"
            "|v1/fleet/assets/reefers",
        )
        self.assertIsNotNone(bounds)
        self.assertEqual(bounds[1] - bounds[0], 6 * 3_600_000)


if __name__ == "__main__":
    unittest.main()
