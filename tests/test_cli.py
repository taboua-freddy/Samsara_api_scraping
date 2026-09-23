import subprocess
import sys
import unittest
from pathlib import Path

from modules.metadata import get_tables_default_table_names


class DefaultTablesCliTests(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parents[1] / "main.py"),
             "--dry-run", *arguments],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_explicit_default_tables_selects_all_curated_tables(self):
        result = self.run_cli("--default-tables")
        self.assertEqual(result.returncode, 0, result.stderr)
        selected_line = next(
            line for line in result.stdout.splitlines()
            if line.startswith("Table names to process:")
        )
        for table in get_tables_default_table_names():
            self.assertIn(table, selected_line)
        self.assertNotIn("'addresses'", selected_line)

    def test_default_tables_rejects_ambiguous_selection(self):
        result = self.run_cli("--default-tables", "--table", "fleet_tags")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ne peut pas être combiné", result.stderr)


if __name__ == "__main__":
    unittest.main()
