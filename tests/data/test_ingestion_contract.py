from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from dineiq.dataset.ingestion_audit import resolve_source_paths, scan_source_files
from dineiq.dataset.ingestion_audit import TABLE_FILENAMES


class IngestionContractTests(unittest.TestCase):
    def test_canonical_file_in_raw_data_directory_is_supported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Customers.csv").write_text("Customer_ID\nC1\n", encoding="utf-8")
            self.assertEqual(resolve_source_paths(root, "customers"), (root / "Customers.csv",))

    def test_canonical_file_wins_over_split_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / "data"
            data.mkdir()
            columns = ("Customer_ID", "Customer_Name")
            for filename in ("Customers.csv",):
                with (data / filename).open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=columns)
                    writer.writeheader()
                    writer.writerow({column: "" for column in columns})
            parts = data / "customers"
            parts.mkdir()
            (parts / "part-000.csv").write_text("ignored\n", encoding="utf-8")
            self.assertEqual(resolve_source_paths(root, "customers"), (data / "Customers.csv",))

    def test_split_files_are_sorted_and_inventoried(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / "data"
            data.mkdir()
            columns = ("Order_ID", "Channel")
            for filename, name in (("orders_02.csv", "two"), ("orders_01.csv", "one")):
                with (data / filename).open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=columns)
                    writer.writeheader()
                    writer.writerow({"Order_ID": name, "Channel": "Dine-in"})
            paths = resolve_source_paths(root, "orders")
            self.assertEqual([path.name for path in paths], ["orders_01.csv", "orders_02.csv"])
            report = scan_source_files(root, ["orders"])
            self.assertTrue(report["passed"])
            self.assertEqual(report["source_file_count"], 2)
            self.assertEqual(report["total_rows"], 2)


if __name__ == "__main__":
    unittest.main()
