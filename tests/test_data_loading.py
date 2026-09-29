from __future__ import annotations

import importlib
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


class DataLoadingTests(unittest.TestCase):
    def test_bounded_parquet_reader_converts_scanned_record_groups(self) -> None:
        class RecordGroup:
            def __init__(self, values):
                self.values = values

            def to_pandas(self):
                return pd.DataFrame(self.values)

        class Scanner:
            def to_batches(self):
                return [
                    RecordGroup({"id": [1, 2]}),
                    RecordGroup({"id": [3, 4]}),
                ]

        class Dataset:
            schema = types.SimpleNamespace(names=["id"])

            def scanner(self, **_kwargs):
                return Scanner()

        fake_dataset_module = types.ModuleType("pyarrow.dataset")
        fake_dataset_module.dataset = lambda *_args, **_kwargs: Dataset()

        # Import the application module normally, then patch its existing
        # PyArrow dataset reference. This also works if the module was imported
        # earlier by another test in the same Python process.
        module = importlib.import_module("dineiq.ui.data")
        with patch.object(module, "ds", fake_dataset_module):
            with tempfile.TemporaryDirectory() as directory:
                parquet_path = Path(directory) / "sample"
                parquet_path.mkdir()
                result = module.read_parquet(parquet_path, max_rows=3)

        self.assertEqual(result["id"].tolist(), [1, 2, 3])

    def test_missing_pyarrow_is_reported_as_actionable_runtime_error(self) -> None:
        module = importlib.import_module("dineiq.ui.data")
        with tempfile.TemporaryDirectory() as directory:
            parquet_path = Path(directory) / "outputs"
            parquet_path.mkdir()
            with patch.object(module, "ds", None), patch.dict("sys.modules", {"pyarrow": None, "pyarrow.dataset": None}):
                with self.assertRaisesRegex(RuntimeError, "PyArrow is not installed"):
                    module.read_parquet(parquet_path)


if __name__ == "__main__":
    unittest.main()
