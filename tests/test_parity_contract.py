from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from dineiq.dataset.parity import check_table_parity


class ParityContractTests(unittest.TestCase):
    def test_missing_external_parquet_is_reported_not_silently_passed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = check_table_parity(Path(tmp), "customers")
        self.assertFalse(result["passed"])
        self.assertFalse(result["available"])
        self.assertIn("missing", result["error"])


if __name__ == "__main__":
    unittest.main()
