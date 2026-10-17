from __future__ import annotations

import tempfile
import sqlite3
import unittest
from pathlib import Path

from dineiq.error_reporting import operation_error_message


class OperationErrorReportingTests(unittest.TestCase):
    def test_missing_input_error_gives_actionable_safe_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "logs" / "application.log"
            message = operation_error_message(
                "Forecast build", FileNotFoundError("private/path/customer.csv"), log
            )
            self.assertIn("Forecast build could not complete", message)
            self.assertIn("check the configured dataset and build outputs", message)
            self.assertIn("FileNotFoundError", message)
            self.assertNotIn("private/path/customer.csv", message)
            self.assertIn("FileNotFoundError", log.read_text(encoding="utf-8"))
            self.assertIn("private/path/customer.csv", log.read_text(encoding="utf-8"))

    def test_database_error_logs_diagnostics_and_returns_user_facing_message(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "application.log"
            error = sqlite3.OperationalError("database is locked")
            message = operation_error_message("Saving prediction", error, log)
            self.assertIn("unexpected processing, model, Spark, or database error", message)
            self.assertIn("Diagnostic details (OperationalError)", message)
            self.assertIn("database is locked", log.read_text(encoding="utf-8"))

    def test_permission_error_explains_how_to_recover(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            message = operation_error_message(
                "Saving report", PermissionError("denied"), Path(directory) / "app.log"
            )
            self.assertIn("check file permissions", message)


if __name__ == "__main__":
    unittest.main()
