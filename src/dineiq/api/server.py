"""Dependency-light HTTP adapter over the existing DineIQ services.

The SRS does not require a separate analytics microservice.  This adapter exists
for the required integration boundary: authenticated operational CRUD, report
catalog discovery, persisted-model prediction requests, health diagnostics, and
structured errors.  Streamlit and this adapter call the same SQLite/domain
functions; the adapter never creates alternate analytics or synthetic results.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import math
import sqlite3
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, unquote, urlsplit

from dineiq.config import Settings, load_settings
from dineiq.db.auth import authenticate
from dineiq.db.operations import recent_jobs
from dineiq.db.permissions import Principal
from dineiq.db.repository import list_records, save_record
from dineiq.db.schema import initialize_database


LOGGER = logging.getLogger(__name__)
MAX_BODY_BYTES = 1_000_000


class ApiError(Exception):
    """An expected request or service error with an HTTP status."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ApiResponse:
    status: int
    payload: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)


def _json_safe(value: Any) -> Any:
    """Convert pandas/numpy scalar values without allowing NaN JSON output."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item") and callable(value.item):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _json_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(_json_safe(payload), ensure_ascii=False, allow_nan=False).encode("utf-8")


class ApiApplication:
    """Route requests to existing authenticated DineIQ services.

    ``demand_predictor`` and ``wastage_predictor`` are injectable for contract
    tests.  In production they are loaded lazily from the persisted artifacts.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        demand_predictor: Any | None = None,
        demand_predictor_factory: Callable[[Settings], Any] | None = None,
        wastage_predictor: Any | None = None,
        wastage_predictor_factory: Callable[[Settings], Any] | None = None,
        max_body_bytes: int = MAX_BODY_BYTES,
    ) -> None:
        self.settings = settings or load_settings()
        self.max_body_bytes = max(1, int(max_body_bytes))
        self._demand_predictor = demand_predictor
        self._demand_predictor_factory = demand_predictor_factory
        self._wastage_predictor = wastage_predictor
        self._wastage_predictor_factory = wastage_predictor_factory
        self._spark = None
        initialize_database(self.settings.database_path)

    def dispatch(
        self,
        method: str,
        target: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes | str | None = None,
    ) -> ApiResponse:
        """Dispatch one HTTP-like request without requiring a network socket."""
        method = method.upper().strip()
        request_headers = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        parsed = urlsplit(target)
        segments = [unquote(part) for part in parsed.path.strip("/").split("/") if part]
        query = parse_qs(parsed.query, keep_blank_values=True)
        try:
            if segments in (["health"], ["api", "v1", "health"], ["api", "v1", "status"]):
                if method != "GET":
                    raise ApiError(405, "method_not_allowed", "This endpoint accepts GET requests only.")
                return self._ok(self._health())

            if segments == ["api", "v1", "reports"]:
                if method != "GET":
                    raise ApiError(405, "method_not_allowed", "This endpoint accepts GET requests only.")
                self._principal(request_headers)
                return self._ok({"reports": self._report_catalog()})

            if segments == ["api", "v1", "jobs"]:
                if method != "GET":
                    raise ApiError(405, "method_not_allowed", "This endpoint accepts GET requests only.")
                self._principal(request_headers)
                return self._ok({"jobs": recent_jobs(str(self.settings.database_path))})

            if len(segments) == 4 and segments[:3] == ["api", "v1", "entities"]:
                principal = self._principal(request_headers)
                entity = segments[3]
                if method == "GET":
                    return self._ok({"entity": entity, "records": list_records(
                        str(self.settings.database_path), entity, principal,
                        limit=self._limit(query),
                    )})
                if method == "POST":
                    payload = self._body_object(body)
                    values = payload.get("values", payload)
                    if not isinstance(values, dict):
                        raise ApiError(400, "invalid_payload", "Request body must be a JSON object of record fields.")
                    record_id = payload.get("record_id")
                    save_record(str(self.settings.database_path), entity, values, principal,
                                record_id=str(record_id) if record_id is not None else None)
                    return self._ok({"entity": entity, "saved": True}, status=201)
                raise ApiError(405, "method_not_allowed", "This endpoint accepts GET and POST requests.")

            if segments == ["api", "v1", "predictions", "demand"]:
                if method != "POST":
                    raise ApiError(405, "method_not_allowed", "This endpoint accepts POST requests only.")
                self._principal(request_headers)
                return self._ok({"prediction": self._predict_demand(self._body_object(body))})

            if segments == ["api", "v1", "predictions", "wastage"]:
                if method != "POST":
                    raise ApiError(405, "method_not_allowed", "This endpoint accepts POST requests only.")
                self._principal(request_headers)
                return self._ok({"prediction": self._predict_wastage(self._body_object(body))})

            raise ApiError(404, "not_found", "The requested DineIQ API route does not exist.")
        except ApiError as exc:
            return self._error(exc.status, exc.code, exc.message)
        except PermissionError as exc:
            return self._error(403, "forbidden", str(exc))
        except sqlite3.IntegrityError as exc:
            return self._error(409, "integrity_error", f"The record could not be saved: {exc}")
        except LookupError as exc:
            return self._error(404, "not_found", str(exc))
        except ValueError as exc:
            return self._error(422, "validation_error", str(exc))
        except FileNotFoundError:
            return self._error(503, "service_unavailable", "The required persisted model or data artifact is unavailable.")
        except Exception:
            LOGGER.exception("Unhandled DineIQ API request failure")
            return self._error(500, "internal_error", "The DineIQ service could not complete the request.")

    @staticmethod
    def _ok(data: dict[str, Any], *, status: int = 200) -> ApiResponse:
        return ApiResponse(status, {"ok": True, **data})

    @staticmethod
    def _error(status: int, code: str, message: str) -> ApiResponse:
        return ApiResponse(status, {"ok": False, "error": {"code": code, "message": message}})

    def _health(self) -> dict[str, Any]:
        return {
            "service": "dineiq-api",
            "status": "ok",
            "dataset_available": (self.settings.data_root / "data").is_dir(),
            "artifacts_available": self.settings.artifacts_root.is_dir(),
            "reports_available": self.settings.reports_root.is_dir(),
            "database_path": str(self.settings.database_path),
        }

    def _principal(self, headers: Mapping[str, str]) -> Principal:
        raw = headers.get("authorization", "")
        if not raw.lower().startswith("basic "):
            raise ApiError(401, "authentication_required", "Basic authentication is required for this endpoint.")
        try:
            decoded = base64.b64decode(raw[6:].strip(), validate=True).decode("utf-8")
            username, password = decoded.split(":", 1)
        except (ValueError, UnicodeError, binascii.Error) as exc:
            raise ApiError(401, "invalid_credentials", "The Authorization header is invalid.") from exc
        principal = authenticate(str(self.settings.database_path), username, password)
        if principal is None:
            raise ApiError(401, "invalid_credentials", "Username or password is incorrect.")
        return principal

    def _body_object(self, body: bytes | str | None) -> dict[str, Any]:
        if body is None:
            raise ApiError(400, "invalid_payload", "A JSON request body is required.")
        raw = body.encode("utf-8") if isinstance(body, str) else bytes(body)
        if len(raw) > self.max_body_bytes:
            raise ApiError(413, "payload_too_large", "The JSON request body is too large.")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, "invalid_json", "Request body must be valid UTF-8 JSON.") from exc
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_payload", "Request body must be a JSON object.")
        return payload

    @staticmethod
    def _limit(query: Mapping[str, list[str]]) -> int:
        values = query.get("limit", ["500"])
        try:
            limit = int(values[0])
        except (TypeError, ValueError) as exc:
            raise ApiError(400, "invalid_limit", "limit must be an integer between 1 and 5000.") from exc
        if not 1 <= limit <= 5000:
            raise ApiError(400, "invalid_limit", "limit must be an integer between 1 and 5000.")
        return limit

    def _report_catalog(self) -> list[dict[str, Any]]:
        from dineiq.ui.catalog import OUTPUT_PATHS, output_path
        from dineiq.ui.reports import REPORT_EXPORT_KEYS, REPORT_INPUTS, REPORT_REQUIREMENTS, report_filename

        entries = []
        for name, requirement in REPORT_REQUIREMENTS.items():
            source_key = REPORT_INPUTS[name]
            if source_key == "wastage_records":
                available = (self.settings.data_root / "data" / "wastage.csv").is_file()
            elif source_key == "anomalies":
                available = all(output_path(self.settings, key).is_dir() for key in ("sales_anomalies", "rating_anomalies"))
            elif source_key in OUTPUT_PATHS:
                available = output_path(self.settings, source_key).is_dir()
            else:
                available = False
            entries.append({
                "name": name,
                "requirement": requirement,
                "export_key": REPORT_EXPORT_KEYS[name],
                "filename": report_filename(name),
                "available": available,
            })
        return entries

    def _predict_demand(self, payload: dict[str, Any]) -> dict[str, Any]:
        from dineiq.analytics.model_serving import DemandRequest

        required = {"forecast_date", "item_id", "location_id", "is_available", "lag_1", "lag_7", "trailing_7_mean"}
        missing = sorted(required - set(payload))
        if missing:
            raise ApiError(422, "validation_error", f"Demand request is missing fields: {missing}")
        request = DemandRequest(**{key: payload[key] for key in required})
        request.validated()
        predictor = self._get_demand_predictor()
        return _json_safe(predictor.predict(request))

    def _get_demand_predictor(self) -> Any:
        if self._demand_predictor is not None:
            return self._demand_predictor
        if self._demand_predictor_factory is not None:
            self._demand_predictor = self._demand_predictor_factory(self.settings)
            return self._demand_predictor
        from dineiq.analytics.model_serving import DualDemandPredictor
        from dineiq.dataset.pipeline import create_local_spark

        self._spark = create_local_spark(self.settings, "DineIQ-API-Live-Demand-Inference")
        self._demand_predictor = DualDemandPredictor.load(self._spark, self.settings)
        return self._demand_predictor

    def _predict_wastage(self, payload: dict[str, Any]) -> dict[str, Any]:
        import pandas as pd
        from dineiq.analytics.wastage_risk_model import FEATURE_COLUMNS

        features = payload.get("features", payload)
        if not isinstance(features, dict):
            raise ApiError(422, "validation_error", "Wastage features must be a JSON object.")
        missing = sorted(set(FEATURE_COLUMNS) - set(features))
        if missing:
            raise ApiError(422, "validation_error", f"Wastage request is missing features: {missing}")
        predictor = self._get_wastage_predictor()
        result = predictor.predict(pd.DataFrame([features]))
        if result.empty:
            raise ApiError(503, "prediction_unavailable", "The wastage model returned no prediction.")
        return _json_safe(result.iloc[0].to_dict())

    def _get_wastage_predictor(self) -> Any:
        if self._wastage_predictor is not None:
            return self._wastage_predictor
        if self._wastage_predictor_factory is not None:
            self._wastage_predictor = self._wastage_predictor_factory(self.settings)
            return self._wastage_predictor
        from dineiq.analytics.wastage_risk_model import WastageRiskPredictor

        self._wastage_predictor = WastageRiskPredictor.load(
            self.settings.models_dir / "wastage_risk" / "selected_model.joblib"
        )
        return self._wastage_predictor


class _DineIQRequestHandler(BaseHTTPRequestHandler):
    """Translate HTTP requests into :class:`ApiApplication` responses."""

    server: "DineIQHTTPServer"

    def _handle(self) -> None:
        length_text = self.headers.get("Content-Length", "0")
        try:
            length = int(length_text)
        except ValueError:
            length = -1
        if length < 0 or length > self.server.application.max_body_bytes:
            response = ApiResponse(413, {"ok": False, "error": {"code": "payload_too_large", "message": "Request body is too large."}})
        else:
            body = self.rfile.read(length) if length else b""
            response = self.server.application.dispatch(
                self.command, self.path, headers=dict(self.headers.items()), body=body
            )
        payload = _json_payload(response.payload)
        self.send_response(response.status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        for key, value in response.headers.items():
            self.send_header(key, value)
        if response.status == 401:
            self.send_header("WWW-Authenticate", 'Basic realm="DineIQ API"')
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.address_string(), format % args)


class DineIQHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], application: ApiApplication):
        self.application = application
        super().__init__(address, _DineIQRequestHandler)


def create_server(
    settings: Settings | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8502,
    application: ApiApplication | None = None,
) -> DineIQHTTPServer:
    """Create a testable local server; callers control its lifecycle."""
    if not 0 <= int(port) <= 65535:
        raise ValueError("port must be between 0 and 65535")
    return DineIQHTTPServer((host, int(port)), application or ApiApplication(settings))


def run_server(settings: Settings | None = None, *, host: str = "127.0.0.1", port: int = 8502) -> None:
    """Run the local API until interrupted."""
    server = create_server(settings, host=host, port=port)
    print(f"DineIQ API listening on http://{host}:{server.server_address[1]}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


__all__ = ["ApiApplication", "ApiError", "ApiResponse", "create_server", "run_server"]
