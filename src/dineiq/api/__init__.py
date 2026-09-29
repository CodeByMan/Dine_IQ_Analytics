"""Minimal local HTTP integration adapter for the DineIQ domain services."""

from dineiq.api.server import ApiApplication, ApiResponse, create_server, run_server

__all__ = ["ApiApplication", "ApiResponse", "create_server", "run_server"]
