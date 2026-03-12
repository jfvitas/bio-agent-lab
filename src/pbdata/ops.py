"""Backward-compatible operator-facing report wrappers."""

from __future__ import annotations

from pbdata.config import AppConfig
from pbdata.storage import StorageLayout
from pbdata.workspace_state import (
    build_demo_readiness_report as _build_demo_readiness_report,
    build_doctor_report as _build_doctor_report,
    build_status_report as _build_status_report,
)


def build_status_report(layout: StorageLayout) -> dict[str, object]:
    return _build_status_report(layout).to_dict()


def build_doctor_report(layout: StorageLayout, config: AppConfig) -> dict[str, object]:
    return _build_doctor_report(layout, config).to_dict()


def build_demo_readiness_report(layout: StorageLayout, config: AppConfig) -> dict[str, object]:
    return _build_demo_readiness_report(layout, config).to_dict()
