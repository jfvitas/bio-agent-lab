"""Storage-usage reporting helpers for large local and cluster workspaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class StorageAreaReport:
    label: str
    path: Path
    bytes_used: int
    file_count: int
    exists: bool


@dataclass(frozen=True)
class StorageUsageReport:
    storage_root: Path
    total_bytes: int
    areas: tuple[StorageAreaReport, ...]


def _safe_tree_stats(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    total_bytes = 0
    file_count = 0
    for child in path.rglob("*"):
        try:
            if child.is_file():
                total_bytes += child.stat().st_size
                file_count += 1
        except OSError:
            continue
    return total_bytes, file_count


def _area(label: str, path: Path) -> StorageAreaReport:
    bytes_used, file_count = _safe_tree_stats(path)
    return StorageAreaReport(
        label=label,
        path=path,
        bytes_used=bytes_used,
        file_count=file_count,
        exists=path.exists(),
    )


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"


def build_storage_usage_report(layout: StorageLayout) -> StorageUsageReport:
    areas = (
        _area("Raw RCSB", layout.raw_rcsb_dir),
        _area("Raw packages", layout.raw_rcsb_packages_dir),
        _area("Structures", layout.structures_rcsb_dir),
        _area("Extracted", layout.extracted_dir),
        _area("Consolidated extracted", layout.extracted_consolidated_dir),
        _area("Graph", layout.graph_dir),
        _area("Features", layout.features_dir),
        _area("Training examples", layout.training_dir),
        _area("Models", layout.models_dir),
        _area("Precompute runs", layout.precompute_runs_dir),
    )
    total_bytes = sum(area.bytes_used for area in areas)
    return StorageUsageReport(storage_root=layout.root, total_bytes=total_bytes, areas=areas)


def render_storage_usage_report(report: StorageUsageReport) -> list[str]:
    lines = [
        f"Storage root: {report.storage_root}",
        f"Tracked total: {_format_bytes(report.total_bytes)}",
    ]
    sorted_areas = sorted(report.areas, key=lambda area: area.bytes_used, reverse=True)
    for area in sorted_areas:
        status = "present" if area.exists else "missing"
        lines.append(
            f"{area.label:22} | {_format_bytes(area.bytes_used):>10} | files={area.file_count:,} | {status} | {area.path}"
        )
    largest = next((area for area in sorted_areas if area.bytes_used > 0), None)
    if largest is not None:
        lines.append(f"Largest tracked area: {largest.label} ({_format_bytes(largest.bytes_used)})")
    return lines
