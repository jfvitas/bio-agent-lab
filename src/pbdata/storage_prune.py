"""Safe cleanup helpers for regenerable managed storage artifacts."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from pbdata.precompute import build_precompute_run_status
from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class StoragePruneCandidate:
    category: str
    label: str
    path: Path
    bytes_used: int
    file_count: int
    run_id: str | None = None


@dataclass(frozen=True)
class StoragePrunePlan:
    storage_root: Path
    total_bytes: int
    total_files: int
    candidates: tuple[StoragePruneCandidate, ...]


@dataclass(frozen=True)
class StoragePruneResult:
    storage_root: Path
    total_bytes: int
    total_files: int
    deleted_paths: tuple[Path, ...]
    candidates: tuple[StoragePruneCandidate, ...]


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


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"


def build_storage_prune_plan(
    layout: StorageLayout,
    *,
    include_precompute_shards: bool = True,
    include_precompute_merged: bool = True,
    run_id: str | None = None,
) -> StoragePrunePlan:
    candidates: list[StoragePruneCandidate] = []
    run_dirs = (
        [layout.precompute_runs_dir / run_id]
        if run_id
        else sorted(path for path in layout.precompute_runs_dir.iterdir() if path.is_dir())
        if layout.precompute_runs_dir.exists()
        else []
    )
    for run_dir in run_dirs:
        manifest_path = run_dir / "run_manifest.json"
        if not manifest_path.exists():
            continue
        candidate_run_id = run_dir.name
        try:
            status = build_precompute_run_status(layout, run_id=candidate_run_id)
        except Exception:
            continue
        stage = str(status.get("stage") or "")
        if not str(status.get("status") or "").startswith("completed"):
            continue
        merged_dir = run_dir / "merged" / stage
        merge_manifest_path = merged_dir / "merge_manifest.json"
        if not merge_manifest_path.exists():
            continue
        if include_precompute_shards:
            shard_dir = run_dir / "shards" / stage
            bytes_used, file_count = _safe_tree_stats(shard_dir)
            if shard_dir.exists() and file_count > 0:
                candidates.append(
                    StoragePruneCandidate(
                        category="precompute_shards",
                        label=f"Precompute shards ({candidate_run_id})",
                        path=shard_dir,
                        bytes_used=bytes_used,
                        file_count=file_count,
                        run_id=candidate_run_id,
                    )
                )
        if include_precompute_merged:
            bytes_used, file_count = _safe_tree_stats(merged_dir)
            if merged_dir.exists() and file_count > 0:
                candidates.append(
                    StoragePruneCandidate(
                        category="precompute_merged",
                        label=f"Precompute merged ({candidate_run_id})",
                        path=merged_dir,
                        bytes_used=bytes_used,
                        file_count=file_count,
                        run_id=candidate_run_id,
                    )
                )
    total_bytes = sum(candidate.bytes_used for candidate in candidates)
    total_files = sum(candidate.file_count for candidate in candidates)
    return StoragePrunePlan(
        storage_root=layout.root,
        total_bytes=total_bytes,
        total_files=total_files,
        candidates=tuple(candidates),
    )


def render_storage_prune_plan(plan: StoragePrunePlan) -> list[str]:
    lines = [
        f"Storage root: {plan.storage_root}",
        f"Prunable total: {_format_bytes(plan.total_bytes)} across {plan.total_files:,} files",
    ]
    if not plan.candidates:
        lines.append("No safe prune targets found.")
        return lines
    for candidate in sorted(plan.candidates, key=lambda item: item.bytes_used, reverse=True):
        lines.append(
            f"{candidate.category:18} | {_format_bytes(candidate.bytes_used):>10} | "
            f"files={candidate.file_count:,} | {candidate.path}"
        )
    return lines


def prune_storage(
    layout: StorageLayout,
    *,
    include_precompute_shards: bool = True,
    include_precompute_merged: bool = True,
    run_id: str | None = None,
) -> StoragePruneResult:
    plan = build_storage_prune_plan(
        layout,
        include_precompute_shards=include_precompute_shards,
        include_precompute_merged=include_precompute_merged,
        run_id=run_id,
    )
    deleted_paths: list[Path] = []
    for candidate in plan.candidates:
        if candidate.path.exists():
            shutil.rmtree(candidate.path)
            deleted_paths.append(candidate.path)
    return StoragePruneResult(
        storage_root=layout.root,
        total_bytes=plan.total_bytes,
        total_files=plan.total_files,
        deleted_paths=tuple(deleted_paths),
        candidates=plan.candidates,
    )


def render_storage_prune_result(result: StoragePruneResult) -> list[str]:
    lines = [
        f"Storage root: {result.storage_root}",
        f"Deleted: {_format_bytes(result.total_bytes)} across {result.total_files:,} files",
    ]
    if not result.deleted_paths:
        lines.append("No storage paths were deleted.")
        return lines
    for path in result.deleted_paths:
        lines.append(f"Removed: {path}")
    return lines
