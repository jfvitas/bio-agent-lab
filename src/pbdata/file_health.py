from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ValidatorFn = Callable[[dict[str, Any]], None] | None


@dataclass(frozen=True)
class JsonFileHealthSummary:
    total_count: int
    valid_count: int
    empty_count: int
    corrupt_count: int
    invalid_count: int
    sample_problem_files: list[str]
    generated_at: str | None = None
    cache_used: bool = False
    cache_stale: bool = False
    directory_signature: dict[str, int] | None = None

    @property
    def problem_count(self) -> int:
        return self.empty_count + self.corrupt_count + self.invalid_count

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["problem_count"] = self.problem_count
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "JsonFileHealthSummary":
        return cls(
            total_count=int(payload.get("total_count") or 0),
            valid_count=int(payload.get("valid_count") or 0),
            empty_count=int(payload.get("empty_count") or 0),
            corrupt_count=int(payload.get("corrupt_count") or 0),
            invalid_count=int(payload.get("invalid_count") or 0),
            sample_problem_files=[
                str(item) for item in (payload.get("sample_problem_files") or []) if str(item).strip()
            ],
            generated_at=str(payload.get("generated_at") or "") or None,
            cache_used=bool(payload.get("cache_used")),
            cache_stale=bool(payload.get("cache_stale")),
            directory_signature=payload.get("directory_signature")
            if isinstance(payload.get("directory_signature"), dict)
            else None,
        )


def _directory_signature(directory: Path) -> dict[str, int]:
    file_count = 0
    latest_file_mtime_ns = 0
    if directory.exists():
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if not entry.is_file() or not entry.name.endswith(".json"):
                        continue
                    file_count += 1
                    try:
                        latest_file_mtime_ns = max(latest_file_mtime_ns, entry.stat().st_mtime_ns)
                    except OSError:
                        continue
        except OSError:
            file_count = 0
            latest_file_mtime_ns = 0
    try:
        dir_stat = directory.stat() if directory.exists() else None
    except OSError:
        dir_stat = None
    return {
        "file_count": file_count,
        "dir_mtime_ns": int(getattr(dir_stat, "st_mtime_ns", int(dir_stat.st_mtime * 1_000_000_000))) if dir_stat else 0,
        "latest_file_mtime_ns": latest_file_mtime_ns,
    }


def _read_cached_health_summary(cache_path: Path) -> JsonFileHealthSummary | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return JsonFileHealthSummary.from_dict(payload)
    except Exception:
        return None


def write_health_summary_report(
    summary: JsonFileHealthSummary,
    *,
    json_path: Path,
    title: str,
    scan_target: Path,
    md_path: Path | None = None,
) -> tuple[Path, Path | None]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")
    if md_path is None:
        return json_path, None
    lines = [
        f"# {title}",
        "",
        f"- Scan target: {scan_target}",
        f"- Generated at: {summary.generated_at or 'unknown'}",
        f"- Cache used: {'yes' if summary.cache_used else 'no'}",
        f"- Cache stale: {'yes' if summary.cache_stale else 'no'}",
        f"- Total files: {summary.total_count:,}",
        f"- Valid files: {summary.valid_count:,}",
        f"- Problem files: {summary.problem_count:,}",
        f"- Empty files: {summary.empty_count:,}",
        f"- Corrupt files: {summary.corrupt_count:,}",
        f"- Schema-invalid files: {summary.invalid_count:,}",
        "",
        "## Problem examples",
    ]
    if summary.sample_problem_files:
        lines.extend(f"- {name}" for name in summary.sample_problem_files)
    else:
        lines.append("- none")
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def scan_json_directory(
    directory: Path,
    *,
    validator: ValidatorFn = None,
    limit_examples: int = 10,
) -> JsonFileHealthSummary:
    valid_count = 0
    empty_count = 0
    corrupt_count = 0
    invalid_count = 0
    sample_problem_files: list[str] = []
    total_count = 0

    if directory.exists():
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if not entry.is_file() or not entry.name.endswith(".json"):
                        continue
                    total_count += 1
                    path = Path(entry.path)
                    try:
                        if entry.stat().st_size == 0:
                            empty_count += 1
                            if len(sample_problem_files) < limit_examples:
                                sample_problem_files.append(entry.name)
                            continue
                    except OSError:
                        corrupt_count += 1
                        if len(sample_problem_files) < limit_examples:
                            sample_problem_files.append(entry.name)
                        continue
                    try:
                        raw = json.loads(path.read_text(encoding="utf-8"))
                    except Exception:
                        corrupt_count += 1
                        if len(sample_problem_files) < limit_examples:
                            sample_problem_files.append(entry.name)
                        continue
                    if validator is not None:
                        try:
                            validator(raw)
                        except Exception:
                            invalid_count += 1
                            if len(sample_problem_files) < limit_examples:
                                sample_problem_files.append(entry.name)
                            continue
                    valid_count += 1
        except OSError:
            total_count = 0

    return JsonFileHealthSummary(
        total_count=total_count,
        valid_count=valid_count,
        empty_count=empty_count,
        corrupt_count=corrupt_count,
        invalid_count=invalid_count,
        sample_problem_files=sample_problem_files,
        generated_at=datetime.now(timezone.utc).isoformat(),
        cache_used=False,
        directory_signature=_directory_signature(directory),
    )


def load_or_scan_json_directory(
    directory: Path,
    *,
    validator: ValidatorFn = None,
    cache_path: Path | None = None,
    limit_examples: int = 10,
    force_refresh: bool = False,
    prefer_cached: bool = False,
) -> JsonFileHealthSummary:
    signature = _directory_signature(directory)
    if cache_path is not None and not force_refresh:
        cached = _read_cached_health_summary(cache_path)
        if cached is not None:
            if cached.directory_signature == signature:
                return JsonFileHealthSummary(
                    total_count=cached.total_count,
                    valid_count=cached.valid_count,
                    empty_count=cached.empty_count,
                    corrupt_count=cached.corrupt_count,
                    invalid_count=cached.invalid_count,
                    sample_problem_files=list(cached.sample_problem_files),
                    generated_at=cached.generated_at,
                    cache_used=True,
                    cache_stale=False,
                    directory_signature=cached.directory_signature,
                )
            if prefer_cached:
                return JsonFileHealthSummary(
                    total_count=cached.total_count,
                    valid_count=cached.valid_count,
                    empty_count=cached.empty_count,
                    corrupt_count=cached.corrupt_count,
                    invalid_count=cached.invalid_count,
                    sample_problem_files=list(cached.sample_problem_files),
                    generated_at=cached.generated_at,
                    cache_used=True,
                    cache_stale=True,
                    directory_signature=cached.directory_signature,
                )

    summary = scan_json_directory(directory, validator=validator, limit_examples=limit_examples)
    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(summary.to_dict(), indent=2), encoding="utf-8")
    return summary


def remove_problem_json_files(
    directory: Path,
    *,
    validator: ValidatorFn = None,
) -> list[Path]:
    summary = scan_json_directory(directory, validator=validator, limit_examples=10_000)
    if summary.problem_count == 0:
        return []
    removed: list[Path] = []
    for path in sorted(directory.glob("*.json")):
        remove = False
        try:
            if path.stat().st_size == 0:
                remove = True
        except OSError:
            remove = True
        if not remove:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if validator is not None:
                    validator(raw)
            except Exception:
                remove = True
        if remove:
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)
    return path
