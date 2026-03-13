"""Run the pytest suite in smaller sequential shards.

This is primarily an operational workaround for environments where a single
long-running `pytest -q` invocation is less stable than several shorter ones.
"""

from __future__ import annotations

import argparse
import math
import subprocess
import sys
import time
from pathlib import Path


def _discover_test_files(test_root: Path) -> list[Path]:
    return sorted(path for path in test_root.glob("test_*.py") if path.is_file())


def _build_shards(files: list[Path], *, batch_size: int) -> list[list[Path]]:
    return [files[index:index + batch_size] for index in range(0, len(files), batch_size)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pytest in stable sequential shards.")
    parser.add_argument("--batch-size", type=int, default=8, help="Number of test files per shard.")
    parser.add_argument(
        "--pytest-args",
        nargs=argparse.REMAINDER,
        default=[],
        help="Additional arguments forwarded to pytest after '--'.",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive.")

    repo_root = Path(__file__).resolve().parents[1]
    test_root = repo_root / "tests"
    files = _discover_test_files(test_root)
    if not files:
        print("No test files discovered under tests/.")
        return 1

    shards = _build_shards(files, batch_size=args.batch_size)
    pytest_args = [arg for arg in args.pytest_args if arg != "--"]
    overall_start = time.perf_counter()

    print(
        f"Running {len(files)} test files in {len(shards)} shard(s) "
        f"(batch_size={args.batch_size})."
    )
    for shard_index, shard in enumerate(shards, start=1):
        rel_files = [path.as_posix() for path in shard]
        command = [sys.executable, "-m", "pytest", "-q", *rel_files, *pytest_args]
        shard_start = time.perf_counter()
        print(
            f"\n[{shard_index}/{len(shards)}] "
            f"{len(shard)} file(s), approx {math.ceil(len(files) / len(shards))} avg: "
            + ", ".join(rel_files)
        )
        completed = subprocess.run(command, cwd=repo_root)
        shard_elapsed = time.perf_counter() - shard_start
        print(
            f"[{shard_index}/{len(shards)}] exit_code={completed.returncode} "
            f"elapsed={shard_elapsed:.2f}s"
        )
        if completed.returncode != 0:
            total_elapsed = time.perf_counter() - overall_start
            print(f"Stopped after failing shard. Total elapsed={total_elapsed:.2f}s")
            return completed.returncode

    total_elapsed = time.perf_counter() - overall_start
    print(f"\nAll shards passed in {total_elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
