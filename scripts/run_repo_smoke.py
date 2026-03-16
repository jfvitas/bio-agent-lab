from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SmokeCommand:
    label: str
    args: tuple[str, ...]
    artifact_paths: tuple[Path, ...] = ()


def find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / "AGENTS.md").exists() and (candidate / "apps" / "PbdataWinUI").exists():
            return candidate
    raise FileNotFoundError("Could not locate the repo root from the current working directory.")


def resolve_python(repo_root: Path) -> Path:
    local_python = repo_root / ".venv" / "Scripts" / "python.exe"
    if local_python.exists():
        return local_python
    return Path(sys.executable)


def resolve_dotnet(repo_root: Path) -> str:
    local_dotnet = repo_root / ".tools" / "dotnet" / "dotnet.exe"
    if local_dotnet.exists():
        return str(local_dotnet)
    return "dotnet"


def run_command(
    *,
    repo_root: Path,
    command: list[str],
    label: str,
    log_lines: list[str],
) -> None:
    log_lines.append(f"$ {' '.join(command)}")
    completed = subprocess.run(
        command,
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        env=dict(os.environ),
    )
    if completed.stdout.strip():
        log_lines.append(completed.stdout.rstrip())
    if completed.stderr.strip():
        log_lines.append(completed.stderr.rstrip())
    if completed.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {completed.returncode}.")


def ensure_artifacts(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Expected artifacts were not created: {', '.join(missing)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a lightweight operational smoke check for the current pbdata workspace.",
    )
    parser.add_argument(
        "--skip-winui-build",
        action="store_true",
        help="Skip the WinUI Release build verification step.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run the shortest check path: status, doctor, and recommendation export only.",
    )
    args = parser.parse_args(argv)

    repo_root = find_repo_root(Path.cwd())
    python_exe = resolve_python(repo_root)
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "repo_smoke_check.log"
    log_lines = [f"pbdata repo smoke check", f"Repo root: {repo_root}", f"Python: {python_exe}"]

    cli_prefix = [str(python_exe), "-m", "pbdata", "--storage-root", "."]
    commands = [
        SmokeCommand(
            label="Workspace status",
            args=tuple(cli_prefix + ["status"]),
        ),
        SmokeCommand(
            label="Workspace doctor",
            args=tuple(cli_prefix + ["doctor"]),
        ),
        SmokeCommand(
            label="Model recommendation export",
            args=tuple(cli_prefix + ["report-model-recommendation"]),
            artifact_paths=(
                repo_root / "data" / "reports" / "model_studio_recommendation.json",
                repo_root / "data" / "reports" / "model_studio_recommendation.md",
            ),
        ),
    ]

    if not args.quick:
        commands.append(
            SmokeCommand(
                label="Recommended model training",
                args=tuple(cli_prefix + ["train-recommended-model", "--execution-strategy", "safe_baseline"]),
                artifact_paths=(
                    repo_root / "data" / "catalog" / "stage_state" / "train-recommended-model.json",
                ),
            )
        )

    try:
        for command in commands:
            run_command(
                repo_root=repo_root,
                command=list(command.args),
                label=command.label,
                log_lines=log_lines,
            )
            ensure_artifacts(list(command.artifact_paths))

        if not args.skip_winui_build:
            run_command(
                repo_root=repo_root,
                command=[
                    resolve_dotnet(repo_root),
                    "build",
                    "apps\\PbdataWinUI\\PbdataWinUI.csproj",
                    "-c",
                    "Release",
                    "-p:Platform=x64",
                ],
                label="WinUI Release build",
                log_lines=log_lines,
            )

        log_lines.append("Smoke check completed successfully.")
        log_path.write_text("\n\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"Smoke check passed. Log: {log_path}")
        return 0
    except Exception as exc:
        log_lines.append(f"Smoke check failed: {exc}")
        log_path.write_text("\n\n".join(log_lines) + "\n", encoding="utf-8")
        print(f"Smoke check failed. Log: {log_path}", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
