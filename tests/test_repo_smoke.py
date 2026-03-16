from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from uuid import uuid4


def _load_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_repo_smoke.py"
    spec = importlib.util.spec_from_file_location("run_repo_smoke", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_find_repo_root_detects_workspace() -> None:
    module = _load_module()
    repo_root = Path(__file__).resolve().parents[1]

    detected = module.find_repo_root(repo_root / "apps" / "PbdataWinUI")

    assert detected == repo_root


def test_resolve_python_prefers_current_interpreter_without_local_venv() -> None:
    module = _load_module()
    tmp_path = Path(__file__).resolve().parent / "_tmp" / f"repo_smoke_{uuid4().hex}"
    tmp_path.mkdir(parents=True, exist_ok=True)

    resolved = module.resolve_python(tmp_path)

    assert resolved == Path(sys.executable)
