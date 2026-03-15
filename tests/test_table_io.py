import json
import time
from pathlib import Path
from uuid import uuid4

from pbdata.table_io import clear_table_io_cache, load_json_rows, load_table_json

_LOCAL_TMP = Path(__file__).parent / "_tmp"
_LOCAL_TMP.mkdir(exist_ok=True)


def _tmp_dir(name: str) -> Path:
    path = _LOCAL_TMP / f"{uuid4().hex}_{name}"
    path.mkdir(exist_ok=True)
    return path


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_json_rows_uses_signature_cache(monkeypatch) -> None:
    clear_table_io_cache()
    path = _tmp_dir("json_rows_cache") / "row.json"
    _write_json(path, {"pdb_id": "1ABC"})

    read_count = 0
    original_read_text = Path.read_text

    def counting_read_text(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal read_count
        if self == path:
            read_count += 1
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read_text)

    assert load_json_rows(path) == [{"pdb_id": "1ABC"}]
    assert load_json_rows(path) == [{"pdb_id": "1ABC"}]
    assert read_count == 1


def test_load_table_json_invalidates_when_file_changes() -> None:
    clear_table_io_cache()
    table_dir = _tmp_dir("table_reload") / "entry"
    _write_json(table_dir / "1ABC.json", {"pdb_id": "1ABC"})

    assert load_table_json(table_dir) == [{"pdb_id": "1ABC"}]

    _write_json(table_dir / "1ABC.json", {"pdb_id": "2DEF"})
    time.sleep(0.02)
    path = table_dir / "1ABC.json"
    path.touch()

    assert load_table_json(table_dir) == [{"pdb_id": "2DEF"}]


def test_load_table_json_invalidates_when_directory_membership_changes() -> None:
    clear_table_io_cache()
    table_dir = _tmp_dir("table_membership") / "entry"
    _write_json(table_dir / "1ABC.json", {"pdb_id": "1ABC"})

    assert load_table_json(table_dir) == [{"pdb_id": "1ABC"}]

    _write_json(table_dir / "2DEF.json", {"pdb_id": "2DEF"})

    assert load_table_json(table_dir) == [{"pdb_id": "1ABC"}, {"pdb_id": "2DEF"}]
