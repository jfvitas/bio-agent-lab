# Claude Code repo instructions

## Immutable test outcome files

**NEVER modify stress test files or expected outcome files to fix test failures.**
The following files define biological ground truth and are read-only for all agents:

- `stress_test_panel.yaml`
- `stress_test_panel_B.yaml`
- `stress_test_panel_C.yaml`
- `expected_outcomes_table.md`
- `expected_outcomes_panel_B.md`
- `expected_outcomes_panel_C.md`

If a test fails against these files, fix the classification code, the test
logic/assertions, or document a known data-source limitation with `pytest.xfail`.
These files are authoritative and must not be weakened to make tests pass.

## General conventions

- Python 3.11+, Pydantic v2, Typer CLI, Tkinter GUI
- All records use frozen Pydantic models; provenance must include `ingested_at`
- Run tests with `.venv/Scripts/python.exe -m pytest` (Windows venv)
- Integration tests require network: `pytest -m integration`
- Unit tests run by default (integration excluded via pyproject.toml addopts)
- For biological logic, explain assumptions explicitly
