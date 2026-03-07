# pbdata

Spec-driven repository for constructing, auditing, and versioning
protein binding datasets for machine learning.

## Quick start

1. Create the virtualenv:
   `python -m venv .venv`
2. Install the project and test dependencies:
   `.venv\Scripts\python.exe -m pip install -e .[dev]`
3. Create the repo scaffold:
   `.venv\Scripts\python.exe bootstrap_repo.py`
4. Validate the schema:
   `.venv\Scripts\python.exe scripts/validate_schema.py`
5. Run tests:
   `.venv\Scripts\python.exe -m pytest -q`
