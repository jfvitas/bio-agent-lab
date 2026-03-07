.PHONY: validate test format tree

PYTHON := .venv/Scripts/python.exe

validate:
	$(PYTHON) scripts/validate_schema.py

test:
	$(PYTHON) -m pytest -q

format:
	$(PYTHON) -m ruff check . --fix

tree:
	find . -maxdepth 3 | sort
