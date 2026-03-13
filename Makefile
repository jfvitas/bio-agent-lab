.PHONY: validate test test-sharded format tree

PYTHON := .venv/Scripts/python.exe

validate:
	$(PYTHON) scripts/validate_schema.py

test:
	$(PYTHON) -m pytest -q

test-sharded:
	$(PYTHON) scripts/run_test_shards.py

format:
	$(PYTHON) -m ruff check . --fix

tree:
	find . -maxdepth 3 | sort
