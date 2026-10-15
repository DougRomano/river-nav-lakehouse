.PHONY: check test lint

# Runs pytest and ruff with the project venv. No `source .venv/bin/activate` needed.
check: test lint

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check src tests scripts
