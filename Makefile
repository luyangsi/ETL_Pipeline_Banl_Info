.PHONY: help venv install run test lint format clean

PYTHON ?= python
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

help:
	@echo "Targets:"
	@echo "  make venv      - create venv"
	@echo "  make install   - install project (editable)"
	@echo "  make run       - run ETL pipeline"
	@echo "  make test      - run tests"
	@echo "  make clean     - remove outputs/logs/build artifacts"

venv:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -e .
	$(PIP) install pytest

run: install
	@mkdir -p outputs
	$(VENV)/bin/largest-banks-etl \
	  --url "https://en.wikipedia.org/wiki/List_of_largest_banks" \
	  --rates "data/exchange_rates.csv" \
	  --out-csv "outputs/largest_banks.csv" \
	  --db "outputs/largest_banks.db" \
	  --table "Largest_banks" \
	  --log "etl_project_log.txt"

test: install
	$(VENV)/bin/pytest -q

clean:
	rm -rf $(VENV) outputs *.db *.sqlite *.log etl_project_log.txt code_log.txt .pytest_cache .mypy_cache
	find . -type d -name "__pycache__" -prune -exec rm -rf {} \;
