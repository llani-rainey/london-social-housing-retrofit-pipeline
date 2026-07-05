.PHONY: setup ingest dbt dbt-test dbt-docs test test-spark flow all clean

# ── First-time setup ──────────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt
	cd dbt && dbt deps --profiles-dir .
	pre-commit install

# ── Pipeline ──────────────────────────────────────────────────────────────────
ingest:
	python src/pipeline.py

flow:
	python src/prefect_flow.py

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	pytest tests/test_helpers.py tests/test_gold.py -v -m 'not spark'

test-spark:
	pytest tests/test_silver_epc.py tests/test_silver_core.py -v -m spark

# ── Full run ──────────────────────────────────────────────────────────────────
all: ingest dbt dbt-test test

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	rm -rf dbt/target dbt/dbt_packages
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
