# Makefile
# ─────────
# Shortcuts for every step of the pipeline.
# Usage:  make <target>
# Requires: make (pre-installed on Mac/Linux; use Git Bash on Windows)

.PHONY: help install diagnose extract load dbt-deps dbt-run dbt-test dbt-all pipeline clean

# ── Default: show help ────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Audio Intelligence Pipeline — available commands"
	@echo "  ────────────────────────────────────────────────"
	@echo "  make install      Install all Python dependencies"
	@echo "  make diagnose     Test Deezer API connectivity"
	@echo "  make extract      Run Deezer extraction → raw JSON"
	@echo "  make load         Load raw JSON → Snowflake RAW tables"
	@echo "  make dbt-deps     Install dbt packages (run once)"
	@echo "  make dbt-run      Run all dbt models (staging + marts)"
	@echo "  make dbt-test     Run dbt data quality tests"
	@echo "  make dbt-all      dbt-deps + dbt-run + dbt-test"
	@echo "  make pipeline     Full run: extract + load + dbt-all"
	@echo "  make clean        Remove generated files"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

# ── Diagnose Deezer connectivity FIRST if getting 0 records ──────────
diagnose:
	python -m ingestion.deezer_ingest diagnose

# ── Extraction ────────────────────────────────────────────────────────
extract:
	python -m ingestion.deezer_ingest

# ── Load to Snowflake ─────────────────────────────────────────────────
load:
	python -m ingestion.snowflake_loader

# ── dbt ───────────────────────────────────────────────────────────────
dbt-deps:
	cd dbt_project && dbt deps --profiles-dir .

dbt-run:
	cd dbt_project && dbt run --profiles-dir .

dbt-test:
	cd dbt_project && dbt test --profiles-dir .

dbt-run-staging:
	cd dbt_project && dbt run --select staging --profiles-dir .

dbt-run-marts:
	cd dbt_project && dbt run --select marts --profiles-dir .

dbt-all: dbt-deps dbt-run dbt-test

# ── Full pipeline ─────────────────────────────────────────────────────
pipeline: extract load dbt-all
	@echo "Full pipeline complete"

# ── Prefect ───────────────────────────────────────────────────────────
prefect-run:
	python orchestration/pipeline.py

prefect-deploy:
	python orchestration/pipeline.py deploy

# ── Cleanup ───────────────────────────────────────────────────────────
clean:
	rm -rf data/raw dbt_project/target dbt_project/dbt_packages
	find . -type d -name __pycache__ -exec rm -rf {} +