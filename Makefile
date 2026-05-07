# ─────────────────────────────────────────────────────────────────────
# Audio Intelligence Pipeline · Makefile
# Source: Last.fm API → Snowflake → dbt → Analytics
# ─────────────────────────────────────────────────────────────────────
.PHONY: help install env-check diagnose extract load \
        dbt-deps dbt-run-staging dbt-run-marts dbt-run dbt-test \
        dbt-docs dbt-all pipeline clean

# ── Default ──────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  🎵 Audio Intelligence Pipeline"
	@echo "  ─────────────────────────────────────────────────────"
	@echo "  make install          Install all Python dependencies"
	@echo "  make env-check        Verify all env vars are set"
	@echo "  make diagnose         Test Last.fm API connectivity"
	@echo "  make extract          Pull data from Last.fm → raw JSON"
	@echo "  make load             Load raw JSON → Snowflake RAW tables"
	@echo "  make dbt-deps         Install dbt packages (run once)"
	@echo "  make dbt-run-staging  Run staging views only"
	@echo "  make dbt-run-marts    Run mart tables only"
	@echo "  make dbt-run          Run ALL dbt models"
	@echo "  make dbt-test         Run data quality tests"
	@echo "  make dbt-docs         Generate + serve dbt docs (localhost:8080)"
	@echo "  make dbt-all          deps + run + test"
	@echo "  make pipeline         Full run: extract → load → dbt-all"
	@echo "  make clean            Remove generated files"
	@echo ""

# ── Setup ────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

env-check:
	@echo "Checking required environment variables..."
	@test -n "$$SNOWFLAKE_ACCOUNT"   || (echo "❌ SNOWFLAKE_ACCOUNT not set"   && exit 1)
	@test -n "$$SNOWFLAKE_USER"      || (echo "❌ SNOWFLAKE_USER not set"      && exit 1)
	@test -n "$$SNOWFLAKE_PASSWORD"  || (echo "❌ SNOWFLAKE_PASSWORD not set"  && exit 1)
	@test -n "$$SNOWFLAKE_WAREHOUSE" || (echo "❌ SNOWFLAKE_WAREHOUSE not set" && exit 1)
	@test -n "$$SNOWFLAKE_ROLE"      || (echo "❌ SNOWFLAKE_ROLE not set"      && exit 1)
	@test -n "$$LASTFM_API_KEY"      || (echo "❌ LASTFM_API_KEY not set"      && exit 1)
	@echo "All environment variables set"

# ── Diagnose ─────────────────────────────────────────────────────────
diagnose:
	@echo "Testing Last.fm API connectivity..."
	python -c "from ingestion.lastfm_client import LastFMClient; c = LastFMClient(); r = c.diagnose(); print(r)"

# ── Extraction ───────────────────────────────────────────────────────
extract:
	@echo "Extracting from Last.fm API..."
	python -m ingestion.lastfm_ingest

# ── Load to Snowflake ─────────────────────────────────────────────────
load:
	@echo "Loading raw JSON → Snowflake RAW tables..."
	python -m ingestion.snowflake_loader

# ── dbt ──────────────────────────────────────────────────────────────
DBT_FLAGS = --profiles-dir . --project-dir dbt_project

dbt-deps:
	dbt deps $(DBT_FLAGS)

dbt-run-staging:
	dbt run --select staging $(DBT_FLAGS)

dbt-run-marts:
	dbt run --select marts $(DBT_FLAGS)

dbt-run:
	dbt run $(DBT_FLAGS)

dbt-test:
	dbt test $(DBT_FLAGS)

dbt-docs:
	dbt docs generate $(DBT_FLAGS)
	dbt docs serve $(DBT_FLAGS)

dbt-compile:
	dbt compile $(DBT_FLAGS)

dbt-debug:
	dbt debug $(DBT_FLAGS)

dbt-all: dbt-deps dbt-run dbt-test

# ── Prefect ───────────────────────────────────────────────────────────
prefect-run:
	python orchestration/pipeline.py

prefect-deploy:
	python orchestration/pipeline.py deploy

# ── Full pipeline ─────────────────────────────────────────────────────
pipeline: env-check extract load dbt-all
	@echo ""
	@echo "Full pipeline complete — $(shell date)"

# ── Clean ─────────────────────────────────────────────────────────────
clean:
	rm -rf data/raw dbt_project/target dbt_project/dbt_packages dbt_project/logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned generated files"