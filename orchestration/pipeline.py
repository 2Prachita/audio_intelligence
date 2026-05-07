"""
pipeline.py
────────────
Prefect flow that orchestrates the full pipeline daily:

  [Extract from Spotify API]
        ↓
  [Load raw JSON → Snowflake RAW tables]
        ↓
  [Run dbt: staging → marts → tests]
        ↓
  [Alert on success / failure]

Run locally:     python -m orchestration.pipeline
Deploy schedule: prefect deploy
"""

import subprocess
import sys
import os
from datetime import date
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# ── Root dir so imports work regardless of where you run from ─────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from ingestion.spotify_ingest import extract_all as extract_all_spotify
from ingestion.deezer_ingest import extract_all as extract_all_deezer
from ingestion.lastfm_ingest import extract_all as extract_all_lastfm
from ingestion.snowflake_loader import setup_snowflake, load_raw

SOURCE_PROVIDER = os.getenv("SOURCE_PROVIDER", "lastfm").strip().lower()
VALID_SOURCE_PROVIDERS = {"deezer", "spotify", "lastfm"}


# ═══════════════════════════════════════════════════════════════════════
# TASKS
# Each @task is independently retried, logged, and observable in Prefect UI
# ═══════════════════════════════════════════════════════════════════════

@task(
    name="setup-snowflake-objects",
    retries=2,
    retry_delay_seconds=10,
    description="Creates database, schemas, and raw tables if they don't exist."
)
def task_setup_snowflake():
    logger = get_run_logger()
    logger.info("Setting up Snowflake objects...")
    setup_snowflake()
    logger.info("Snowflake setup complete ✓")


@task(
    name="extract-from-source",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,        # skip re-extraction if same inputs
    cache_expiration=timedelta(hours=20), # but refresh daily
    description="Pulls tracks, artists, audio features from configured source → local raw JSON."
)
def task_extract() -> dict:
    logger = get_run_logger()
    if SOURCE_PROVIDER not in VALID_SOURCE_PROVIDERS:
        raise ValueError(
            f"Unsupported SOURCE_PROVIDER='{SOURCE_PROVIDER}'. "
            f"Expected one of: {sorted(VALID_SOURCE_PROVIDERS)}"
        )

    logger.info("Starting source extraction (%s) — %s", SOURCE_PROVIDER, date.today())
    if SOURCE_PROVIDER == "spotify":
        summary = extract_all_spotify()
    elif SOURCE_PROVIDER == "lastfm":
        summary = extract_all_lastfm()
    else:
        summary = extract_all_deezer()
    logger.info("Extraction complete: %s", summary)
    return summary


@task(
    name="load-raw-to-snowflake",
    retries=2,
    retry_delay_seconds=15,
    description="Loads today's raw JSON files into Snowflake RAW schema (idempotent)."
)
def task_load_raw(extraction_summary: dict) -> dict:
    logger = get_run_logger()
    logger.info("Loading raw data to Snowflake for %s", extraction_summary.get("run_date"))
    load_summary = load_raw(run_date=extraction_summary.get("run_date"))
    logger.info("Snowflake load complete: %s", load_summary)
    return load_summary


@task(
    name="run-dbt-staging",
    retries=1,
    retry_delay_seconds=20,
    description="Runs dbt staging models: stg_tracks, stg_artists, stg_audio_features."
)
def task_dbt_staging() -> None:
    logger = get_run_logger()
    logger.info("Running dbt staging models...")
    _run_dbt(["run", "--select", "staging"])
    logger.info("dbt staging complete ✓")


@task(
    name="run-dbt-marts",
    retries=1,
    retry_delay_seconds=20,
    description="Runs dbt mart models: dim_artists (SCD-2), fct_audio_features, mart_audio_trends."
)
def task_dbt_marts() -> None:
    logger = get_run_logger()
    logger.info("Running dbt mart models...")
    _run_dbt(["run", "--select", "marts"])
    logger.info("dbt marts complete ✓")


@task(
    name="run-dbt-tests",
    retries=0,   # tests failing = real data quality issue, don't mask it
    description="Runs dbt data quality tests — fails pipeline if data is bad."
)
def task_dbt_tests() -> None:
    logger = get_run_logger()
    logger.info("Running dbt data quality tests...")
    _run_dbt(["test"])
    logger.info("All dbt tests passed ✓")


# ─── dbt helper ──────────────────────────────────────────────────────

def _run_dbt(args: list[str]) -> None:
    """
    Runs dbt commands via subprocess from the dbt_project/ directory.
    Raises RuntimeError if dbt exits non-zero.
    """
    dbt_dir = ROOT / "dbt_project"
    cmd = ["dbt"] + args + ["--project-dir", str(dbt_dir),
                             "--profiles-dir", str(dbt_dir)]
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Always print dbt output so it appears in Prefect logs
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(args)}\n{result.stderr}")


# ═══════════════════════════════════════════════════════════════════════
# MAIN FLOW
# ═══════════════════════════════════════════════════════════════════════

@flow(
    name="audio-intelligence-pipeline",
    description="Daily source → Snowflake → dbt pipeline for audio trend analytics.",
    log_prints=True,
)
def audio_pipeline_flow():
    """
    Full daily pipeline.
    Task dependency graph:

      setup_snowflake
           ↓
        extract  ──────────────────────────────────────┐
           ↓                                           │
       load_raw                                      (summary passed downstream)
           ↓
      dbt_staging
           ↓
       dbt_marts
           ↓
       dbt_tests
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Audio Intelligence Pipeline — %s", date.today())
    logger.info("=" * 60)

    # Step 1: Ensure Snowflake objects exist
    task_setup_snowflake()

    # Step 2: Extract from configured source
    extraction_summary = task_extract()

    # Step 3: Load to Snowflake (waits for extract)
    task_load_raw(extraction_summary)

    # Step 4: dbt staging (waits for load)
    task_dbt_staging()

    # Step 5: dbt marts (waits for staging)
    task_dbt_marts()

    # Step 6: dbt tests (waits for marts)
    task_dbt_tests()

    logger.info("Pipeline complete — %s", date.today())
    return {"status": "success", "run_date": date.today().isoformat()}


# ─── Deployment helper (run once to register schedule) ───────────────

def deploy():
    """
    deploy the flow to the prefect cloud
    """
    from prefect import flow


if __name__ == "__main__":
    audio_pipeline_flow.serve(name="audio-intelligence-pipeline",
        tags=["onboarding"],
        cron="0 6 * * *"
    )
    print("Deployment registered — runs daily at 6am IST")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        deploy()
    else:
        # Run immediately (local test)
        audio_pipeline_flow()