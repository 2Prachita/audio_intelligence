"""
snowflake_loader.py
────────────────────
Loads raw JSON files from data/raw/ into Snowflake RAW schema tables.
dbt then reads from these raw tables and transforms them.

Tables created (if not exist):
  RAW.TRACKS
  RAW.ARTISTS
  RAW.AUDIO_FEATURES

Uses VARIANT column + PARSE_JSON for schema flexibility (Snowflake best practice
for raw landing — keeps ingestion decoupled from schema changes).
"""

import json
import logging
import os
from datetime import date
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def _get_connection():
    """Return a Snowflake connection using env vars."""
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE", "AUDIO_PIPELINE"),
        schema    = "RAW",
        role      = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


DDL_STATEMENTS = """
CREATE DATABASE IF NOT EXISTS AUDIO_PIPELINE;

CREATE SCHEMA IF NOT EXISTS AUDIO_PIPELINE.RAW;
CREATE SCHEMA IF NOT EXISTS AUDIO_PIPELINE.STAGING;
CREATE SCHEMA IF NOT EXISTS AUDIO_PIPELINE.MARTS;

CREATE TABLE IF NOT EXISTS AUDIO_PIPELINE.RAW.TRACKS (
    raw_data    VARIANT,
    loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    run_date    DATE
);

CREATE TABLE IF NOT EXISTS AUDIO_PIPELINE.RAW.ARTISTS (
    raw_data    VARIANT,
    loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    run_date    DATE
);

CREATE TABLE IF NOT EXISTS AUDIO_PIPELINE.RAW.AUDIO_FEATURES (
    raw_data    VARIANT,
    loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    run_date    DATE
);

CREATE STAGE IF NOT EXISTS AUDIO_PIPELINE.RAW.RAW_JSON_STAGE;
"""


def setup_snowflake() -> None:
    """Create database, schemas, and raw tables if they don't exist."""
    logger.info("Setting up Snowflake objects...")
    conn = _get_connection()
    cur  = conn.cursor()
    try:
        for stmt in DDL_STATEMENTS.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        logger.info("Snowflake setup complete")
    finally:
        cur.close()
        conn.close()


def _load_json_file(filepath: Path) -> list[dict]:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def _insert_records(cur, table: str, records: list[dict], run_date: str) -> int:
    """
    Bulk insert records as VARIANT using executemany.
    Each record becomes a JSON string → parsed by Snowflake's PARSE_JSON.
    """
    if not records:
        logger.warning("No records to insert into %s", table)
        return 0

    sql = f"""
        INSERT INTO AUDIO_PIPELINE.RAW.{table} (raw_data, run_date)
        SELECT PARSE_JSON(%s), %s::DATE
    """
    data = [(json.dumps(r), run_date) for r in records]
    cur.executemany(sql, data)
    logger.info("Inserted %d records into RAW.%s", len(records), table)
    return len(records)


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _put_and_copy_json_array(cur, table: str, filepath: Path, run_date: str) -> int:
    """
    Upload local JSON array file to internal stage, then COPY into RAW table.
    JSON payload is expected as a top-level list of objects.
    """
    stage_base = "AUDIO_PIPELINE.RAW.RAW_JSON_STAGE"
    stage_path = f"{table.lower()}/{run_date}"
    file_uri = f"file://{filepath.resolve()}"
    escaped_run_date = _escape_sql_string(run_date)
    expected_name = f"{filepath.name}.gz"

    # PUT uploads local file and auto-compresses to .gz on stage.
    cur.execute(
        f"PUT '{file_uri}' @{stage_base}/{stage_path} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )

    temp_table = f"AUDIO_PIPELINE.RAW.TMP_{table}_LOAD"

    cur.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} (payload VARIANT)")

    # COPY INTO must remain a simple stage select.
    cur.execute(
        f"""
        COPY INTO {temp_table} (payload)
        FROM @{stage_base}/{stage_path}
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN = '.*{expected_name}'
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )

    # Flatten top-level JSON array into one RAW row per object.
    cur.execute(
        f"""
        INSERT INTO AUDIO_PIPELINE.RAW.{table} (raw_data, run_date)
        SELECT
            flattened.value::VARIANT,
            '{escaped_run_date}'::DATE
        FROM {temp_table} src,
             LATERAL FLATTEN(input => src.payload) flattened
        """
    )

    cur.execute(
        f"SELECT COUNT(*) FROM AUDIO_PIPELINE.RAW.{table} WHERE run_date = %s",
        (run_date,),
    )
    count = int(cur.fetchone()[0])
    logger.info("Loaded %d records into RAW.%s via COPY INTO", count, table)
    return count


def load_raw_records(
    tracks: list[dict],
    artists: list[dict],
    audio_features: list[dict],
    run_date: str | None = None,
) -> dict:
    """
    Direct-load in-memory extracted records into Snowflake RAW tables.
    Useful when skipping local JSON landing for faster iteration.
    """
    run_date = run_date or date.today().isoformat()
    payload_by_table = {
        "TRACKS": tracks,
        "ARTISTS": artists,
        "AUDIO_FEATURES": audio_features,
    }

    conn = _get_connection()
    cur = conn.cursor()
    summary: dict[str, int] = {}
    try:
        for table in payload_by_table:
            cur.execute(
                f"DELETE FROM AUDIO_PIPELINE.RAW.{table} WHERE run_date = %s",
                (run_date,),
            )
            logger.info("Cleared existing %s rows for %s", table, run_date)

        # Insert fresh data via stage + COPY INTO
        for table, filepath in files.items():
            count = _put_and_copy_json_array(cur, table, filepath, run_date)


        conn.commit()
        logger.info("Direct Snowflake load complete: %s", summary)
    except Exception as e:
        conn.rollback()
        logger.error("Direct Snowflake load failed, rolled back: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

    return summary


def load_raw(run_date: str = None) -> dict:
    """
    Load today's raw JSON files into Snowflake RAW tables.
    Idempotent: deletes existing rows for run_date before inserting.
    """
    run_date = run_date or date.today().isoformat()
    raw_base = Path("data/raw")

    # Find today's files
    files = {
        "TRACKS":         raw_base / "tracks"         / run_date / "tracks.json",
        "ARTISTS":        raw_base / "artists"        / run_date / "artists.json",
        "AUDIO_FEATURES": raw_base / "audio_features" / run_date / "audio_features.json",
    }

    missing = [k for k, v in files.items() if not v.exists()]
    if missing:
        raise FileNotFoundError(
            f"Raw files not found for {run_date}: {missing}. "
            "Run ingestion/spotify_ingest.py or ingestion/deezer_ingest.py first."
        )

    conn = _get_connection()
    cur  = conn.cursor()
    summary = {}

    try:
        # Idempotency: delete today's data before re-inserting
        for table in files:
            cur.execute(
                f"DELETE FROM AUDIO_PIPELINE.RAW.{table} WHERE run_date = %s",
                (run_date,)
            )
            logger.info("Cleared existing %s rows for %s", table, run_date)

        # Insert fresh data via stage + COPY INTO
        for table, filepath in files.items():
            count = _put_and_copy_json_array(cur, table, filepath, run_date)
            summary[table.lower()] = count

        conn.commit()
        logger.info("Snowflake load complete: %s", summary)

    except Exception as e:
        conn.rollback()
        logger.error("Snowflake load failed, rolled back: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    setup_snowflake()
    result = load_raw()
    print("\nLoaded to Snowflake:", result)