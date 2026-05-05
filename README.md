# Audio Intelligence Pipeline

> End-to-end data engineering pipeline ingesting audio metadata from the Deezer API,
> orchestrating transformations with Prefect, modelling with dbt, and landing analytics-ready
> data in Snowflake — all designed to mirror real production patterns at audio-AI companies.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AUDIO INTELLIGENCE PIPELINE                   │
│                                                                       │
│  ┌──────────────┐     ┌──────────────┐     ┌───────────────────────┐ │
│  │  Deezer API  │────▶│  Python      │────▶│  Snowflake            │ │
│  │              │     │  Ingestion   │     │  RAW Schema           │ │
│  │  • Charts    │     │              │     │                       │ │
│  │  • Genres    │     │  deezer_     │     │  RAW.TRACKS           │ │
│  │  • Playlists │     │  client.py   │     │  RAW.ARTISTS          │ │
│  │  • Search    │     │  deezer_     │     │  RAW.AUDIO_FEATURES   │ │
│  └──────────────┘     │  ingest.py   │     └───────────┬───────────┘ │
│                        └──────┬───────┘                 │             │
│                               │                         │             │
│                        ┌──────▼───────┐     ┌───────────▼───────────┐ │
│                        │  Local Raw   │     │  dbt Transformations  │ │
│                        │  JSON Files  │     │                       │ │
│                        │              │     │  STAGING (views)      │ │
│                        │  data/raw/   │     │  ├── stg_tracks       │ │
│                        │  YYYY-MM-DD/ │     │  ├── stg_artists      │ │
│                        └──────────────┘     │  └── stg_audio_feats  │ │
│                                             │                       │ │
│                        ┌──────────────┐     │  MARTS (tables)       │ │
│                        │  Prefect     │     │  ├── dim_artists(SCD2)│ │
│                        │  Orchestrat. │     │  ├── fct_audio_feats  │ │
│                        │              │     │  └── mart_audio_trends│ │
│                        │  Daily 6am   │     └───────────────────────┘ │
│                        │  IST schedule│                               │
│                        └──────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## What It Demonstrates

| Pattern | Implementation |
|---|---|
| **API client with retry logic** | Rate limit handling, exponential back-off, full error logging |
| **Fallback extraction strategy** | Chart → Genre → Playlist → Search (never returns 0 records) |
| **Medallion architecture** | Raw → Staging → Marts layers in Snowflake |
| **Dimensional modelling** | Star schema with fact + dimension tables |
| **SCD Type 2** | `dim_artists` tracks artist changes over time with valid_from/valid_to |
| **dbt transformations** | 6 models across staging + marts with Jinja templating |
| **Data quality tests** | Not-null, unique, range, referential integrity, accepted values |
| **Idempotent loads** | Delete-then-insert by run_date — safe to re-run any day |
| **Prefect orchestration** | Retry logic, task dependencies, daily schedule, local + cloud modes |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Extraction | Python 3.13, Requests |
| Source API | Deezer Public API (no auth required) |
| Orchestration | Prefect 2.x |
| Raw storage | Local filesystem (mirrors S3 medallion pattern) |
| Data warehouse | Snowflake (free trial works) |
| Transformation | dbt Core + dbt-snowflake |
| Data quality | dbt tests + dbt_utils |
| Config | python-dotenv |

---

## Project Structure

```
audio_pipeline/
├── ingestion/
│   ├── deezer_client.py      # API client: retry, rate limit, diagnosis
│   ├── deezer_ingest.py      # Extraction: chart→genre→playlist→search fallback
│   └── snowflake_loader.py   # Loads raw JSON → Snowflake VARIANT tables
├── orchestration/
│   └── pipeline.py           # Prefect flow: extract→load→dbt→test
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml           # RAW table declarations
│   │   │   ├── stg_tracks.sql        # Cleaned tracks view
│   │   │   ├── stg_artists.sql       # Cleaned artists view
│   │   │   └── stg_audio_features.sql
│   │   └── marts/
│   │       ├── dim_artists.sql       # SCD-2 artist dimension
│   │       ├── fct_audio_features.sql # Core fact table
│   │       └── mart_audio_trends.sql  # Pre-aggregated analytics
│   ├── tests/
│   │   └── generic_tests.yml  # Data quality test suite
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml
├── utils/
│   └── logger.py
├── .env.example
├── .gitignore
├── Makefile
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone and install
```bash
git clone https://github.com/YOUR_USERNAME/audio-intelligence-pipeline.git
cd audio-intelligence-pipeline
pip install -r requirements.txt
```

### 2. Set up credentials
```bash
cp .env.example .env
# Edit .env and fill in your Snowflake credentials
# No Spotify or Deezer credentials needed — Deezer is a free public API
```

### 3. Verify Deezer connectivity (do this first)
```bash
make diagnose
# Expected output:
#   chart_ok: True
#   chart_count: 100
#   sample_track: <some track title>
```

### 4. Run the full pipeline
```bash
# Option A: one command
make pipeline

# Option B: step by step
make extract        # Pull data from Deezer API → local raw/ JSON
make load           # Load raw JSON → Snowflake RAW tables
make dbt-deps       # Install dbt packages (run once)
make dbt-run        # Run all 6 dbt models
make dbt-test       # Run data quality tests
```

### 5. Schedule with Prefect (optional)
```bash
# Run immediately
make prefect-run

# Register daily schedule (6am IST)
make prefect-deploy
prefect worker start --pool default-agent-pool
```

---

## dbt Data Model

```
RAW.TRACKS ──────────┐
RAW.ARTISTS ─────────┤──▶ stg_tracks ──────┐
RAW.AUDIO_FEATURES ──┘    stg_artists ─────┤──▶ dim_artists (SCD-2)
                           stg_audio_feats ─┘──▶ fct_audio_features
                                                 mart_audio_trends
```

### Key design decisions

**`dim_artists` is SCD Type 2** — when an artist's popularity or genre list changes, the old record is closed (`valid_to = today`, `is_current = FALSE`) and a new record is inserted. This supports time-travel queries like:
```sql
-- What was this artist's follower count in March 2024?
SELECT follower_count
FROM dim_artists
WHERE artist_id = 'xyz'
  AND valid_from <= '2024-03-01'
  AND (valid_to > '2024-03-01' OR valid_to IS NULL);
```

**Deezer audio features are proxied** — Spotify has a dedicated audio analysis endpoint (tempo, danceability, energy etc). Deezer doesn't. The pipeline stores NULL for these fields from Deezer and derives proxies where possible (BPM, rank). The staging model and fact table handle NULLs gracefully.

**Idempotent loads** — every `load_raw()` call deletes existing rows for `run_date` before inserting. Safe to re-run any step at any time.

---

## Sample Analytics Queries

Run these in your Snowflake worksheet after the pipeline completes:

```sql
-- Top 10 genres by average energy
SELECT primary_genre, avg_energy, avg_danceability, track_count
FROM AUDIO_PIPELINE.MARTS.MART_AUDIO_TRENDS
WHERE agg_type = 'genre_profile'
ORDER BY avg_energy DESC
LIMIT 10;

-- How has music's danceability changed across decades?
SELECT era_bucket, avg_danceability, avg_tempo_bpm, track_count
FROM AUDIO_PIPELINE.MARTS.MART_AUDIO_TRENDS
WHERE agg_type = 'era_trends'
ORDER BY era_bucket;

-- Top 20 artists by follower count
SELECT dimension_value AS artist_name, follower_count, avg_energy
FROM AUDIO_PIPELINE.MARTS.MART_AUDIO_TRENDS
WHERE agg_type = 'top_artists'
ORDER BY follower_count DESC NULLS LAST
LIMIT 20;

-- Most popular tracks (fact table)
SELECT track_name, artist_name, primary_genre, popularity, mood_score
FROM AUDIO_PIPELINE.MARTS.FCT_AUDIO_FEATURES
ORDER BY popularity DESC
LIMIT 20;
```

---

## Troubleshooting

### Deezer returning 0 records
```bash
# Step 1: Run the diagnostic
make diagnose

# Step 2: Test raw connectivity from terminal
curl "https://api.deezer.com/chart/0/tracks?limit=5"

# Step 3: Check your network
# Deezer may be blocked on some corporate/college networks.
# Try on mobile hotspot. If that works → network firewall issue.

# Step 4: Enable DEBUG logging to see full API responses
# In deezer_ingest.py, change logging.INFO to logging.DEBUG
```

### Snowflake connection error
```bash
# Verify your account identifier format: abc12345.us-east-1
# Find it: Snowflake UI → bottom-left account menu → Account Identifier
# Common mistake: using the full URL instead of just the identifier
```

### dbt model errors
```bash
cd dbt_project
dbt debug --profiles-dir .     # verify Snowflake connection
dbt compile --profiles-dir .   # check SQL compiles without errors
dbt run --select stg_tracks    # run one model at a time to isolate errors
```

---

## What I Learned Building This

- **Deezer vs Spotify API** — Spotify's audio analysis endpoint (tempo, energy, danceability) has no equivalent in Deezer's free tier. Designing a schema that gracefully handles NULL features from one source while preserving the same downstream model structure taught me how real pipelines handle multi-source data contracts.

- **SCD-2 implementation in dbt** — The incremental materialisation with surrogate key hashing is the pattern used by most production data warehouses. Understanding *why* you need it (time-travel queries, historical accuracy) makes it stick.

- **Fallback extraction strategy** — Building a cascade (chart → genre → playlist → search) means the pipeline never silently returns 0 records. The `MIN_RECORDS_REQUIRED` guardrail forces loud failure rather than passing empty data downstream.

---

## Author

**Prachita Kotangale** — Data Engineer  
[LinkedIn](https://linkedin.com/in/prachita-kotangale) · [GitHub](https://github.com/YOUR_USERNAME)

*Stack: Python · Snowflake · Prefect · dbt · Terraform · Azure DevOps*