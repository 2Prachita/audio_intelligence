# 🎵 Audio Intelligence Pipeline

> End-to-end data engineering portfolio project — ingests global music chart data from the Last.fm API, orchestrates transformations with Prefect, models with dbt, and lands analytics-ready data in Snowflake. Designed to mirror real production patterns at audio-AI companies like ElevenLabs, Deepgram, and Sarvam AI.

[![Python](https://img.shields.io/badge/Python-3.11+-blue)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.8.7-orange)](https://getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-cloud-29B5E8)](https://snowflake.com)
[![Prefect](https://img.shields.io/badge/Prefect-2.x-blue)](https://prefect.io)
[![Last.fm](https://img.shields.io/badge/Last.fm-API-D51007)](https://last.fm/api)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      AUDIO INTELLIGENCE PIPELINE                        │
│                                                                         │
│  ┌─────────────┐    ┌──────────────────┐    ┌────────────────────────┐  │
│  │  Last.fm    │    │  Python          │    │  Snowflake             │  │
│  │  Public API │───▶│  Ingestion Layer │───▶│  RAW Schema            │  │
│  │             │    │                  │    │                        │  │
│  │ • Global    │    │  lastfm_client   │    │  RAW.TRACKS            │  │
│  │   charts    │    │  lastfm_ingest   │    │  RAW.ARTISTS           │  │
│  │ • Genre     │    │  snowflake_      │    │  RAW.AUDIO_FEATURES    │  │
│  │   charts    │    │  loader          │    │  (VARIANT columns)     │  │
│  │ • Artist    │    │                  │    └───────────┬────────────┘  │
│  │   top tracks│    │  Fallback chain: │                │               │
│  └─────────────┘    │  chart→genre→    │    ┌───────────▼────────────┐  │
│                     │  artist→search   │    │  dbt Transformations   │  │
│  ┌─────────────┐    └──────┬───────────┘    │                        │  │
│  │  Local Raw  │           │                │  STAGING (views)       │  │
│  │  JSON Files │◀──────────┘                │  ├── stg_tracks        │  │
│  │             │                            │  ├── stg_artists       │  │
│  │  data/raw/  │                            │  └── stg_audio_features│  │
│  │  YYYY-MM-DD │                            │                        │  │
│  └─────────────┘                            │  MARTS (tables)        │  │
│                                             │  ├── dim_artists (SCD2)│  │
│  ┌─────────────┐                            │  ├── fct_audio_features│  │
│  │  Prefect    │                            │  └── mart_audio_trends │  │
│  │  2.x        │────────────────────────────│                        │  │
│  │             │                            └────────────────────────┘  │
│  │  Daily 6am  │                                                        │
│  │  IST cron   │                                                        │
│  └─────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## What This Project Demonstrates

| Pattern | Implementation |
|---|---|
| **Production API client** | Rate limiting, exponential back-off, retry logic, full error logging |
| **Fallback extraction strategy** | Chart → Genre → Artist → Search — never silently returns 0 records |
| **Geo-blocking problem solving** | Diagnosed Deezer blocking Indian IPs via curl; switched to Last.fm |
| **Medallion architecture** | Raw → Staging → Marts in Snowflake |
| **Dimensional modelling** | Star schema with fact + dimension tables |
| **SCD Type 2** | `dim_artists` tracks popularity/genre changes with valid_from/valid_to |
| **Multi-source schema design** | `artist_name_key` join bridge solves Last.fm's name-based vs UUID artist IDs |
| **dbt transformations** | 6 models, Jinja templating, `generate_schema_name` macro |
| **Data quality tests** | not_null, unique, accepted_range, referential integrity, accepted_values |
| **Idempotent loads** | Delete-then-insert by run_date — safe to re-run any day |
| **Prefect orchestration** | Task dependencies, retry logic, daily IST schedule |
| **Custom dbt macro** | `generate_schema_name` overrides default schema prefixing behaviour |

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Language | Python 3.11+ | |
| Source API | Last.fm Public API | Free, no geo-restrictions, no auth required for reads |
| Orchestration | Prefect 2.x | Local + cloud deployment support |
| Raw storage | Local filesystem | Mirrors S3 medallion pattern; swap path for S3 URI |
| Data warehouse | Snowflake | Free trial works; VARIANT columns for raw JSON |
| Transformation | dbt-core 1.8.7 + dbt-snowflake | Pinned for Python 3.13 compatibility |
| Data quality | dbt tests + dbt_utils 1.3.3 | |
| Package manager | uv | Fast Python package management |

---

## Project Structure

```
audio_intelligence/
├── ingestion/
│   ├── lastfm_client.py       # API client: auth, retry, rate limit, diagnose()
│   ├── lastfm_ingest.py       # Extraction: chart→genre→artist fallback chain
│   ├── snowflake_loader.py    # Idempotent load: raw JSON → Snowflake VARIANT
│   ├── deezer_client.py       # Original client (superseded — geo-blocked in India)
│   └── deezer_ingest.py       # Original ingest (superseded)
├── orchestration/
│   └── pipeline.py            # Prefect flow: extract→load→dbt staging→marts→test
├── dbt_project/
│   ├── macros/
│   │   └── generate_schema_name.sql   # Override dbt schema prefixing
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml            # RAW table declarations
│   │   │   ├── stg_tracks.sql         # Cleans tracks, converts duration s→ms
│   │   │   ├── stg_artists.sql        # Name-key join bridge for Last.fm IDs
│   │   │   └── stg_audio_features.sql # Normalises Last.fm field names
│   │   └── marts/
│   │       ├── dim_artists.sql        # SCD-2 with surrogate key hashing
│   │       ├── fct_audio_features.sql # Core fact table, name-key join
│   │       └── mart_audio_trends.sql  # Pre-aggregated analytics
│   ├── tests/
│   │   └── generic_tests.yml  # Quality tests for all 6 models
│   ├── dbt_project.yml        # Model materialisation config
│   ├── packages.yml           # dbt_utils dependency
│   └── package-lock.yml       # Locked package versions (commit this)
├── utils/
│   └── logger.py
├── data/                      # Auto-created at runtime (gitignored)
├── set_env.sh.example         # Env var template (copy → set_env.sh, gitignored)
├── .env.example               # Python env var template
├── .gitignore
├── Makefile
└── requirements.txt
```

---

## Quick Start

### 1. Clone and install
```bash
git clone https://github.com/2Prachita/audio_intelligence.git
cd audio_intelligence
pip install -r requirements.txt
```

### 2. Get a free Last.fm API key
Go to [last.fm/api/account/create](https://www.last.fm/api/account/create) — instant, no credit card.

### 3. Set environment variables
```bash
cp set_env.sh.example set_env.sh
# Edit set_env.sh with your values, then:
source set_env.sh
```

Required variables:
```bash
export LASTFM_API_KEY=your_key_here
export SNOWFLAKE_ACCOUNT=orgname-accountname    # NOT the full URL
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 4. Verify connectivity
```bash
make diagnose       # tests Last.fm API
make env-check      # verifies all env vars are set
```

### 5. Run the full pipeline
```bash
# All in one:
make pipeline

# Or step by step:
make extract          # Last.fm API → data/raw/YYYY-MM-DD/
make load             # raw JSON → Snowflake RAW schema
make dbt-deps         # install dbt_utils (once only)
make dbt-run          # build all 6 models
make dbt-test         # run data quality checks
```

### 6. Explore the data (Snowflake worksheet)
```sql
-- Top 20 artists by listener count
SELECT dimension_value AS artist, listener_count, track_count
FROM AUDIO_PIPELINE.MARTS.MART_AUDIO_TRENDS
WHERE agg_type = 'top_artists'
ORDER BY listener_count DESC NULLS LAST
LIMIT 20;

-- Most played tracks
SELECT track_name, artist_name, play_count, popularity_normalised
FROM AUDIO_PIPELINE.MARTS.FCT_AUDIO_FEATURES
ORDER BY play_count DESC NULLS LAST
LIMIT 20;

-- Genre breakdown
SELECT dimension_value AS genre, track_count, avg_popularity
FROM AUDIO_PIPELINE.MARTS.MART_AUDIO_TRENDS
WHERE agg_type = 'genre_profile'
ORDER BY track_count DESC;
```

### 7. Generate dbt docs (optional but impressive)
```bash
make dbt-docs
# Opens at http://localhost:8080 — screenshot the lineage DAG
```

---

## dbt Data Lineage

```
RAW.TRACKS ──────────┐
RAW.ARTISTS ─────────┼──▶ stg_tracks ──────┐
RAW.AUDIO_FEATURES ──┘    stg_artists ─────┼──▶ dim_artists (SCD-2, incremental)
                           stg_audio_feats ─┘──▶ fct_audio_features (table)
                                                  mart_audio_trends (table)
```

### Key design decisions

**SCD-2 on `dim_artists`** — when an artist's listener count or genre changes, the old record is closed and a new row is inserted. Supports time-travel queries:
```sql
SELECT listener_count FROM AUDIO_PIPELINE.MARTS.DIM_ARTISTS
WHERE artist_name_key = 'taylor swift'
  AND valid_from <= '2024-06-01'
  AND (valid_to > '2024-06-01' OR valid_to IS NULL);
```

**`artist_name_key` join bridge** — Last.fm's artist table uses name-based IDs (`"lfm:kanye west"`) while track records use MBIDs (UUIDs). A direct join would produce 100% NULLs. Both staging models expose `lower(trim(artist_name))` as a consistent join key.

**`generate_schema_name` macro** — dbt's default behaviour prefixes custom schemas with the profile's default schema (producing `staging_marts` instead of `MARTS`). The macro overrides this to use exact schema names.

**Idempotent loads** — every `load_raw()` call deletes rows for `run_date` before inserting. Safe to re-run any step at any time without duplicating data.

**`popularity_normalised`** — Last.fm's popularity is a play count (up to billions), Spotify's is 0–100. A log-scaled normalisation produces a unified 0–100 proxy that works in ORDER BY and visualisations regardless of source.

---

## Engineering Decisions & Lessons Learned

**Geo-blocking diagnosis** — Original design used Deezer API. After getting `{"data":[],"total":0}` on both WiFi and mobile hotspot, diagnosed with:
```bash
curl "https://api.deezer.com/chart/0/tracks?limit=5"
# → {"data":[],"total":0}
```
Deezer geo-blocks Indian IPs. Switched to Last.fm which has no regional restrictions. Lesson: never trust HTTP 200 alone — always validate payload shape.

**dbt + Python 3.13 incompatibility** — dbt-core 1.10.x crashes on Python 3.13 with an `AttributeError: __class_getitem__` in `mashumaro`. Pinned to `dbt-core==1.8.7` + `dbt-snowflake==1.8.3` which are stable. Documented in `requirements.txt`.

**Schema naming** — dbt prefixed schema names with the profile's default schema, creating `staging_staging` and `staging_marts`. Fixed with a `generate_schema_name` macro — standard production pattern for clean schema naming.

---

## What's Next

- [ ] Add Apache Airflow as alternative orchestrator
- [ ] Swap local raw storage for S3 (one-line path change in `ingest.py`)
- [ ] Add Spotify source for audio features (danceability, energy, valence)
- [ ] Build Streamlit dashboard on top of `mart_audio_trends`
- [ ] Add dbt snapshots for full historical tracking

---

## Author

**Prachita Kotangale** — Data Engineer  
[LinkedIn](https://linkedin.com/in/prachita-kotangale) · [GitHub](https://github.com/2Prachita)

*Stack: Python · Snowflake · Prefect · dbt · Terraform · Azure DevOps*