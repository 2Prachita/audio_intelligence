-- models/marts/fct_audio_features.sql
-- ────────────────────────────────────
-- Core fact table: one row per track per run_date.
-- Joins stg_tracks + stg_audio_features + dim_artists.
-- This is the table analytics queries run against.

{{ config(materialized='table') }}

with tracks as (

    select * from {{ ref('stg_tracks') }}

),

features as (

    select * from {{ ref('stg_audio_features') }}

),

artists as (

    -- Only current artist records (SCD-2)
    select * from {{ ref('dim_artists') }}
    where is_current = true

),

joined as (

    select
        -- ── Keys ──────────────────────────────────────────────────────
        t.track_id,
        t.artist_id,
        t.album_id,
        a.artist_surrogate_key,

        -- ── Track metadata ────────────────────────────────────────────
        t.track_name,
        t.artist_name,
        t.album_name,
        t.album_type,
        t.release_date,
        date_part('year', t.release_date)           as release_year,
        t.duration_ms,
        t.duration_minutes,
        t.is_explicit,
        t.popularity,
        t.preview_url,

        -- ── Artist dimensions ─────────────────────────────────────────
        a.genres_str,
        a.primary_genre,
        a.popularity_score                          as artist_popularity,
        a.follower_count,

        -- ── Audio features (may be NULL for Deezer source) ───────────
        f.danceability,
        f.energy,
        f.loudness_db,
        f.speechiness,
        f.acousticness,
        f.instrumentalness,
        f.liveness,
        f.valence,
        f.effective_tempo_bpm                       as tempo_bpm,
        f.musical_key,
        f.musical_mode,
        f.key_mode_label,
        f.time_signature,
        f.deezer_rank,

        -- ── Derived / computed columns ────────────────────────────────
        -- Mood score: combination of energy + valence (high = upbeat)
        case
            when f.energy is not null and f.valence is not null
            then round((f.energy + f.valence) / 2, 3)
            else null
        end                                         as mood_score,

        -- Track era bucket
        case
            when date_part('year', t.release_date) >= 2020 then 'Recent (2020+)'
            when date_part('year', t.release_date) >= 2010 then '2010s'
            when date_part('year', t.release_date) >= 2000 then '2000s'
            when date_part('year', t.release_date) >= 1990 then '90s'
            else 'Classic (pre-1990)'
        end                                         as era_bucket,

        -- Tempo category
        case
            when f.effective_tempo_bpm < 70  then 'Slow'
            when f.effective_tempo_bpm < 120 then 'Moderate'
            when f.effective_tempo_bpm < 160 then 'Fast'
            when f.effective_tempo_bpm >= 160 then 'Very Fast'
            else 'Unknown'
        end                                         as tempo_category,

        -- ── Lineage ───────────────────────────────────────────────────
        t.source,
        t.ingested_at,
        t.run_date,
        current_timestamp()                         as dbt_created_at

    from tracks t
    left join features f on t.track_id = f.track_id
    left join artists  a on t.artist_id = a.artist_id

)

select * from joined