-- models/marts/fct_audio_features.sql
-- ─────────────────────────────────────────────────────────────────────
-- Core fact table. One row per track.
-- Joins on artist_name_key (not artist_id) because Last.fm uses
-- name-based artist IDs in artists table vs UUIDs in tracks table.

{{ config(materialized='table') }}

with tracks as (
    select * from {{ ref('stg_tracks') }}
),

features as (
    select * from {{ ref('stg_audio_features') }}
),

artists as (
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
        date_part('year', t.release_date)       as release_year,
        t.duration_ms,
        t.duration_minutes,
        t.is_explicit,

        -- ── Popularity (source-aware) ─────────────────────────────────
        t.play_count,
        t.popularity_score,
        t.popularity_normalised,

        t.preview_url,

        -- ── Artist dimension (joined on name key) ─────────────────────
        a.genres_str,
        a.primary_genre,
        a.listener_count                        as artist_listener_count,
        a.follower_count,

        -- ── Audio features (all NULL for Last.fm) ─────────────────────
        f.danceability,
        f.energy,
        f.loudness_db,
        f.speechiness,
        f.acousticness,
        f.instrumentalness,
        f.liveness,
        f.valence,
        f.effective_tempo_bpm                   as tempo_bpm,
        f.musical_key,
        f.musical_mode,
        f.key_mode_label,
        f.time_signature,

        -- ── Derived ───────────────────────────────────────────────────
        case
            when f.energy is not null and f.valence is not null
            then round((f.energy + f.valence) / 2, 3)
            else null
        end                                     as mood_score,

        case
            when t.release_date is null          then 'Unknown Era'
            when date_part('year', t.release_date) >= 2020 then 'Recent (2020+)'
            when date_part('year', t.release_date) >= 2010 then '2010s'
            when date_part('year', t.release_date) >= 2000 then '2000s'
            when date_part('year', t.release_date) >= 1990 then '90s'
            else 'Classic (pre-1990)'
        end                                     as era_bucket,

        case
            when f.effective_tempo_bpm is null   then 'No Data'
            when f.effective_tempo_bpm < 70      then 'Slow'
            when f.effective_tempo_bpm < 120     then 'Moderate'
            when f.effective_tempo_bpm < 160     then 'Fast'
            else 'Very Fast'
        end                                     as tempo_category,

        -- ── Lineage ───────────────────────────────────────────────────
        t.source,
        t.ingested_at,
        t.run_date,
        current_timestamp()                     as dbt_created_at

    from tracks t
    left join features f on t.track_id = f.track_id
    -- Join on normalised name key, not artist_id
    left join artists  a on t.artist_name_key = a.artist_name_key

)

select * from joined