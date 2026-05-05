-- models/marts/mart_audio_trends.sql
-- ─────────────────────────────────────
-- Pre-aggregated analytics mart — answers business questions directly.
-- This is what you screenshot for your README and LinkedIn post.
--
-- Answers questions like:
--   → Which genres have the highest energy tracks?
--   → How has average danceability changed over decades?
--   → What's the BPM distribution by genre?
--   → Which artists dominate by follower count vs popularity score?

{{ config(materialized='table') }}

with fact as (

    select * from {{ ref('fct_audio_features') }}
    where run_date = (select max(run_date) from {{ ref('fct_audio_features') }})

),

-- ── 1. Genre-level audio profile ─────────────────────────────────────
genre_profile as (

    select
        coalesce(primary_genre, 'Unknown')          as genre,
        count(*)                                    as track_count,
        round(avg(danceability), 3)                 as avg_danceability,
        round(avg(energy), 3)                       as avg_energy,
        round(avg(valence), 3)                      as avg_valence,
        round(avg(tempo_bpm), 1)                    as avg_tempo_bpm,
        round(avg(popularity), 1)                   as avg_popularity,
        round(avg(mood_score), 3)                   as avg_mood_score,
        count(case when is_explicit then 1 end)     as explicit_count,
        round(
            100.0 * count(case when is_explicit then 1 end) / nullif(count(*), 0),
            1
        )                                           as explicit_pct

    from fact
    group by 1
    having count(*) >= 5  -- only genres with meaningful sample size

),

-- ── 2. Era-level trends ───────────────────────────────────────────────
era_trends as (

    select
        era_bucket,
        count(*)                                    as track_count,
        round(avg(danceability), 3)                 as avg_danceability,
        round(avg(energy), 3)                       as avg_energy,
        round(avg(valence), 3)                      as avg_valence,
        round(avg(tempo_bpm), 1)                    as avg_tempo_bpm,
        round(avg(acousticness), 3)                 as avg_acousticness,
        round(avg(popularity), 1)                   as avg_popularity

    from fact
    where era_bucket is not null
    group by 1

),

-- ── 3. Top artists by follower count (current) ───────────────────────
top_artists as (

    select
        artist_name,
        primary_genre,
        max(follower_count)                         as follower_count,
        max(artist_popularity)                      as artist_popularity,
        count(distinct track_id)                    as track_count_in_data,
        round(avg(popularity), 1)                   as avg_track_popularity,
        round(avg(danceability), 3)                 as avg_danceability,
        round(avg(energy), 3)                       as avg_energy

    from fact
    group by 1, 2
    having count(distinct track_id) >= 2

),

-- ── 4. Tempo distribution ─────────────────────────────────────────────
tempo_dist as (

    select
        tempo_category,
        primary_genre                               as genre,
        count(*)                                    as track_count,
        round(avg(popularity), 1)                   as avg_popularity,
        round(avg(danceability), 3)                 as avg_danceability

    from fact
    where tempo_category != 'Unknown'
    group by 1, 2

),

-- ── 5. Daily pipeline health summary ─────────────────────────────────
pipeline_health as (

    select
        run_date,
        source,
        count(*)                                    as total_tracks,
        count(distinct artist_id)                   as unique_artists,
        count(case when tempo_bpm is not null then 1 end) as tracks_with_features,
        round(
            100.0 * count(case when tempo_bpm is not null then 1 end)
            / nullif(count(*), 0), 1
        )                                           as feature_coverage_pct,
        round(avg(popularity), 1)                   as avg_track_popularity,
        min(release_date)                           as oldest_track_date,
        max(release_date)                           as newest_track_date

    from fact
    group by 1, 2

)

-- Final output: union all aggregations with a type label
-- This makes it easy to filter in BI tools

select 'genre_profile'   as agg_type, genre           as dimension_value,
        track_count, avg_danceability, avg_energy, avg_valence,
        avg_tempo_bpm, avg_popularity, avg_mood_score,
        null::varchar as artist_name, null::integer as follower_count,
        null::varchar as era_bucket, null::varchar as tempo_category,
        null::varchar as source, null::date as run_date
from genre_profile

union all

select 'era_trends'      as agg_type, era_bucket       as dimension_value,
        track_count, avg_danceability, avg_energy, avg_valence,
        avg_tempo_bpm, avg_popularity, null,
        null, null, era_bucket, null, null, null
from era_trends

union all

select 'top_artists'     as agg_type, artist_name      as dimension_value,
        track_count_in_data, avg_danceability, avg_energy, null,
        null, avg_track_popularity, null,
        artist_name, follower_count, null, null, null, null
from top_artists
order by follower_count desc nulls last
limit 50

union all

select 'tempo_dist'      as agg_type, tempo_category   as dimension_value,
        track_count, avg_danceability, null, null,
        null, avg_popularity, null,
        null, null, null, tempo_category, null, null
from tempo_dist