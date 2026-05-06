-- models/marts/mart_audio_trends.sql
-- ─────────────────────────────────────────────────────────────────────
-- Pre-aggregated analytics mart.
-- Uses play_count and popularity_normalised for Last.fm compatibility.
-- Audio feature aggregations only run where values are not null.

{{ config(materialized='table') }}

with fact as (

    select * from {{ ref('fct_audio_features') }}
    where run_date = (select max(run_date) from {{ ref('fct_audio_features') }})

),

-- ── 1. Artist leaderboard (most meaningful for Last.fm data) ─────────
top_artists as (

    select
        'top_artists'                               as agg_type,
        coalesce(artist_name, 'Unknown')            as dimension_value,
        count(distinct track_id)                    as track_count,
        max(play_count)                             as total_plays,
        max(follower_count)                         as follower_count,
        max(artist_listener_count)                  as listener_count,
        round(avg(popularity_normalised), 1)        as avg_popularity,
        round(avg(danceability), 3)                 as avg_danceability,
        round(avg(energy), 3)                       as avg_energy,
        round(avg(valence), 3)                      as avg_valence,
        null::float                                 as avg_tempo_bpm,
        null::float                                 as avg_mood_score,
        null::varchar                               as era_bucket,
        null::varchar                               as tempo_category

    from fact
    group by artist_name
    having count(distinct track_id) >= 1
    order by max(play_count) desc nulls last
    limit 100

),

-- ── 2. Genre profile ─────────────────────────────────────────────────
genre_profile as (

    select
        'genre_profile'                             as agg_type,
        coalesce(primary_genre, 'Unknown')          as dimension_value,
        count(*)                                    as track_count,
        null::integer                               as total_plays,
        null::integer                               as follower_count,
        null::integer                               as listener_count,
        round(avg(popularity_normalised), 1)        as avg_popularity,
        round(avg(danceability), 3)                 as avg_danceability,
        round(avg(energy), 3)                       as avg_energy,
        round(avg(valence), 3)                      as avg_valence,
        round(avg(tempo_bpm), 1)                    as avg_tempo_bpm,
        round(avg(mood_score), 3)                   as avg_mood_score,
        null::varchar                               as era_bucket,
        null::varchar                               as tempo_category

    from fact
    group by primary_genre
    having count(*) >= 3

),

-- ── 3. Era trends ────────────────────────────────────────────────────
era_trends as (

    select
        'era_trends'                                as agg_type,
        era_bucket                                  as dimension_value,
        count(*)                                    as track_count,
        null::integer                               as total_plays,
        null::integer                               as follower_count,
        null::integer                               as listener_count,
        round(avg(popularity_normalised), 1)        as avg_popularity,
        round(avg(danceability), 3)                 as avg_danceability,
        round(avg(energy), 3)                       as avg_energy,
        round(avg(valence), 3)                      as avg_valence,
        round(avg(tempo_bpm), 1)                    as avg_tempo_bpm,
        round(avg(mood_score), 3)                   as avg_mood_score,
        era_bucket,
        null::varchar                               as tempo_category

    from fact
    where era_bucket is not null
    group by era_bucket

),

-- ── 4. Pipeline health ───────────────────────────────────────────────
pipeline_health as (

    select
        'pipeline_health'                           as agg_type,
        source                                      as dimension_value,
        count(*)                                    as track_count,
        sum(play_count)                             as total_plays,
        null::integer                               as follower_count,
        null::integer                               as listener_count,
        round(avg(popularity_normalised), 1)        as avg_popularity,
        null::float                                 as avg_danceability,
        null::float                                 as avg_energy,
        null::float                                 as avg_valence,
        null::float                                 as avg_tempo_bpm,
        null::float                                 as avg_mood_score,
        null::varchar                               as era_bucket,
        null::varchar                               as tempo_category

    from fact
    group by source

)

select * from top_artists
union all
select * from genre_profile
union all
select * from era_trends
union all
select * from pipeline_health