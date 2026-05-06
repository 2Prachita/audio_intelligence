-- models/staging/stg_tracks.sql
-- ─────────────────────────────────────────────────────────────────────
-- Last.fm schema notes:
--   - duration_ms field actually contains SECONDS → multiply ×1000
--   - popularity = play count (can be millions, not 0–100)
--   - album / release_date NULL for chart endpoint
--   - artist_id is a UUID; artist_name_key is the join bridge to stg_artists

{{ config(materialized='view') }}

with raw as (

    select raw_data, run_date, loaded_at
    from {{ source('raw', 'tracks') }}

),

parsed as (

    select
        raw_data:track_id::varchar              as track_id,
        raw_data:primary_artist_id::varchar     as artist_id,
        raw_data:album_id::varchar              as album_id,
        raw_data:track_name::varchar            as track_name,
        raw_data:primary_artist_name::varchar   as artist_name,

        -- Name-based join key — matches stg_artists.artist_name_key
        lower(trim(raw_data:primary_artist_name::varchar)) as artist_name_key,

        raw_data:album_name::varchar            as album_name,
        raw_data:album_type::varchar            as album_type,
        raw_data:explicit::boolean              as is_explicit,
        raw_data:preview_url::varchar           as preview_url,
        raw_data:source::varchar                as source,
        raw_data:ingested_at::date              as ingested_at,
        run_date,
        loaded_at,

        -- Duration: Last.fm stores seconds, not milliseconds
        case
            when raw_data:source::varchar = 'lastfm'
            then coalesce(raw_data:duration_ms::integer, 0) * 1000
            else raw_data:duration_ms::integer
        end                                     as duration_ms,

        -- Play count (Last.fm) vs popularity score (Spotify 0–100)
        raw_data:popularity::integer            as play_count,
        case
            when raw_data:source::varchar = 'lastfm' then null
            else raw_data:popularity::integer
        end                                     as popularity_score,

        -- Release date: Spotify sends YYYY / YYYY-MM / YYYY-MM-DD
        case
            when length(raw_data:release_date::varchar) = 4
                then to_date(raw_data:release_date::varchar || '-01-01', 'YYYY-MM-DD')
            when length(raw_data:release_date::varchar) = 7
                then to_date(raw_data:release_date::varchar || '-01', 'YYYY-MM-DD')
            else try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD')
        end                                     as release_date,

        row_number() over (
            partition by raw_data:track_id::varchar
            order by loaded_at desc
        )                                       as row_num

    from raw
    where raw_data:track_id::varchar   is not null
      and raw_data:track_name::varchar is not null

),

final as (

    select
        track_id,
        artist_id,
        artist_name_key,       -- join bridge to stg_artists / dim_artists
        album_id,
        track_name,
        artist_name,
        album_name,
        album_type,
        release_date,
        duration_ms,
        round(duration_ms / 60000.0, 2)             as duration_minutes,
        is_explicit,
        play_count,
        popularity_score,

        -- Unified 0–100 proxy (log-normalised play count for Last.fm)
        coalesce(
            popularity_score,
            least(100, round(
                ln(greatest(coalesce(play_count, 1), 1) + 1)
                / ln(20000000) * 100, 0
            ))
        )                                            as popularity_normalised,

        preview_url,
        source,
        ingested_at,
        run_date

    from parsed
    where row_num = 1

)

select * from final