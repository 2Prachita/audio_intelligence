-- models/staging/stg_tracks.sql
-- ──────────────────────────────
-- Reads raw VARIANT JSON from RAW.TRACKS
-- Casts every field to its correct type
-- Adds basic cleaning + deduplication
-- This view is the "source of truth" for all downstream track models

{{ config(materialized='view') }}

with raw as (

    select
        raw_data,
        run_date,
        loaded_at

    from {{ source('raw', 'tracks') }}

),

parsed as (

    select
        -- IDs
        raw_data:track_id::varchar            as track_id,
        raw_data:primary_artist_id::varchar   as artist_id,
        raw_data:album_id::varchar            as album_id,

        -- Track fields
        raw_data:track_name::varchar          as track_name,
        raw_data:duration_ms::integer         as duration_ms,
        raw_data:explicit::boolean            as is_explicit,
        raw_data:popularity::integer          as popularity,
        raw_data:preview_url::varchar         as preview_url,

        -- Album fields
        raw_data:album_name::varchar          as album_name,
        raw_data:album_type::varchar          as album_type,

        -- Release date — Spotify gives YYYY, YYYY-MM, or YYYY-MM-DD
        -- We normalise to DATE by padding short formats
        case
            when length(raw_data:release_date::varchar) = 4
                then to_date(raw_data:release_date::varchar || '-01-01', 'YYYY-MM-DD')
            when length(raw_data:release_date::varchar) = 7
                then to_date(raw_data:release_date::varchar || '-01', 'YYYY-MM-DD')
            else
                try_to_date(raw_data:release_date::varchar, 'YYYY-MM-DD')
        end                                   as release_date,

        -- Artist name (denormalised for convenience)
        raw_data:primary_artist_name::varchar as artist_name,

        -- Data lineage
        raw_data:source::varchar              as source,
        raw_data:ingested_at::date            as ingested_at,
        run_date,
        loaded_at,

        -- Deduplication key — keep latest load per track
        row_number() over (
            partition by raw_data:track_id::varchar
            order by loaded_at desc
        )                                     as row_num

    from raw

    where raw_data:track_id::varchar is not null
      and raw_data:track_name::varchar is not null

),

final as (

    select
        track_id,
        artist_id,
        album_id,
        track_name,
        artist_name,
        album_name,
        album_type,
        release_date,
        duration_ms,
        round(duration_ms / 60000.0, 2)      as duration_minutes,
        is_explicit,
        popularity,
        preview_url,
        source,
        ingested_at,
        run_date

    from parsed
    where row_num = 1  -- deduplicate: one row per track_id

)

select * from final