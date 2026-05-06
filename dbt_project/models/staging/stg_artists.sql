-- models/staging/stg_artists.sql
-- ─────────────────────────────────────────────────────────────────────
-- Last.fm artist_id format: "lfm:kanye west" (name-based, not UUID)
-- stg_tracks artist_id format: UUID "7441014f-..."
--
-- These two ID formats will NEVER join directly.
-- Solution: create a normalised join key from artist_name (lowercase trim)
-- Both staging models expose artist_name_key for downstream joins.

{{ config(materialized='view') }}

with raw as (

    select raw_data, run_date, loaded_at
    from {{ source('raw', 'artists') }}

),

parsed as (

    select
        raw_data:artist_id::varchar                 as artist_id_raw,
        raw_data:artist_name::varchar               as artist_name,

        -- Normalised join key: lowercase + trim
        -- stg_tracks will also expose this so dim_artists can join on it
        lower(trim(raw_data:artist_name::varchar))  as artist_name_key,

        -- genres is an array — convert to comma-separated string
        array_to_string(raw_data:genres, ', ')      as genres_str,
        raw_data:genres[0]::varchar                 as primary_genre,

        -- Last.fm popularity = listener count (not 0-100)
        raw_data:popularity::integer                as listener_count,
        raw_data:followers::integer                 as follower_count,
        raw_data:image_url::varchar                 as image_url,
        raw_data:source::varchar                    as source,
        raw_data:ingested_at::date                  as ingested_at,
        run_date,
        loaded_at,

        row_number() over (
            partition by lower(trim(raw_data:artist_name::varchar))
            order by loaded_at desc
        )                                           as row_num

    from raw
    where raw_data:artist_name::varchar is not null

),

final as (

    select
        artist_id_raw,
        artist_name,
        artist_name_key,
        genres_str,
        -- Last.fm genres array is often empty — default to 'Unknown'
        coalesce(nullif(primary_genre, ''), 'Unknown') as primary_genre,
        listener_count,
        follower_count,
        image_url,
        source,
        ingested_at,
        run_date

    from parsed
    where row_num = 1

)

select * from final