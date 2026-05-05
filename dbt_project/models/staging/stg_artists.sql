-- models/staging/stg_artists.sql
-- ─────────────────────────────────
-- Cleans raw VARIANT artist data
-- Explodes genres array into a comma-separated string (Snowflake-friendly)
-- Deduplicates by artist_id

{{ config(materialized='view') }}

with raw as (

    select
        raw_data,
        run_date,
        loaded_at

    from {{ source('raw', 'artists') }}

),

parsed as (

    select
        raw_data:artist_id::varchar           as artist_id,
        raw_data:artist_name::varchar         as artist_name,

        -- Snowflake ARRAY_TO_STRING on VARIANT array
        array_to_string(raw_data:genres, ', ')  as genres_str,

        -- Extract first genre as primary_genre (useful for grouping)
        raw_data:genres[0]::varchar           as primary_genre,

        raw_data:popularity::integer          as popularity_score,
        raw_data:followers::integer           as follower_count,
        raw_data:image_url::varchar           as image_url,
        raw_data:source::varchar              as source,
        raw_data:ingested_at::date            as ingested_at,
        run_date,
        loaded_at,

        row_number() over (
            partition by raw_data:artist_id::varchar
            order by loaded_at desc
        )                                     as row_num

    from raw

    where raw_data:artist_id::varchar is not null
      and raw_data:artist_name::varchar is not null

),

final as (

    select
        artist_id,
        artist_name,
        genres_str,
        primary_genre,
        popularity_score,
        follower_count,
        image_url,
        source,
        ingested_at,
        run_date

    from parsed
    where row_num = 1

)

select * from final