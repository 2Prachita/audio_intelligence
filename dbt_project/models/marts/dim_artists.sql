-- models/marts/dim_artists.sql
-- ──────────────────────────────
-- Slowly Changing Dimension Type 2 (SCD-2) for artists.
--
-- What is SCD-2?
-- When an artist's popularity or genre changes over time, we don't overwrite
-- the old row. Instead we:
--   1. Close the old row (set valid_to = today, is_current = FALSE)
--   2. Insert a new row (valid_from = today, valid_to = NULL, is_current = TRUE)
--
-- This lets us answer: "What was this artist's popularity in March 2024?"
-- That kind of time-travel query is what separates a senior DE from a junior one.
--
-- This runs as INCREMENTAL — dbt only processes new/changed records.

{{
    config(
        materialized = 'incremental',
        unique_key   = 'artist_surrogate_key',
        on_schema_change = 'append_new_columns'
    )
}}

with source as (

    select * from {{ ref('stg_artists') }}

),

-- Build a surrogate key: hash of artist_id + the fields we track for changes
-- If any of these fields change → new SCD-2 row is created
with_change_key as (

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
        run_date,

        -- Surrogate key: hash of business key + tracked attributes
        md5(
            coalesce(artist_id, '') || '|' ||
            coalesce(artist_name, '') || '|' ||
            coalesce(genres_str, '') || '|' ||
            coalesce(cast(popularity_score as varchar), '') || '|' ||
            coalesce(cast(follower_count as varchar), '')
        )                               as artist_surrogate_key,

        -- Business key hash (for detecting which artist changed)
        md5(coalesce(artist_id, ''))    as artist_natural_key

    from source

),

{% if is_incremental() %}
-- On incremental runs: only process artists that are new OR have changed
-- "Changed" = their current surrogate_key doesn't match what's in the table
changed as (

    select wck.*
    from with_change_key wck
    left join {{ this }} existing
        on wck.artist_natural_key = existing.artist_natural_key
        and existing.is_current = true
    where
        existing.artist_natural_key is null              -- new artist
        or wck.artist_surrogate_key != existing.artist_surrogate_key  -- changed artist

),
{% endif %}

final as (

    select
        artist_surrogate_key,
        artist_natural_key,
        artist_id,
        artist_name,
        genres_str,
        primary_genre,
        popularity_score,
        follower_count,
        image_url,
        source,

        -- SCD-2 validity window
        run_date                        as valid_from,
        cast(null as date)              as valid_to,       -- NULL = still current
        true                            as is_current,

        current_timestamp()             as dbt_updated_at

    {% if is_incremental() %}
    from changed
    {% else %}
    from with_change_key
    {% endif %}

)

select * from final

-- ── How to query this table ───────────────────────────────────────────
-- Current state of all artists:
--   SELECT * FROM dim_artists WHERE is_current = TRUE
--
-- Artist history (all versions of one artist):
--   SELECT * FROM dim_artists WHERE artist_id = 'xyz' ORDER BY valid_from
--
-- What was an artist's follower count in a specific month?
--   SELECT follower_count FROM dim_artists
--   WHERE artist_id = 'xyz'
--     AND valid_from <= '2024-03-01'
--     AND (valid_to > '2024-03-01' OR valid_to IS NULL)