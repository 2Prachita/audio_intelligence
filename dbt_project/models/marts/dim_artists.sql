-- models/marts/dim_artists.sql
-- SCD Type 2 artist dimension.
-- Join key: artist_name_key (lowercase name) — because Last.fm uses
-- name-based IDs ("lfm:kanye west") while tracks use UUIDs.

{{
    config(
        materialized='incremental',
        unique_key='artist_surrogate_key',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ ref('stg_artists') }}
),

with_keys as (

    select
        artist_id_raw,
        artist_name,
        artist_name_key,
        genres_str,
        primary_genre,
        listener_count,
        follower_count,
        image_url,
        source,
        ingested_at,
        run_date,

        md5(
            coalesce(artist_name_key, '') || '|' ||
            coalesce(genres_str, '') || '|' ||
            coalesce(cast(listener_count as varchar), '') || '|' ||
            coalesce(cast(follower_count as varchar), '')
        )                                   as artist_surrogate_key,

        md5(coalesce(artist_name_key, '')) as artist_natural_key

    from source

),

{% if is_incremental() %}

changed as (
    select wk.*
    from with_keys wk
    left join {{ this }} ex
        on wk.artist_natural_key = ex.artist_natural_key
        and ex.is_current = true
    where ex.artist_natural_key is null
       or wk.artist_surrogate_key != ex.artist_surrogate_key
),

{% endif %}

final as (

    select
        artist_surrogate_key,
        artist_natural_key,
        artist_id_raw,
        artist_name,
        artist_name_key,
        genres_str,
        primary_genre,
        listener_count,
        follower_count,
        image_url,
        source,
        run_date                            as valid_from,
        cast(null as date)                  as valid_to,
        true                                as is_current,
        current_timestamp()                 as dbt_updated_at

    {% if is_incremental() %}
    from changed
    {% else %}
    from with_keys
    {% endif %}

)

select * from final