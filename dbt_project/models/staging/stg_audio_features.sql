-- models/staging/stg_audio_features.sql
-- ─────────────────────────────────────────────────────────────────────
-- Last.fm raw field names differ from Spotify:
--   raw: loudness      → renamed: loudness_db
--   raw: key           → renamed: musical_key
--   raw: mode          → renamed: musical_mode
--   raw: duration_ms   → actually seconds for Last.fm, multiply ×1000
--   raw: tempo         → renamed: tempo_bpm
-- All numeric features are NULL for Last.fm source — handled gracefully.

{{ config(materialized='view') }}

with raw as (

    select raw_data, run_date, loaded_at
    from {{ source('raw', 'audio_features') }}

),

parsed as (

    select
        raw_data:track_id::varchar          as track_id,

        -- Spotify audio features (all NULL for Last.fm)
        raw_data:danceability::float        as danceability,
        raw_data:energy::float              as energy,
        raw_data:loudness::float            as loudness_db,       -- raw field: "loudness"
        raw_data:speechiness::float         as speechiness,
        raw_data:acousticness::float        as acousticness,
        raw_data:instrumentalness::float    as instrumentalness,
        raw_data:liveness::float            as liveness,
        raw_data:valence::float             as valence,
        raw_data:tempo::float               as tempo_bpm,         -- raw field: "tempo"
        raw_data:key::integer               as musical_key,       -- raw field: "key"
        raw_data:mode::integer              as musical_mode,      -- raw field: "mode"
        raw_data:time_signature::integer    as time_signature,

        -- Duration: Last.fm sends seconds → convert to ms
        case
            when raw_data:source::varchar = 'lastfm'
            then coalesce(raw_data:duration_ms::integer, 0) * 1000
            else raw_data:duration_ms::integer
        end                                 as duration_ms,

        raw_data:source::varchar            as source,
        raw_data:ingested_at::date          as ingested_at,
        run_date,
        loaded_at,

        row_number() over (
            partition by raw_data:track_id::varchar
            order by loaded_at desc
        )                                   as row_num

    from raw
    where raw_data:track_id::varchar is not null

),

final as (

    select
        track_id,
        danceability,
        energy,
        loudness_db,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo_bpm,
        tempo_bpm                           as effective_tempo_bpm,  -- alias used by fct
        musical_key,
        musical_mode,

        case musical_mode
            when 1 then 'Major'
            when 0 then 'Minor'
            else 'Unknown'
        end                                 as key_mode_label,

        time_signature,
        duration_ms,

        -- Last.fm-specific (NULL for this source but keeping schema consistent)
        null::float                         as bpm_deezer,
        null::integer                       as deezer_rank,

        source,
        ingested_at,
        run_date

    from parsed
    where row_num = 1

)

select * from final