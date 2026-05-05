-- models/staging/stg_audio_features.sql
-- ────────────────────────────────────────
-- Cleans audio features. Handles NULLs from Deezer (which lacks Spotify's
-- audio analysis endpoint). Downstream models use COALESCE / IS NOT NULL.

{{ config(materialized='view') }}

with raw as (

    select
        raw_data,
        run_date,
        loaded_at

    from {{ source('raw', 'audio_features') }}

),

parsed as (

    select
        raw_data:track_id::varchar            as track_id,

        -- Spotify-native features (NULL for Deezer source)
        raw_data:danceability::float          as danceability,
        raw_data:energy::float                as energy,
        raw_data:loudness::float              as loudness_db,
        raw_data:speechiness::float           as speechiness,
        raw_data:acousticness::float          as acousticness,
        raw_data:instrumentalness::float      as instrumentalness,
        raw_data:liveness::float              as liveness,
        raw_data:valence::float               as valence,
        raw_data:tempo::float                 as tempo_bpm,
        raw_data:key::integer                 as musical_key,
        raw_data:mode::integer                as musical_mode,  -- 1=major, 0=minor
        raw_data:time_signature::integer      as time_signature,
        raw_data:duration_ms::integer         as duration_ms,

        -- Deezer-native fields (NULL for Spotify source)
        raw_data:bpm::float                   as bpm_deezer,
        raw_data:rank::integer                as deezer_rank,

        -- Combine BPM sources: prefer Spotify tempo, fall back to Deezer BPM
        coalesce(
            raw_data:tempo::float,
            raw_data:bpm::float
        )                                     as effective_tempo_bpm,

        raw_data:source::varchar              as source,
        raw_data:ingested_at::date            as ingested_at,
        run_date,
        loaded_at,

        row_number() over (
            partition by raw_data:track_id::varchar
            order by loaded_at desc
        )                                     as row_num

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
        effective_tempo_bpm,
        musical_key,
        musical_mode,
        case musical_mode
            when 1 then 'Major'
            when 0 then 'Minor'
            else 'Unknown'
        end                                   as key_mode_label,
        time_signature,
        duration_ms,
        bpm_deezer,
        deezer_rank,
        source,
        ingested_at,
        run_date

    from parsed
    where row_num = 1

)

select * from final