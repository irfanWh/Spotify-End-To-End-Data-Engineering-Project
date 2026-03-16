-- stg_tracks_enriched.sql
-- Staging view over the Spark-cleaned and joined tracks data.
-- Source: staging.tracks_enriched (output of Spark Job 1)

WITH source AS (
    SELECT * FROM {{ source('staging_source', 'tracks_enriched') }}
)

SELECT
    track_id,
    track_name,
    album,
    album_id,
    artists,
    artist_ids,
    track_number,
    disc_number,
    explicit,
    danceability,
    energy,
    key,
    loudness,
    mode,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    duration_ms,
    time_signature,
    year,
    release_date,
    popularity,
    energy_dance_ratio,
    decade,
    in_both_sources,
    CURRENT_TIMESTAMP AS loaded_at
FROM source
