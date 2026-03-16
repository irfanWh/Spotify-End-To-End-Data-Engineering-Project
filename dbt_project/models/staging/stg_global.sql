-- stg_global.sql
-- Staging view over the global music dataset (spotify_data clean.csv).
-- Source: raw.global

WITH source AS (
    SELECT * FROM {{ source('raw_source', 'global') }}
)

SELECT
    track_id,
    track_name,
    NULLIF(TRIM(track_number::text), '')::FLOAT::INTEGER AS track_number,
    NULLIF(TRIM(track_popularity::text), '')::FLOAT::INTEGER AS track_popularity,
    explicit,
    artist_name,
    NULLIF(TRIM(artist_popularity::text), '')::FLOAT::INTEGER AS artist_popularity,
    NULLIF(TRIM(artist_followers::text), '')::FLOAT::BIGINT AS artist_followers,
    artist_genres,
    album_id,
    album_name,
    album_release_date,
    NULLIF(TRIM(album_total_tracks::text), '')::FLOAT::INTEGER AS album_total_tracks,
    UPPER(album_type) AS album_type,
    track_duration_min
FROM source
