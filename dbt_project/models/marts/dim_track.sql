-- dim_track.sql
-- Dimension: track attributes including audio features and metadata.
-- PK: track_id

WITH source AS (
    SELECT * FROM {{ ref('int_tracks_with_popularity') }}
),

global AS (
    SELECT * FROM {{ ref('stg_global') }}
)

SELECT
    s.track_id,
    s.track_name,
    s.explicit,
    s.danceability,
    s.energy,
    s.loudness,
    s.speechiness,
    s.acousticness,
    s.instrumentalness,
    s.liveness,
    s.valence,
    s.tempo,
    s.duration_ms,
    s.time_signature,
    s.energy_dance_ratio,
    s.decade,
    g.artist_genres AS genre,
    s.is_charting,
    COALESCE(s.popularity, 0) AS popularity
FROM source s
LEFT JOIN (
    SELECT DISTINCT ON (track_id)
        track_id,
        artist_genres
    FROM global
    WHERE track_id IS NOT NULL
    ORDER BY track_id, track_popularity DESC
) g ON s.track_id = g.track_id
