-- int_tracks_with_popularity.sql
-- Intermediate: joins enriched tracks with 2024 streaming data.
-- Adds cross-platform stream counts and charting flag.

WITH tracks AS (
    SELECT * FROM {{ ref('stg_tracks_enriched') }}
),

streams AS (
    SELECT * FROM {{ ref('stg_streams_2024') }}
),

joined AS (
    SELECT
        t.track_id,
        t.track_name,
        t.album,
        t.album_id,
        t.artists,
        t.artist_ids,
        t.explicit,
        t.danceability,
        t.energy,
        t.key,
        t.loudness,
        t.mode,
        t.speechiness,
        t.acousticness,
        t.instrumentalness,
        t.liveness,
        t.valence,
        t.tempo,
        t.duration_ms,
        t.time_signature,
        t.year,
        t.release_date,
        t.popularity,
        t.energy_dance_ratio,
        t.decade,
        t.in_both_sources,
        s.spotify_streams,
        s.spotify_popularity AS streams_2024_popularity,
        s.youtube_views,
        s.tiktok_views,
        s.shazam_counts,
        s.apple_music_playlist_count,
        s.deezer_playlist_count,
        CASE
            WHEN s.track_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_charting
    FROM tracks t
    LEFT JOIN streams s
        ON LOWER(TRIM(t.track_name)) = LOWER(TRIM(s.track_name))
        AND LOWER(TRIM(SPLIT_PART(t.artists, ',', 1))) = LOWER(TRIM(s.artist_name))
)

SELECT * FROM joined
