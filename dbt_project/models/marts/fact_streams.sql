-- fact_streams.sql
-- Fact table: one row per (track, date, platform) with stream measures.
-- Grain: track_id × date × platform

WITH tracks AS (
    SELECT * FROM {{ ref('int_tracks_with_popularity') }}
),

play_events AS (
    SELECT * FROM {{ ref('stg_play_events') }}
),

-- Build fact from multiple sources
track_streams AS (
    SELECT
        t.track_id,
        CASE 
            WHEN LENGTH(t.release_date) = 4 THEN CAST(t.release_date || '-01-01' AS DATE)
            WHEN LENGTH(t.release_date) = 7 THEN CAST(t.release_date || '-01' AS DATE)
            ELSE CAST(t.release_date AS DATE)
        END AS date_id,
        t.track_name,
        t.artists AS artist_name,
        COALESCE(t.spotify_streams, 0) AS total_streams,
        COALESCE(t.streams_2024_popularity, t.popularity, 0) AS popularity,
        t.is_charting,
        1 AS play_count,
        0 AS chart_position
    FROM tracks t
    WHERE t.track_id IS NOT NULL
),

event_streams AS (
    SELECT
        pe.track_id,
        CAST(pe.window_start AS DATE) AS date_id,
        pe.play_count,
        pe.skip_count,
        pe.like_count
    FROM play_events pe
    WHERE pe.track_id IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ts.track_id', 'ts.date_id']) }} AS stream_id,
    ts.track_id,
    ts.date_id,
    ts.artist_name,
    ts.play_count,
    ts.total_streams,
    ts.popularity,
    ts.chart_position,
    ts.is_charting
FROM track_streams ts
