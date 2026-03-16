-- stg_play_events.sql
-- Staging view over the Spark Structured Streaming windowed output.
-- Source: raw.play_events_windowed

WITH source AS (
    SELECT * FROM {{ source('raw_source', 'play_events_windowed') }}
)

SELECT
    window_start,
    window_end,
    track_id,
    play_count,
    skip_count,
    like_count
FROM source
