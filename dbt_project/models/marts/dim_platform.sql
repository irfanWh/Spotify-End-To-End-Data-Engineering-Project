-- dim_platform.sql
-- Dimension: cross-platform presence for tracks from 2024 streaming data.
-- PK: platform_id (surrogate key)

WITH source AS (
    SELECT * FROM {{ ref('stg_streams_2024') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['track_name', 'artist_name']) }} AS platform_id,
    track_name,
    artist_name,
    spotify_playlist_count  AS in_spotify_playlists,
    spotify_streams,
    apple_music_playlist_count AS in_apple_playlists,
    deezer_playlist_count   AS in_deezer_playlists,
    shazam_counts           AS in_shazam_charts,
    tiktok_views,
    tiktok_posts,
    youtube_views,
    youtube_likes,
    pandora_streams,
    soundcloud_streams,
    tidal_popularity
FROM source
