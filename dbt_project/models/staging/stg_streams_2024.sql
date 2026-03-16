-- stg_streams_2024.sql
-- Staging view over the Most Streamed Spotify Songs 2024 dataset.
-- Source: raw.streams_2024

WITH source AS (
    SELECT * FROM {{ source('raw_source', 'streams_2024') }}
)

SELECT
    track                                       AS track_name,
    album_name,
    artist                                      AS artist_name,
    release_date,
    isrc,
    CAST(CAST(NULLIF(all_time_rank, '') AS FLOAT) AS INTEGER)   AS all_time_rank,
    CAST(NULLIF(track_score, '') AS FLOAT)       AS track_score,
    CAST(NULLIF(REPLACE(spotify_streams, ',', ''), '') AS BIGINT)  AS spotify_streams,
    CAST(CAST(NULLIF(REPLACE(spotify_playlist_count, ',', ''), '') AS FLOAT) AS INTEGER) AS spotify_playlist_count,
    CAST(NULLIF(REPLACE(spotify_playlist_reach, ',', ''), '') AS BIGINT)  AS spotify_playlist_reach,
    CAST(CAST(NULLIF(spotify_popularity, '') AS FLOAT) AS INTEGER)  AS spotify_popularity,
    CAST(NULLIF(REPLACE(youtube_views, ',', ''), '') AS BIGINT)    AS youtube_views,
    CAST(NULLIF(REPLACE(youtube_likes, ',', ''), '') AS BIGINT)    AS youtube_likes,
    CAST(NULLIF(REPLACE(tiktok_posts, ',', ''), '') AS BIGINT)     AS tiktok_posts,
    CAST(NULLIF(REPLACE(tiktok_likes, ',', ''), '') AS BIGINT)     AS tiktok_likes,
    CAST(NULLIF(REPLACE(tiktok_views, ',', ''), '') AS BIGINT)     AS tiktok_views,
    CAST(NULLIF(REPLACE(youtube_playlist_reach, ',', ''), '') AS BIGINT) AS youtube_playlist_reach,
    CAST(CAST(NULLIF(REPLACE(apple_music_playlist_count, ',', ''), '') AS FLOAT) AS INTEGER) AS apple_music_playlist_count,
    CAST(CAST(NULLIF(REPLACE(airplay_spins, ',', ''), '') AS FLOAT) AS INTEGER)   AS airplay_spins,
    CAST(CAST(NULLIF(REPLACE(siriusxm_spins, ',', ''), '') AS FLOAT) AS INTEGER)  AS siriusxm_spins,
    CAST(CAST(NULLIF(REPLACE(deezer_playlist_count, ',', ''), '') AS FLOAT) AS INTEGER) AS deezer_playlist_count,
    CAST(NULLIF(REPLACE(deezer_playlist_reach, ',', ''), '') AS BIGINT)  AS deezer_playlist_reach,
    CAST(CAST(NULLIF(REPLACE(amazon_playlist_count, ',', ''), '') AS FLOAT) AS INTEGER) AS amazon_playlist_count,
    CAST(NULLIF(REPLACE(pandora_streams, ',', ''), '') AS BIGINT)  AS pandora_streams,
    CAST(CAST(NULLIF(REPLACE(pandora_track_stations, ',', ''), '') AS FLOAT) AS INTEGER) AS pandora_track_stations,
    CAST(NULLIF(REPLACE(soundcloud_streams, ',', ''), '') AS BIGINT)     AS soundcloud_streams,
    CAST(NULLIF(REPLACE(shazam_counts, ',', ''), '') AS BIGINT)    AS shazam_counts,
    CAST(CAST(NULLIF(tidal_popularity, '') AS FLOAT) AS INTEGER)                   AS tidal_popularity,
    explicit_track
FROM source
