-- =============================================================
-- Spotify Data Intelligence Platform — Database Initialization
-- Creates schemas and tables for all data layers
-- =============================================================

-- ─────────────────────────────────────────────
-- RAW LAYER (append-only landing zone)
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;

-- Source: tracks_features.csv (1.2M songs with audio features)
CREATE TABLE IF NOT EXISTS raw.tracks_1m (
    id                  VARCHAR(100),
    name                TEXT,
    album               TEXT,
    album_id            VARCHAR(100),
    artists             TEXT,
    artist_ids          TEXT,
    track_number        INTEGER,
    disc_number         INTEGER,
    explicit            BOOLEAN,
    danceability        FLOAT,
    energy              FLOAT,
    key                 INTEGER,
    loudness            FLOAT,
    mode                INTEGER,
    speechiness         FLOAT,
    acousticness        FLOAT,
    instrumentalness    FLOAT,
    liveness            FLOAT,
    valence             FLOAT,
    tempo               FLOAT,
    duration_ms         INTEGER,
    time_signature      INTEGER,
    year                INTEGER,
    release_date        VARCHAR(50)
);

-- Source: tracks.csv (600K historical tracks 1921-2020)
CREATE TABLE IF NOT EXISTS raw.tracks_historical (
    id                  VARCHAR(100),
    name                TEXT,
    popularity          INTEGER,
    duration_ms         INTEGER,
    explicit            BOOLEAN,
    artists             TEXT,
    id_artists          TEXT,
    release_date        VARCHAR(50),
    danceability        FLOAT,
    energy              FLOAT,
    key                 INTEGER,
    loudness            FLOAT,
    mode                INTEGER,
    speechiness         FLOAT,
    acousticness        FLOAT,
    instrumentalness    FLOAT,
    liveness            FLOAT,
    valence             FLOAT,
    tempo               FLOAT,
    time_signature      INTEGER
);

-- Source: Most Streamed Spotify Songs 2024.csv
CREATE TABLE IF NOT EXISTS raw.streams_2024 (
    track                   TEXT,
    album_name              TEXT,
    artist                  TEXT,
    release_date            TEXT,
    isrc                    VARCHAR(50),
    all_time_rank           TEXT,
    track_score             TEXT,
    spotify_streams         TEXT,
    spotify_playlist_count  TEXT,
    spotify_playlist_reach  TEXT,
    spotify_popularity      TEXT,
    youtube_views           TEXT,
    youtube_likes           TEXT,
    tiktok_posts            TEXT,
    tiktok_likes            TEXT,
    tiktok_views            TEXT,
    youtube_playlist_reach  TEXT,
    apple_music_playlist_count TEXT,
    airplay_spins           TEXT,
    siriusxm_spins          TEXT,
    deezer_playlist_count   TEXT,
    deezer_playlist_reach   TEXT,
    amazon_playlist_count   TEXT,
    pandora_streams         TEXT,
    pandora_track_stations  TEXT,
    soundcloud_streams      TEXT,
    shazam_counts           TEXT,
    tidal_popularity        TEXT,
    explicit_track          TEXT
);

-- Source: spotify_data clean.csv (global music data 2009-2025)
CREATE TABLE IF NOT EXISTS raw.global (
    track_id                VARCHAR(100),
    track_name              TEXT,
    track_number            INTEGER,
    track_popularity        INTEGER,
    explicit                BOOLEAN,
    artist_name             TEXT,
    artist_popularity       INTEGER,
    artist_followers        BIGINT,
    artist_genres           TEXT,
    album_id                VARCHAR(100),
    album_name              TEXT,
    album_release_date      VARCHAR(50),
    album_total_tracks      INTEGER,
    album_type              VARCHAR(50),
    track_duration_min      FLOAT
);

-- Source: artists.csv (artist metadata)
CREATE TABLE IF NOT EXISTS raw.artists (
    id                  VARCHAR(100),
    followers           BIGINT,
    genres              TEXT,
    name                TEXT,
    popularity          INTEGER
);

-- Kafka streaming events (synthetic play events)
CREATE TABLE IF NOT EXISTS raw.play_events (
    event_id            TEXT PRIMARY KEY,
    user_id             VARCHAR(50),
    track_id            VARCHAR(50),
    event_type          VARCHAR(20),
    duration_played     INTEGER,
    device_type         VARCHAR(20),
    country             VARCHAR(10),
    ts                  TIMESTAMP DEFAULT NOW()
);

-- Spark Structured Streaming windowed aggregations
CREATE TABLE IF NOT EXISTS raw.play_events_windowed (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    track_id            VARCHAR(50),
    play_count          INTEGER,
    skip_count          INTEGER,
    like_count          INTEGER
);

-- ─────────────────────────────────────────────
-- STAGING LAYER (Spark output lands here)
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.tracks_enriched (
    track_id                VARCHAR(100),
    track_name              TEXT,
    album                   TEXT,
    album_id                VARCHAR(100),
    artists                 TEXT,
    artist_ids              TEXT,
    track_number            INTEGER,
    disc_number             INTEGER,
    explicit                BOOLEAN,
    danceability            FLOAT,
    energy                  FLOAT,
    key                     INTEGER,
    loudness                FLOAT,
    mode                    INTEGER,
    speechiness             FLOAT,
    acousticness            FLOAT,
    instrumentalness        FLOAT,
    liveness                FLOAT,
    valence                 FLOAT,
    tempo                   FLOAT,
    duration_ms             INTEGER,
    time_signature          INTEGER,
    year                    INTEGER,
    release_date            VARCHAR(50),
    popularity              INTEGER,
    energy_dance_ratio      FLOAT,
    decade                  INTEGER,
    in_both_sources         BOOLEAN,
    loaded_at               TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- MARTS LAYER (dbt builds these)
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS marts;

-- ─────────────────────────────────────────────
-- ML LAYER
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.feature_store (
    track_id                VARCHAR(50) PRIMARY KEY,
    danceability            FLOAT,
    energy                  FLOAT,
    loudness                FLOAT,
    speechiness             FLOAT,
    acousticness            FLOAT,
    instrumentalness        FLOAT,
    liveness                FLOAT,
    valence                 FLOAT,
    tempo                   FLOAT,
    duration_ms             INTEGER,
    rolling_30d_plays       INTEGER,
    popularity_label        INTEGER,
    genre                   VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS ml.predictions (
    track_id                VARCHAR(50),
    predicted_popularity    FLOAT,
    predicted_genre         VARCHAR(100),
    model_version           VARCHAR(50),
    scored_at               TIMESTAMP DEFAULT NOW()
);
