-- dim_artist.sql
-- Dimension: deduplicated artist records with aggregated metrics.
-- SCD Type 1 (snapshot overwrite on each dbt run).
-- PK: artist_id (surrogate key)

WITH enriched_artists AS (
    -- Extract individual artists from comma-separated lists
    SELECT DISTINCT
        TRIM(UNNEST(STRING_TO_ARRAY(artists, ','))) AS artist_name
    FROM {{ ref('stg_tracks_enriched') }}
    WHERE artists IS NOT NULL
),

artist_metadata AS (
    SELECT
        id AS source_artist_id,
        name AS artist_name,
        followers AS follower_count,
        genres AS artist_genres,
        popularity AS artist_popularity
    FROM {{ source('raw_source', 'artists') }}
),

artist_stats AS (
    -- Compute average popularity and track count per artist
    SELECT
        TRIM(UNNEST(STRING_TO_ARRAY(t.artists, ','))) AS artist_name,
        AVG(t.popularity) AS avg_popularity,
        COUNT(DISTINCT t.track_id) AS track_count
    FROM {{ ref('stg_tracks_enriched') }} t
    WHERE t.artists IS NOT NULL
    GROUP BY 1
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ea.artist_name']) }} AS artist_id,
        ea.artist_name,
        COALESCE(am.follower_count, 0) AS follower_count,
        am.artist_genres,
        COALESCE(ast.avg_popularity, am.artist_popularity, 0) AS avg_popularity,
        COALESCE(ast.track_count, 0) AS track_count
    FROM enriched_artists ea
    LEFT JOIN artist_metadata am
        ON LOWER(TRIM(ea.artist_name)) = LOWER(TRIM(am.artist_name))
    LEFT JOIN artist_stats ast
        ON LOWER(TRIM(ea.artist_name)) = LOWER(TRIM(ast.artist_name))
)

SELECT * FROM final
