-- dim_date.sql
-- Dimension: date spine from 1921-01-01 to 2025-12-31.
-- Generated using dbt_utils.date_spine macro.

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1921-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),

final AS (
    SELECT
        date_day AS date_id,
        date_day AS full_date,
        EXTRACT(DAY FROM date_day)::INTEGER AS day,
        EXTRACT(MONTH FROM date_day)::INTEGER AS month,
        TO_CHAR(date_day, 'Month') AS month_name,
        EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
        EXTRACT(YEAR FROM date_day)::INTEGER AS year,
        (FLOOR(EXTRACT(YEAR FROM date_day) / 10) * 10)::INTEGER AS decade,
        CASE
            WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM date_spine
)

SELECT * FROM final
