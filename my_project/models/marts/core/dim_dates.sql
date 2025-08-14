{{ config(
    materialized='table',
    schema='marts'
) }}

WITH date_spine AS (
    SELECT generate_series(
        '2022-01-01'::date,
        CURRENT_DATE + INTERVAL '1 year',
        '1 day'::interval
    ) as date_day
)
SELECT
    date_day,
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DOW FROM date_day) AS day_of_week_num,
    TO_CHAR(date_day, 'Day') AS day_of_week_name,
    EXTRACT(DOY FROM date_day) AS day_of_year,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month,
    (EXTRACT(DOW FROM date_day) IN (0, 6)) AS is_weekend,
    date_day::DATE = CURRENT_DATE AS is_current_date
FROM
    date_spine
ORDER BY
    date_day