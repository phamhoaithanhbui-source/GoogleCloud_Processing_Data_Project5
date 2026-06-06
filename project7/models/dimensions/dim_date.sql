{{ config(materialized='table') }}

WITH date_source AS (
    SELECT DISTINCT
        DATE(TIMESTAMP_SECONDS(time_stamp)) AS full_date
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE time_stamp IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        CAST(rate_month AS DATE) AS full_date
    FROM {{ ref('exchange_rate_monthly') }}
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) AS date_key
    , full_date
    , EXTRACT(YEAR FROM full_date) AS year_number
    , EXTRACT(QUARTER FROM full_date) AS quarter_number
    , EXTRACT(MONTH FROM full_date) AS month_number
    , FORMAT_DATE('%B', full_date) AS month_name
    , EXTRACT(DAY FROM full_date) AS day_number
    , FORMAT_DATE('%A', full_date) AS weekday_name
    , EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) AS is_weekend
    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_event' AS record_source
FROM date_source

