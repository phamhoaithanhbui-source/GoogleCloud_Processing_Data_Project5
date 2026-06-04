{{ config(materialized='table') }}

WITH raw_customer AS (
    SELECT
        CAST(user_id_db AS STRING) AS customer_id
        , LOWER(TRIM(ANY_VALUE(email_address))) AS email_address
        , MIN(TIMESTAMP_SECONDS(time_stamp)) AS first_seen_at
        , MAX(TIMESTAMP_SECONDS(time_stamp)) AS last_seen_at
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE user_id_db IS NOT NULL
    GROUP BY user_id_db
)

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key
    , customer_id
    , email_address
    , first_seen_at AS valid_from
    , TIMESTAMP('3000-01-01 00:00:00 UTC') AS valid_to
    , TRUE AS is_current
    , CURRENT_TIMESTAMP() AS created_at
    , CURRENT_TIMESTAMP() AS updated_at
    , 'raw_event' AS record_source
FROM raw_customer

