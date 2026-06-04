{{ config(materialized='table') }}

WITH raw_customer AS (
    SELECT
        CAST(user_id_db AS STRING) AS customer_id
        , LOWER(TRIM(COALESCE(email_address, 'UNKNOWN'))) AS email_address
        , TIMESTAMP_SECONDS(time_stamp) AS event_timestamp
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE user_id_db IS NOT NULL
),

customer_email_history AS (
    SELECT
        customer_id
        , email_address
        , MIN(event_timestamp) AS valid_from
    FROM raw_customer
    GROUP BY
        customer_id
        , email_address
),

scd_customer AS (
    SELECT
        customer_id
        , email_address
        , valid_from
        , LEAD(valid_from) OVER (
            PARTITION BY customer_id
            ORDER BY valid_from
        ) AS next_valid_from
    FROM customer_email_history
)

SELECT
    ABS(
        FARM_FINGERPRINT(
            CONCAT(
                customer_id
                , '|'
                , email_address
                , '|'
                , CAST(valid_from AS STRING)
            )
        )
    ) AS customer_key

    , customer_id
    , email_address

    , valid_from
    , COALESCE(
        TIMESTAMP_SUB(next_valid_from, INTERVAL 1 SECOND),
        TIMESTAMP('3000-01-01 00:00:00 UTC')
    ) AS valid_to

    , CASE
        WHEN next_valid_from IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current

    , CURRENT_TIMESTAMP() AS created_at
    , CURRENT_TIMESTAMP() AS updated_at
    , 'raw_event' AS record_source

FROM scd_customer
