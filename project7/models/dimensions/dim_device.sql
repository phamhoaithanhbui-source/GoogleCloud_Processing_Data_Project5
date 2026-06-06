{{ config(materialized='table') }}

WITH source_device AS (
    SELECT
        CAST(device_id AS STRING) AS device_id
        , CAST(user_agent AS STRING) AS user_agent
        , CAST(resolution AS STRING) AS screen_resolution
        , TIMESTAMP_SECONDS(time_stamp) AS event_timestamp
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE device_id IS NOT NULL
),

deduplicated_device AS (
    SELECT
        device_id
        , ARRAY_AGG(
            user_agent
            ORDER BY event_timestamp DESC
            LIMIT 1
        )[OFFSET(0)] AS user_agent

        , ARRAY_AGG(
            screen_resolution
            ORDER BY event_timestamp DESC
            LIMIT 1
        )[OFFSET(0)] AS screen_resolution

        , MIN(event_timestamp) AS first_seen_at
        , MAX(event_timestamp) AS last_seen_at

    FROM source_device
    GROUP BY device_id
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY device_id
    ) AS device_key

    , device_id
    , user_agent
    , screen_resolution
    , first_seen_at
    , last_seen_at

    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_event' AS record_source

FROM deduplicated_device

