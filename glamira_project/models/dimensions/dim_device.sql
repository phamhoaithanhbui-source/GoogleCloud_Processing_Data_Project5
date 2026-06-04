{{ config(materialized='table') }}

WITH source_device AS (
    SELECT DISTINCT
        CAST(device_id AS STRING) AS device_id
        , CAST(user_agent AS STRING) AS user_agent
        , CAST(resolution AS STRING) AS screen_resolution
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE device_id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY device_id) AS device_key
    , device_id
    , user_agent
    , screen_resolution
    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_event' AS record_source
FROM source_device
