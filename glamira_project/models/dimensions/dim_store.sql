{{ config(materialized='table') }}

WITH source_store AS (
    SELECT
        CAST(store_id AS STRING) AS store_id
        , REGEXP_EXTRACT(CAST(current_url AS STRING), r'https?://([^/]+)') AS store_domain
        , TIMESTAMP_SECONDS(time_stamp) AS event_timestamp
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE store_id IS NOT NULL
),

deduplicated_store AS (
    SELECT
        store_id

        , ARRAY_AGG(
            store_domain IGNORE NULLS
            ORDER BY event_timestamp DESC
            LIMIT 1
        )[SAFE_OFFSET(0)] AS store_domain

        , MIN(event_timestamp) AS first_seen_at
        , MAX(event_timestamp) AS last_seen_at

    FROM source_store
    GROUP BY store_id
),

final_store AS (
    SELECT
        store_id
        , store_domain
        , CASE
            WHEN store_domain LIKE '%glamira.co.uk%' THEN 'Glamira UK'
            WHEN store_domain LIKE '%glamira.fr%' THEN 'Glamira France'
            WHEN store_domain LIKE '%glamira.de%' THEN 'Glamira Germany'
            WHEN store_domain LIKE '%glamira.it%' THEN 'Glamira Italy'
            WHEN store_domain LIKE '%glamira.es%' THEN 'Glamira Spain'
            WHEN store_domain LIKE '%glamira.nl%' THEN 'Glamira Netherlands'
            WHEN store_domain LIKE '%glamira.se%' THEN 'Glamira Sweden'
            WHEN store_domain LIKE '%glamira.com%' THEN 'Glamira Global'
            ELSE CONCAT('Glamira Store ', store_id)
        END AS store_name
        , first_seen_at
        , last_seen_at
    FROM deduplicated_store
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY store_id
    ) AS store_key

    , store_id
    , store_name
    , store_domain
    , first_seen_at
    , last_seen_at

    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_event' AS record_source

FROM final_store

