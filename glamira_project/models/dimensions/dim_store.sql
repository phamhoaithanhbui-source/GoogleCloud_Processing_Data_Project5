{{ config(materialized='table') }}

WITH source_store AS (
    SELECT DISTINCT
        CAST(store_id AS STRING) AS store_id
        , REGEXP_EXTRACT(current_url, r'https?://([^/]+)') AS store_domain
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE store_id IS NOT NULL
),

final_store AS (
    SELECT
        store_id
        , store_domain
        , CASE
            WHEN store_domain LIKE '%glamira.co.uk%' THEN 'Glamira UK'
            WHEN store_domain LIKE '%glamira.fr%' THEN 'Glamira France'
            WHEN store_domain LIKE '%glamira.de%' THEN 'Glamira Germany'
            WHEN store_domain LIKE '%glamira.com%' THEN 'Glamira Global'
            ELSE CONCAT('Glamira Store ', store_id)
        END AS store_name
    FROM source_store
)

SELECT
    ROW_NUMBER() OVER (ORDER BY store_id) AS store_key
    , store_id
    , store_name
    , store_domain
    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_event' AS record_source
FROM final_store

