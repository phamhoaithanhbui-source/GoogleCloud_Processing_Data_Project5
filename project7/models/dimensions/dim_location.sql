{{ config(materialized='table') }}

WITH source_location AS (
    SELECT DISTINCT
        COALESCE(CAST(country_code AS STRING), '') AS country_code
        , COALESCE(CAST(country_name AS STRING), '') AS country_name
        , COALESCE(CAST(region_name AS STRING), '') AS region_name
        , COALESCE(CAST(city_name AS STRING), '') AS city_name
    FROM {{ ref('stg_ip_location') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY country_code, country_name, region_name, city_name
    ) AS location_key
    , NULLIF(country_code, '') AS country_code
    , NULLIF(country_name, '') AS country_name
    , NULLIF(region_name, '') AS region_name
    , NULLIF(city_name, '') AS city_name
    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_ip_location' AS record_source
FROM source_location

