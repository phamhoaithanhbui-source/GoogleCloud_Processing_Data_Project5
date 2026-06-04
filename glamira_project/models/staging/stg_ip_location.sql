{{ config(materialized='view') }}

SELECT
    CAST(ip AS STRING) AS ip_address
    , CAST(country_code AS STRING) AS country_code
    , CAST(country_name AS STRING) AS country_name
    , CAST(region AS STRING) AS region_name
    , CAST(city AS STRING) AS city_name
FROM {{ source('raw', 'raw_ip_location') }}
WHERE ip IS NOT NULL
