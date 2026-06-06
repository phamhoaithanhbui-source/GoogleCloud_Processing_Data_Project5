{{ config(materialized='view') }}

SELECT
    CAST(product_id AS STRING) AS product_id
    , CAST(product_name AS STRING) AS product_name
    , CAST(url AS STRING) AS product_url
    , CAST(final_url AS STRING) AS final_url
FROM {{ source('raw', 'raw_product') }}
WHERE product_id IS NOT NULL

