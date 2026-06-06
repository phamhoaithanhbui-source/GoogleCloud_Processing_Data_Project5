{{ config(materialized='table') }}

WITH product_from_raw_event AS (
    SELECT DISTINCT
        CAST(product_id AS STRING) AS product_id
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE product_id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        CAST(viewing_product_id AS STRING) AS product_id
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`
    WHERE viewing_product_id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        CAST(cart.product_id AS STRING) AS product_id
    FROM `project-6978f7f5-636f-40bd-83c.glamira_dataset_raw.raw_event`,
    UNNEST(cart_products) AS cart
    WHERE cart.product_id IS NOT NULL
),

product_from_crawler AS (
    SELECT
        CAST(product_id AS STRING) AS product_id
        , ANY_VALUE(product_name) AS product_name
        , ANY_VALUE(product_url) AS product_url
        , ANY_VALUE(final_url) AS final_url
    FROM {{ ref('stg_product') }}
    WHERE product_id IS NOT NULL
    GROUP BY product_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY raw_product.product_id) AS product_key
    , raw_product.product_id
    , COALESCE(
        crawler.product_name,
        CONCAT('Unknown Product ', raw_product.product_id)
    ) AS product_name
    , crawler.product_url
    , crawler.final_url
    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'raw_event_and_raw_product' AS record_source
FROM product_from_raw_event AS raw_product
LEFT JOIN product_from_crawler AS crawler
    ON raw_product.product_id = crawler.product_id

