SELECT
    'duplicate_sales_order_detail_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
GROUP BY sales_order_detail_key
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'null_customer_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE customer_key IS NULL

UNION ALL

SELECT
    'null_date_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE date_key IS NULL

UNION ALL

SELECT
    'null_location_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE location_key IS NULL

UNION ALL

SELECT
    'null_device_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE device_key IS NULL

UNION ALL

SELECT
    'null_store_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE store_key IS NULL

UNION ALL

SELECT
    'null_currency_key' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE currency_key IS NULL

UNION ALL

SELECT
    'negative_sales_amount' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE sales_amount < 0

UNION ALL

SELECT
    'negative_sales_amount_usd' AS test_name
    , CAST(sales_order_detail_key AS STRING) AS failed_value
    , 1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE sales_amount_usd < 0

