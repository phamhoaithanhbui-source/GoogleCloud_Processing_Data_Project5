-- =====================================================
-- FACT SALES VALIDATION TESTS
-- =====================================================

-- 1. Duplicate sales_order_detail_key
SELECT
    sales_order_detail_key,
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
GROUP BY sales_order_detail_key
HAVING COUNT(*) > 1

UNION ALL

-- 2. Customer key missing
SELECT
    CAST(customer_key AS STRING),
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE customer_key IS NULL
GROUP BY customer_key

UNION ALL

-- 3. Date key missing
SELECT
    CAST(date_key AS STRING),
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE date_key IS NULL
GROUP BY date_key

UNION ALL

-- 4. Location key missing
SELECT
    CAST(location_key AS STRING),
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE location_key IS NULL
GROUP BY location_key

UNION ALL

-- 5. Device key missing
SELECT
    CAST(device_key AS STRING),
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE device_key IS NULL
GROUP BY device_key

UNION ALL

-- 6. Store key missing
SELECT
    CAST(store_key AS STRING),
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE store_key IS NULL
GROUP BY store_key

UNION ALL

-- 7. Currency key missing
SELECT
    CAST(currency_key AS STRING),
    COUNT(*) AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE currency_key IS NULL
GROUP BY currency_key

UNION ALL

-- 8. Negative sales amount
SELECT
    CAST(sales_order_detail_key AS STRING),
    1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE sales_amount < 0

UNION ALL

-- 9. Negative sales amount USD
SELECT
    CAST(sales_order_detail_key AS STRING),
    1 AS total_rows
FROM {{ ref('fact_sales_order_detail') }}
WHERE sales_amount_usd < 0

