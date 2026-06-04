{{ config(materialized='view') }}

SELECT
    checkout.event_id
    , checkout.order_id
    , cart_product_index


    , CAST(cart_product.product_id AS STRING) AS product_id

    , SAFE_CAST(cart_product.amount AS INT64) AS order_qty

    , SAFE_CAST(
        REGEXP_REPLACE(
            CAST(cart_product.price AS STRING)
            , r'[^0-9.]'
            , ''
        ) AS NUMERIC
    ) AS unit_price

    , CASE
    WHEN TRIM(cart_product.currency) IN ('$', 'USD $', 'USD') THEN 'USD'
    WHEN TRIM(cart_product.currency) IN ('€', 'EUR') THEN 'EUR'
    WHEN TRIM(cart_product.currency) IN ('£', 'GBP') THEN 'GBP'
    WHEN TRIM(cart_product.currency) IN ('CHF') THEN 'CHF'

    WHEN TRIM(cart_product.currency) IN ('AU $', 'AUD') THEN 'AUD'
    WHEN TRIM(cart_product.currency) IN ('CAD $', 'CAD') THEN 'CAD'
    WHEN TRIM(cart_product.currency) IN ('NZD $', 'NZD') THEN 'NZD'
    WHEN TRIM(cart_product.currency) IN ('SGD $', 'SGD') THEN 'SGD'
    WHEN TRIM(cart_product.currency) IN ('HKD $', 'HKD') THEN 'HKD'
    WHEN TRIM(cart_product.currency) IN ('MXN $', 'MXN') THEN 'MXN'

    WHEN TRIM(cart_product.currency) IN ('KČ', 'Kč') THEN 'CZK'
    WHEN TRIM(cart_product.currency) IN ('FT', 'Ft') THEN 'HUF'
    WHEN TRIM(cart_product.currency) IN ('ZŁ', 'zł') THEN 'PLN'
    WHEN TRIM(cart_product.currency) IN ('DIN.', 'din.') THEN 'RSD'
    WHEN TRIM(cart_product.currency) IN ('₫') THEN 'VND'
    WHEN TRIM(cart_product.currency) IN ('KN', 'kn') THEN 'HRK'
    WHEN TRIM(cart_product.currency) IN ('ЛВ.', 'лв.') THEN 'BGN'
    WHEN UPPER(TRIM(CAST(cart_product.currency AS STRING))) = 'LEI' THEN 'RON'

    WHEN TRIM(cart_product.currency) IN ('R$', 'BRL') THEN 'BRL'
    WHEN TRIM(cart_product.currency) IN ('₹', 'INR') THEN 'INR'
    WHEN TRIM(cart_product.currency) IN ('￥', '¥') THEN 'JPY'
    WHEN TRIM(cart_product.currency) IN ('₺', 'TRY') THEN 'TRY'
    WHEN TRIM(cart_product.currency) IN ('₱') THEN 'PHP'
    WHEN TRIM(cart_product.currency) IN ('₲') THEN 'PYG'

    WHEN TRIM(cart_product.currency) IN ('BOB Bs', 'BOB BS') THEN 'BOB'
    WHEN TRIM(cart_product.currency) IN ('COP $') THEN 'COP'
    WHEN TRIM(cart_product.currency) IN ('CRC ₡') THEN 'CRC'
    WHEN TRIM(cart_product.currency) IN ('GTQ Q') THEN 'GTQ'
    WHEN TRIM(cart_product.currency) IN ('PEN S/.') THEN 'PEN'
    WHEN TRIM(cart_product.currency) IN ('DOP $') THEN 'DOP'
    WHEN TRIM(cart_product.currency) IN ('CLP') THEN 'CLP'
    WHEN TRIM(cart_product.currency) IN ('UYU') THEN 'UYU'
    WHEN TRIM(cart_product.currency) IN ('د.ك.‏', 'د.ك') THEN 'KWD'

    WHEN TRIM(cart_product.currency) IN ('KR', 'kr') THEN 'SEK'

    WHEN cart_product.currency IS NULL OR TRIM(cart_product.currency) = '' THEN 'UNKNOWN'
    ELSE UPPER(TRIM(CAST(cart_product.currency AS STRING)))
END AS currency_code
FROM {{ ref('stg_checkout_success') }} AS checkout
    , UNNEST(checkout.cart_products) AS cart_product WITH OFFSET AS cart_product_index

