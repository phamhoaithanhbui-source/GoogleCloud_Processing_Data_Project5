{{
    config(
        materialized='incremental',
        unique_key='sales_order_detail_key'
    )
}}

WITH checkout_order_detail AS (
    SELECT
        ABS(
            FARM_FINGERPRINT(
                CONCAT(
                    checkout.event_id
                    , '|'
                    , CAST(cart.cart_product_index AS STRING)
                )
            )
        ) AS sales_order_detail_key

        , checkout.customer_id
        , cart.product_id
        , CAST(FORMAT_DATE('%Y%m%d', checkout.order_date) AS INT64) AS date_key
        , checkout.ip_address
        , checkout.device_id
        , checkout.store_id
        , cart.currency_code

        , checkout.order_id
        , checkout.event_id

        , cart.order_qty
        , cart.unit_price
        , cart.order_qty * cart.unit_price AS sales_amount

        , checkout.order_date
        , checkout.event_timestamp AS order_datetime_utc
        , checkout.order_datetime_local
        , checkout.order_time_local

    FROM {{ ref('stg_checkout_success') }} AS checkout
    LEFT JOIN {{ ref('stg_cart_product') }} AS cart
        ON checkout.event_id = cart.event_id
    WHERE cart.product_id IS NOT NULL

    {% if is_incremental() %}
        AND checkout.event_timestamp > (
            SELECT
                COALESCE(MAX(order_datetime_utc), TIMESTAMP('1900-01-01'))
            FROM {{ this }}
        )
    {% endif %}
),

order_with_location AS (
    SELECT
        order_detail.*
        , ip_location.country_code
        , ip_location.country_name
        , ip_location.region_name
        , ip_location.city_name
    FROM checkout_order_detail AS order_detail
    LEFT JOIN {{ ref('stg_ip_location') }} AS ip_location
        ON order_detail.ip_address = ip_location.ip_address
),

joined_fact AS (
    SELECT
        order_detail.sales_order_detail_key

        , customer.customer_key
        , product.product_key
        , order_detail.date_key
        , location.location_key
        , device.device_key
        , store.store_key
        , currency.currency_key

        , order_detail.order_id
        , order_detail.event_id
        , order_detail.ip_address
        , order_detail.currency_code

        , order_detail.order_qty
        , order_detail.unit_price
        , order_detail.sales_amount

        , exchange_rate.exchange_rate_to_usd
        , order_detail.unit_price
            * exchange_rate.exchange_rate_to_usd AS unit_price_usd
        , order_detail.sales_amount
            * exchange_rate.exchange_rate_to_usd AS sales_amount_usd

        , order_detail.order_date
        , order_detail.order_datetime_utc
        , order_detail.order_datetime_local
        , order_detail.order_time_local

        , CURRENT_TIMESTAMP() AS dbt_loaded_at
        , 'checkout_success' AS record_source

    FROM order_with_location AS order_detail

 
    LEFT JOIN {{ ref('dim_customer') }} AS customer
    ON order_detail.customer_id = customer.customer_id
    AND customer.is_current = TRUE


    LEFT JOIN {{ ref('dim_product') }} AS product
        ON order_detail.product_id = product.product_id

    LEFT JOIN {{ ref('dim_location') }} AS location
        ON COALESCE(order_detail.country_code, '') = COALESCE(location.country_code, '')
        AND COALESCE(order_detail.country_name, '') = COALESCE(location.country_name, '')
        AND COALESCE(order_detail.region_name, '') = COALESCE(location.region_name, '')
        AND COALESCE(order_detail.city_name, '') = COALESCE(location.city_name, '')

    LEFT JOIN {{ ref('dim_device') }} AS device
        ON order_detail.device_id = device.device_id

    LEFT JOIN {{ ref('dim_store') }} AS store
        ON order_detail.store_id = store.store_id

    LEFT JOIN {{ ref('dim_currency') }} AS currency
        ON order_detail.currency_code = currency.currency_code

    LEFT JOIN {{ ref('fact_exchange_rate') }} AS exchange_rate
        ON currency.currency_key = exchange_rate.currency_key
        AND CAST(
            FORMAT_DATE('%Y%m%d', DATE_TRUNC(order_detail.order_date, MONTH))
            AS INT64
        ) = exchange_rate.date_key
)

SELECT
    sales_order_detail_key

    , customer_key
    , product_key
    , date_key
    , location_key
    , device_key
    , store_key
    , currency_key

    , order_id
    , event_id
    , ip_address
    , currency_code

    , order_qty
    , unit_price
    , sales_amount
    , exchange_rate_to_usd
    , unit_price_usd
    , sales_amount_usd

    , order_date
    , order_datetime_utc
    , order_datetime_local
    , order_time_local

    , dbt_loaded_at
    , record_source

FROM joined_fact
