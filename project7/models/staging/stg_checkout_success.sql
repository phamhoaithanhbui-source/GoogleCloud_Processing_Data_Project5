{{ config(materialized='view') }}

SELECT
    CAST(event_id AS STRING) AS event_id
    , CAST(order_id AS STRING) AS order_id
    , CAST(user_id_db AS STRING) AS customer_id
    , CAST(store_id AS STRING) AS store_id
    , CAST(device_id AS STRING) AS device_id
    , CAST(ip AS STRING) AS ip_address

    , CAST(time_stamp AS INT64) AS time_stamp
    , TIMESTAMP_SECONDS(time_stamp) AS event_timestamp
    , DATE(TIMESTAMP_SECONDS(time_stamp)) AS order_date
    , SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', local_time) AS order_datetime_local
    , TIME(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', local_time)) AS order_time_local

    , current_url
    , referrer_url
    , email_address
    , user_agent
    , resolution
    , cart_products
FROM {{ source('raw', 'raw_event') }}
WHERE collection = 'checkout_success'

