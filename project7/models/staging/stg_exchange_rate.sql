{{ config(materialized='view') }}

SELECT
    currency_code
    , SAFE_CAST(exchange_rate_to_usd AS NUMERIC) AS exchange_rate_to_usd
FROM {{ ref('exchange_rate') }}

