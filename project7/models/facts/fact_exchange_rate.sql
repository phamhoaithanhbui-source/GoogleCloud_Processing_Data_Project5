{{ config(materialized='table') }}

WITH exchange_rate_source AS (
    SELECT
        CAST(currency_code AS STRING) AS currency_code
        , CAST(rate_month AS DATE) AS rate_month
        , SAFE_CAST(exchange_rate_to_usd AS NUMERIC) AS exchange_rate_to_usd
    FROM {{ ref('exchange_rate_monthly') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            exchange_rate.currency_code
            , exchange_rate.rate_month
    ) AS exchange_rate_key

    , currency.currency_key
    , CAST(FORMAT_DATE('%Y%m%d', exchange_rate.rate_month) AS INT64) AS date_key

    , exchange_rate.currency_code
    , exchange_rate.rate_month
    , exchange_rate.exchange_rate_to_usd

    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'exchange_rate_monthly_seed' AS record_source

FROM exchange_rate_source AS exchange_rate
LEFT JOIN {{ ref('dim_currency') }} AS currency
    ON exchange_rate.currency_code = currency.currency_code
