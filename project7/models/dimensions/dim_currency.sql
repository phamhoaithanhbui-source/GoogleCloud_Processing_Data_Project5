{{ config(materialized='table') }}

WITH currency_source AS (
    SELECT DISTINCT
        currency_code
    FROM {{ ref('stg_cart_product') }}
    WHERE currency_code IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        CAST(currency_code AS STRING) AS currency_code
    FROM {{ ref('exchange_rate_monthly') }}
    WHERE currency_code IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY currency_code) AS currency_key
    , currency_code

    , CASE
        WHEN currency_code = 'USD' THEN 'US Dollar'
        WHEN currency_code = 'EUR' THEN 'Euro'
        WHEN currency_code = 'GBP' THEN 'British Pound'
        WHEN currency_code = 'CHF' THEN 'Swiss Franc'
        WHEN currency_code = 'AUD' THEN 'Australian Dollar'
        WHEN currency_code = 'CAD' THEN 'Canadian Dollar'
        WHEN currency_code = 'NZD' THEN 'New Zealand Dollar'
        WHEN currency_code = 'SGD' THEN 'Singapore Dollar'
        WHEN currency_code = 'HKD' THEN 'Hong Kong Dollar'
        WHEN currency_code = 'MXN' THEN 'Mexican Peso'
        WHEN currency_code = 'INR' THEN 'Indian Rupee'
        WHEN currency_code = 'JPY' THEN 'Japanese Yen'
        WHEN currency_code = 'TRY' THEN 'Turkish Lira'
        WHEN currency_code = 'BRL' THEN 'Brazilian Real'
        WHEN currency_code = 'SEK' THEN 'Swedish Krona'
        WHEN currency_code = 'CZK' THEN 'Czech Koruna'
        WHEN currency_code = 'HUF' THEN 'Hungarian Forint'
        WHEN currency_code = 'PLN' THEN 'Polish Zloty'
        WHEN currency_code = 'CLP' THEN 'Chilean Peso'
        WHEN currency_code = 'BGN' THEN 'Bulgarian Lev'
        WHEN currency_code = 'HRK' THEN 'Croatian Kuna'
        WHEN currency_code = 'COP' THEN 'Colombian Peso'
        WHEN currency_code = 'PEN' THEN 'Peruvian Sol'
        WHEN currency_code = 'PHP' THEN 'Philippine Peso'
        WHEN currency_code = 'RSD' THEN 'Serbian Dinar'
        WHEN currency_code = 'VND' THEN 'Vietnamese Dong'
        WHEN currency_code = 'GTQ' THEN 'Guatemalan Quetzal'
        WHEN currency_code = 'CRC' THEN 'Costa Rican Colón'
        WHEN currency_code = 'UYU' THEN 'Uruguayan Peso'
        WHEN currency_code = 'BOB' THEN 'Bolivian Boliviano'
        WHEN currency_code = 'DOP' THEN 'Dominican Peso'
        WHEN currency_code = 'PYG' THEN 'Paraguayan Guaraní'
        WHEN currency_code = 'KWD' THEN 'Kuwaiti Dinar'
        WHEN currency_code = 'RON' THEN 'Romanian Leu'
        WHEN currency_code = 'UNKNOWN' THEN 'Unknown Currency'
        ELSE CONCAT(currency_code, ' Currency')
    END AS currency_name

    , CASE
        WHEN currency_code = 'USD' THEN '$'
        WHEN currency_code = 'EUR' THEN '€'
        WHEN currency_code = 'GBP' THEN '£'
        WHEN currency_code = 'JPY' THEN '¥'
        WHEN currency_code = 'VND' THEN '₫'
        WHEN currency_code = 'INR' THEN '₹'
        WHEN currency_code = 'TRY' THEN '₺'
        WHEN currency_code = 'BRL' THEN 'R$'
        ELSE currency_code
    END AS currency_symbol

    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'dbt_transformation' AS record_source

FROM currency_source
