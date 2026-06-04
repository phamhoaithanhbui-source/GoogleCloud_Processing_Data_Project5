{% snapshot dim_customer_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email_address']
    )
}}

SELECT
    customer_id
    , ANY_VALUE(email_address) AS email_address
    , CURRENT_TIMESTAMP() AS dbt_loaded_at
    , 'stg_checkout_success' AS record_source
FROM {{ ref('stg_checkout_success') }}
WHERE customer_id IS NOT NULL
GROUP BY customer_id

{% endsnapshot %}
