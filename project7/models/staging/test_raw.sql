SELECT
    COUNT(*) AS total_rows
FROM {{ source('raw', 'raw_event') }}
