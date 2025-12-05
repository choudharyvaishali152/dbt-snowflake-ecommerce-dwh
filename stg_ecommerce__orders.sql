{{ config(
    materialized='view' -- Use 'view' for staging models for quick execution
) }}

WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        shipping_fee,
        last_updated_at -- Key column for incremental logic later
    FROM
        {{ source('ecommerce', 'raw_orders') }} -- Assumes your source config maps to ECOMMERCE_DB.RAW
),

renamed_and_cleaned AS (
    SELECT
        -- Primary Key
        order_id AS order_key,

        -- Foreign Keys
        customer_id AS customer_key,

        -- Dates
        DATE_TRUNC('day', order_date) AS order_date,

        -- Metrics & Status
        status AS order_status,
        shipping_fee,

        -- Timestamps
        last_updated_at AS updated_timestamp
    FROM
        source_data
)

SELECT * FROM renamed_and_cleaned
