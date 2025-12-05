{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT
        customer_id,
        customer_name,
        region,
        join_date,
        email_address,
        street_address
    FROM
        {{ source('ecommerce', 'raw_customers') }}
),

renamed_and_cleaned AS (
    SELECT
        -- Primary Key
        customer_id AS customer_key,

        -- Attributes
        customer_name,
        region,
        join_date,
        email_address,
        street_address
    FROM
        source_data
)

SELECT * FROM renamed_and_cleaned
