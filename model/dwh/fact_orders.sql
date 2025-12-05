
{{ config(
    materialized='incremental',
    unique_key=['order_key'], -- Key to identify existing rows for updates
    incremental_strategy='merge', -- Standard, high-performance strategy on Snowflake
    snowflake_warehouse='REPORTING_WH', -- Example of warehouse specific configuration
    cluster_by=['order_date', 'customer_key'] -- Snowflake optimization for query performance
) }}

WITH stg_orders AS (
    -- Reference the staging model to ensure dependencies are managed by dbt
    SELECT
        order_key,
        customer_key,
        order_date,
        order_status,
        shipping_fee,
        updated_timestamp
    FROM
        {{ ref('stg_ecommerce__orders') }}
),

-- Calculate total value of the order (requires joining with line items)
order_totals AS (
    SELECT
        t1.order_key,
        SUM(t2.quantity * t2.unit_price) AS total_sales_amount
    FROM
        stg_orders t1
    INNER JOIN
        {{ source('ecommerce', 'raw_line_items') }} t2
        ON t1.order_key = t2.order_id
    GROUP BY 1
),

final_fact AS (
    SELECT
        o.order_key,
        o.customer_key,
        o.order_date,
        o.order_status,
        t.total_sales_amount,
        o.shipping_fee,
        (t.total_sales_amount + o.shipping_fee) AS total_invoice_amount,
        o.updated_timestamp
    FROM
        stg_orders o
    INNER JOIN
        order_totals t ON o.order_key = t.order_key
)

SELECT * FROM final_fact

{% if is_incremental() %}
  -- This filter ensures only new or updated records are processed during subsequent runs.
  -- It is critical for cost optimization on large fact tables.
  WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})
{% endif %}
