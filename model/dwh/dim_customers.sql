
{{ config(
    materialized='table',
    -- Cluster by region/join date if you frequently filter by these attributes
    cluster_by=['region', 'join_date'] 
) }}

SELECT
    customer_key,
    customer_name,
    region,
    join_date,
    email_address,
    -- Simple data transformation: derive a flag
    CASE 
        WHEN join_date >= DATEADD(month, -12, CURRENT_DATE()) THEN TRUE 
        ELSE FALSE 
    END AS is_new_customer_flag
FROM
    {{ ref('stg_ecommerce__customers') }}
