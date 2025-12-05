-- This test verifies that the number of rows in the staging customer model 
-- matches the number of rows in the final customer dimension table.
-- This ensures no records were lost during the transformation step.

-- It passes if the row counts are EQUAL (i.e., the result of this query is 0).

WITH stg_count AS (
    SELECT COUNT(*) AS count_val FROM {{ ref('stg_ecommerce__customers') }}
),

dim_count AS (
    SELECT COUNT(*) AS count_val FROM {{ ref('dim_customers') }}
)

SELECT
    (stg.count_val - dim.count_val) AS row_difference
FROM
    stg_count stg, dim_count dim
-- The test fails if row_difference is not 0 (i.e., if counts are unequal)
WHERE (stg.count_val != dim.count_val)
