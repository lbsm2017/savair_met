-- models/x-forms/xform_pe_lifespan.sql

WITH base_data AS (
    SELECT
        CAST(vintage_year AS INT) AS vintage_year,
        asset_class,
        first_transaction_period,
        CAST(as_of_date AS DATE) AS as_of_date
    FROM {{ ref('base_pe_returns') }}
),

transformed_data AS (
    SELECT *,
        -- Extract year and quarter from `first_transaction_period`
        CAST(SUBSTRING(first_transaction_period, 1, 4) AS FLOAT) AS transaction_year,
        CASE 
            WHEN RIGHT(first_transaction_period, 2) = 'Q1' THEN 0.25
            WHEN RIGHT(first_transaction_period, 2) = 'Q2' THEN 0.50
            WHEN RIGHT(first_transaction_period, 2) = 'Q3' THEN 0.75
            WHEN RIGHT(first_transaction_period, 2) = 'Q4' THEN 1.00
            ELSE 0.0  -- Fallback if quarters are non-standard
        END AS transaction_quarter
    FROM base_data
),

computed_data AS (
    SELECT *,
        -- Combine transaction year and quarter to form the decimal year
        transaction_year + transaction_quarter AS transaction_year_decimal,

        -- Compute the years difference and round to two decimal places
        ROUND(
            CAST(
                EXTRACT(YEAR FROM as_of_date) + (EXTRACT(DOY FROM as_of_date) / 365.25) 
                - (transaction_year + transaction_quarter) AS NUMERIC
            ), 2
        ) AS years_difference
    FROM transformed_data
)

SELECT
    vintage_year,
    asset_class,
    first_transaction_period,
    as_of_date,
    transaction_year_decimal,
    years_difference
FROM computed_data
