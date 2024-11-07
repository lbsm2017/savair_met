-- models\marts\pe_stages.sql

WITH xform_data AS (
    SELECT
        vintage_year,
        asset_class,
        geo_region,
        first_transaction_period,
        as_of_date,
        transaction_year_decimal,
        fund_age
    FROM {{ ref('pe_lifespan') }}
),

performance_data AS (
    SELECT
        vintage_year,
        asset_class,
        geo_region,
        as_of_date
    FROM {{ ref('base_pe_returns') }}
),

merged_data AS (
    SELECT
        xform_data.vintage_year,
        xform_data.asset_class,
        xform_data.geo_region,
        xform_data.as_of_date,
        xform_data.fund_age
    FROM xform_data
    JOIN performance_data
    ON xform_data.vintage_year = performance_data.vintage_year
    AND xform_data.asset_class = performance_data.asset_class
    AND xform_data.geo_region = performance_data.geo_region
    AND xform_data.as_of_date = performance_data.as_of_date
),

-- Join merged_data with base_pe_stage_criteria to dynamically assign fund_stage
staged_data AS (
    SELECT
        md.vintage_year,
        md.asset_class,
        md.geo_region,
        md.as_of_date,
        md.fund_age,
        COALESCE(fsc.fund_stage, 'Undefined') AS fund_stage
    FROM merged_data md
    LEFT JOIN {{ ref('base_pe_stage_criteria') }} fsc
    ON md.asset_class = fsc.asset_class
    AND md.fund_age >= fsc.min_fund_age
    AND (md.fund_age <= fsc.max_fund_age OR fsc.max_fund_age IS NULL)
)

SELECT * FROM staged_data
