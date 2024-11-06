-- models/x-form/xform_fund_stages.sql

WITH xform_data AS (
    SELECT
        vintage_year,
        asset_class,
        geo_region,
        first_transaction_period,
        as_of_date,
        transaction_year_decimal,
        years_difference AS fund_age
    FROM {{ ref('xform_pe_lifespan') }}
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
)

SELECT
    vintage_year,
    asset_class,
    geo_region,
    as_of_date,
    fund_age,
    
    CASE
        -- Buyout & Growth Equity
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age <= 3 THEN 'Commitment'
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age BETWEEN 4 AND 6 THEN 'Growth'
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age BETWEEN 7 AND 10 THEN 'Maturity'
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age > 10 THEN 'Wind-Down'

        -- Venture Capital
        WHEN asset_class = 'Venture Capital' AND fund_age <= 5 THEN 'Commitment'
        WHEN asset_class = 'Venture Capital' AND fund_age BETWEEN 6 AND 8 THEN 'Growth'
        WHEN asset_class = 'Venture Capital' AND fund_age BETWEEN 9 AND 12 THEN 'Distribution'
        WHEN asset_class = 'Venture Capital' AND fund_age > 12 THEN 'Wind-Down'

        -- Real Estate
        WHEN asset_class = 'Real Estate' AND fund_age <= 2 THEN 'Commitment'
        WHEN asset_class = 'Real Estate' AND fund_age BETWEEN 3 AND 5 THEN 'Growth'
        WHEN asset_class = 'Real Estate' AND fund_age BETWEEN 6 AND 10 THEN 'Maturity'
        WHEN asset_class = 'Real Estate' AND fund_age > 10 THEN 'Distribution'

        -- Infrastructure
        WHEN asset_class = 'Infrastructure' AND fund_age <= 4 THEN 'Commitment'
        WHEN asset_class = 'Infrastructure' AND fund_age BETWEEN 5 AND 10 THEN 'Growth'
        WHEN asset_class = 'Infrastructure' AND fund_age BETWEEN 11 AND 15 THEN 'Maturity'
        WHEN asset_class = 'Infrastructure' AND fund_age > 15 THEN 'Wind-Down'

        -- Natural Resources
        WHEN asset_class = 'Natural Resources' AND fund_age <= 4 THEN 'Commitment'
        WHEN asset_class = 'Natural Resources' AND fund_age BETWEEN 5 AND 9 THEN 'Growth'
        WHEN asset_class = 'Natural Resources' AND fund_age BETWEEN 10 AND 15 THEN 'Maturity'
        WHEN asset_class = 'Natural Resources' AND fund_age > 15 THEN 'Wind-Down'

        -- Subordinated Capital & Distressed
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age <= 3 THEN 'Commitment'
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age BETWEEN 4 AND 7 THEN 'Growth'
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age BETWEEN 8 AND 12 THEN 'Maturity'
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age > 12 THEN 'Wind-Down'

        -- Fund of Funds & Secondary Funds
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age <= 3 THEN 'Commitment'
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age BETWEEN 4 AND 7 THEN 'Growth'
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age BETWEEN 8 AND 12 THEN 'Maturity'
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age > 12 THEN 'Wind-Down'

        -- Undefined for any other asset classes
        ELSE 'Undefined'
    END AS fund_stage

FROM
    merged_data
