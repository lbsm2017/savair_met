-- models/fund_stage_classification.sql

WITH xform_data AS (
    SELECT
        vintage_year,
        asset_class,
        geo_region,
        first_transaction_period,
        as_of_date,
        transaction_year_decimal,
        fund_age
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
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age <= 3.0 THEN 'Contribution'
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age > 3.0 AND fund_age <= 6.0 THEN 'Growth'
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age > 6.0 AND fund_age <= 10.0 THEN 'Distribution'
        WHEN asset_class = 'Buyout & Growth Equity' AND fund_age > 10.0 THEN 'Liquidation'

        -- Venture Capital
        WHEN asset_class = 'Venture Capital' AND fund_age <= 5.0 THEN 'Contribution'
        WHEN asset_class = 'Venture Capital' AND fund_age > 5.0 AND fund_age <= 8.0 THEN 'Growth'
        WHEN asset_class = 'Venture Capital' AND fund_age > 8.0 AND fund_age <= 12.0 THEN 'Distribution'
        WHEN asset_class = 'Venture Capital' AND fund_age > 12.0 THEN 'Liquidation'

        -- Real Estate
        WHEN asset_class = 'Real Estate' AND fund_age <= 2.0 THEN 'Contribution'
        WHEN asset_class = 'Real Estate' AND fund_age > 2.0 AND fund_age <= 5.0 THEN 'Growth'
        WHEN asset_class = 'Real Estate' AND fund_age > 5.0 AND fund_age <= 10.0 THEN 'Distribution'
        WHEN asset_class = 'Real Estate' AND fund_age > 10.0 THEN 'Liquidation'

        -- Infrastructure
        WHEN asset_class = 'Infrastructure' AND fund_age <= 4.0 THEN 'Contribution'
        WHEN asset_class = 'Infrastructure' AND fund_age > 4.0 AND fund_age <= 10.0 THEN 'Growth'
        WHEN asset_class = 'Infrastructure' AND fund_age > 10.0 AND fund_age <= 15.0 THEN 'Distribution'
        WHEN asset_class = 'Infrastructure' AND fund_age > 15.0 THEN 'Liquidation'

        -- Natural Resources
        WHEN asset_class = 'Natural Resources' AND fund_age <= 4.0 THEN 'Contribution'
        WHEN asset_class = 'Natural Resources' AND fund_age > 4.0 AND fund_age <= 9.0 THEN 'Growth'
        WHEN asset_class = 'Natural Resources' AND fund_age > 9.0 AND fund_age <= 15.0 THEN 'Distribution'
        WHEN asset_class = 'Natural Resources' AND fund_age > 15.0 THEN 'Liquidation'

        -- Subordinated Capital & Distressed
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age <= 3.0 THEN 'Contribution'
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age > 3.0 AND fund_age <= 7.0 THEN 'Growth'
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age > 7.0 AND fund_age <= 12.0 THEN 'Distribution'
        WHEN asset_class = 'Subordinated Capital & Distressed' AND fund_age > 12.0 THEN 'Liquidation'

        -- Fund of Funds & Secondary Funds
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age <= 3.0 THEN 'Contribution'
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age > 3.0 AND fund_age <= 7.0 THEN 'Growth'
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age > 7.0 AND fund_age <= 12.0 THEN 'Distribution'
        WHEN asset_class = 'Fund of Funds & Secondary Funds' AND fund_age > 12.0 THEN 'Liquidation'

        -- Undefined for any other asset classes
        ELSE 'Undefined'
    END AS fund_stage

FROM
    merged_data
