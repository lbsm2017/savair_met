-- models/base/base_pe_returns.sql

WITH source_data AS (
    SELECT
        CAST(vintage_year AS INT) AS vintage_year,
        CAST(asset_class AS VARCHAR) AS asset_class,
        CAST(geo_region AS VARCHAR) AS geo_region,
        CAST(first_transaction_period AS VARCHAR) AS first_transaction_period,
        CAST(as_of_date AS DATE) AS as_of_date,
        CAST(fund_count AS FLOAT) AS fund_count,
        CAST(irr_pooled AS FLOAT) AS irr_pooled,
        CAST(irr_equal_weighted AS FLOAT) AS irr_equal_weighted,
        CAST(irr_capital_weighted AS FLOAT) AS irr_capital_weighted,
        CAST(irr_average AS FLOAT) AS irr_average,
        CAST(irr_top_5 AS FLOAT) AS irr_top_5,
        CAST(irr_upper_quartile AS FLOAT) AS irr_upper_quartile,
        CAST(irr_median AS FLOAT) AS irr_median,
        CAST(irr_lower_quartile AS FLOAT) AS irr_lower_quartile,
        CAST(irr_bottom_5 AS FLOAT) AS irr_bottom_5,
        CAST(irr_std_dev AS FLOAT) AS irr_std_dev,
        CAST(tvpi_pooled AS FLOAT) AS tvpi_pooled,
        CAST(tvpi_capital_weighted AS FLOAT) AS tvpi_capital_weighted,
        CAST(tvpi_average AS FLOAT) AS tvpi_average,
        CAST(tvpi_top_5 AS FLOAT) AS tvpi_top_5,
        CAST(tvpi_upper_quartile AS FLOAT) AS tvpi_upper_quartile,
        CAST(tvpi_median AS FLOAT) AS tvpi_median,
        CAST(tvpi_lower_quartile AS FLOAT) AS tvpi_lower_quartile,
        CAST(tvpi_bottom_5 AS FLOAT) AS tvpi_bottom_5,
        CAST(tvpi_std_dev AS FLOAT) AS tvpi_std_dev,
        CAST(dpi_pooled AS FLOAT) AS dpi_pooled,
        CAST(dpi_capital_weighted AS FLOAT) AS dpi_capital_weighted,
        CAST(dpi_average AS FLOAT) AS dpi_average,
        CAST(dpi_top_5 AS FLOAT) AS dpi_top_5,
        CAST(dpi_upper_quartile AS FLOAT) AS dpi_upper_quartile,
        CAST(dpi_median AS FLOAT) AS dpi_median,
        CAST(dpi_lower_quartile AS FLOAT) AS dpi_lower_quartile,
        CAST(dpi_bottom_5 AS FLOAT) AS dpi_bottom_5,
        CAST(dpi_std_dev AS FLOAT) AS dpi_std_dev
    FROM {{ source('postgres', 'pe_returns') }}
)

SELECT * FROM source_data
