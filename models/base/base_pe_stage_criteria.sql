-- models/base/base_pe_stage_criteria.sql

WITH source_data AS (
    SELECT
        CAST(asset_class AS VARCHAR) AS asset_class,
        CAST(min_fund_age AS FLOAT) AS min_fund_age,
        CAST(max_fund_age AS FLOAT) AS max_fund_age,
        CAST(fund_stage AS VARCHAR) AS fund_stage
    FROM {{ source('postgres', 'pe_stage_criteria') }}
)

SELECT * FROM source_data
