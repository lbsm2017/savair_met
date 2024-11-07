-- models/base/base_capital_call_schedule.sql

WITH source_data AS (
    SELECT
        CAST(asset_class AS VARCHAR) AS asset_class,
        CAST(geo_region AS VARCHAR) AS geo_region,
        CAST(call_year AS INT) AS call_year,
        CAST(call_value AS FLOAT) AS call_value
    FROM {{ source('postgres', 'pe_capital_call_schedule') }}
)

SELECT * FROM source_data
