-- models/base/base_capital_call_schedule.sql

WITH source_data AS (
    SELECT
        CAST(asset_class AS VARCHAR) AS asset_class,
        CAST(geo_area AS VARCHAR) AS geo_area,
        CAST(call_year AS INT) AS call_year,
        CAST(call_value AS FLOAT) AS call_value
    FROM {{ source('postgres', 'capital_call_schedule') }}
)

SELECT * FROM source_data
