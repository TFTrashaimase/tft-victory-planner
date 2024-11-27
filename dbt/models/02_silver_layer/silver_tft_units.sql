{{ config(
    materialized='table',
    schema='DONGHEE_SILVER_DATA'
) }}

WITH bronze_tft_champion_data AS (
    SELECT * FROM {{ ref('bronze_tft_champion_data_src') }}
),
flattened_data AS (
    SELECT
        f1.value:id AS id,
        f1.value:name::string AS name,
        f1.value:tier::INTEGER AS cost,
        CURRENT_TIMESTAMP() AS created_at
    FROM bronze_tft_champion_data,
    LATERAL FLATTEN(input => bronze_tft_champion_data.json_data:data) AS f1
)
SELECT *
FROM flattened_data
WHERE id LIKE '%TFT13_%'