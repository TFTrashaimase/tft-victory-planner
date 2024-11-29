{{ config(
    materialized='incremental',
    unique_key='match_id',
    schema='SILVER_DATA'
    )
}}

WITH bronze_data AS (
    SELECT DISTINCT
        * 
    FROM
        {{ ref('raw_data_src') }}
)
SELECT
    data:metadata:match_id::STRING AS match_id,
    data:info:tft_set_number::INTEGER AS tft_set_number,
    TO_TIMESTAMP(data:info:game_datetime::INTEGER / 1000) AS game_datetime,
    SUBSTRING(data:info:tft_game_type, 1, LENGTH(data:info:tft_game_type)) AS tft_game_type,
    SUBSTRING(data:info:tft_set_core_name, 1, LENGTH(data:info:tft_set_core_name)) AS tft_set_core_name,
    CURRENT_TIMESTAMP() AS created_at
FROM bronze_data
