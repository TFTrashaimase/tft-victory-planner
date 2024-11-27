{{ config(
    materialized='incremental',
    unique_key='id',
    schema='SILVER_DATA'
) }}

WITH bronze_data AS (
    SELECT DISTINCT * 
    FROM {{ ref('raw_data_src') }}
),
flattened_participants AS (
    SELECT
        id_item.NEXTVAL AS id,
        json_data:metadata:match_id::STRING AS match_id,
        p.value:puuid::STRING AS puuid,
        u.value:character_id::STRING AS character_id,
        ARRAY_TO_STRING(u.value:itemNames, ', ') AS item_name, -- 배열을 문자열로 변환
        u.value:name::STRING AS unit_name,
        CAST(u.value:rarity::INTEGER AS NUMBER) AS rarity,
        CAST(u.value:tier::INTEGER AS NUMBER) AS unit_tier,
        CURRENT_TIMESTAMP() AS created_at
    FROM
        bronze_data,
        LATERAL FLATTEN(INPUT => json_data:info:participants) p,
        LATERAL FLATTEN(INPUT => p.value:units) u
)
SELECT *
FROM flattened_participants