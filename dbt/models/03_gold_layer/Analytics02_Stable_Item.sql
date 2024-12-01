{{ config(materialized='table') }}

WITH latest_top_players AS (
    SELECT DISTINCT MATCH_ID, PUUID
    FROM {{ ref('match_player_details') }}
    WHERE WIN = TRUE
      AND CREATED_AT = (
        SELECT 
            MAX(CREATED_AT) 
        FROM 
            {{ ref('match_player_details') }}
    )
),
cleaned_item_details AS (
    SELECT 
        mu.MATCH_ID, 
        mu.PUUID, 
        REGEXP_REPLACE(TRIM(VALUE), '^TFT[0-9]*_?Item_', '') AS ITEM_NAME -- TFT 접두사 제거 및 정리
    FROM {{ ref('match_unit_details') }} mu
    JOIN latest_top_players tp
        ON mu.MATCH_ID = tp.MATCH_ID AND mu.PUUID = tp.PUUID,
    LATERAL FLATTEN(INPUT => SPLIT(mu.ITEM_NAME, ',')) -- 아이템을 ',' 기준으로 분리
    WHERE mu.ITEM_NAME IS NOT NULL AND ITEM_NAME != ''
      AND mu.CREATED_AT = (SELECT MAX(CREATED_AT) FROM TFT_SILVER_DATA.MATCH_UNIT_DETAILS)
)

-- 최종 결과: 아이템별 등장 횟수 집계
SELECT 
    ITEM_NAME,
    COUNT(*) AS ITEM_COUNT
FROM cleaned_item_details
GROUP BY ITEM_NAME
ORDER BY ITEM_COUNT DESC