{{ config(
    materialized='table'
) }}
WITH ITEM_COUNT AS (
    SELECT 
        MATCH_ID,
        CHARACTER_ID,
        TRIM(VALUE) AS ITEM_NAME_CLEANED
    FROM {{ ref('match_unit_details') }}, 
    LATERAL FLATTEN(INPUT => SPLIT(ITEM_NAME, ',')) AS f
    WHERE ITEM_NAME IS NOT NULL 
      AND TRIM(ITEM_NAME) <> ''
    AND created_at = (
        SELECT MAX(created_at)
        FROM {{ ref('match_unit_details') }}
    )
)
SELECT
    u.NAME AS CHARACTER_NAME,  -- TFT_UNITS 테이블에서 NAME 가져오기
    SPLIT_PART(ITEM_NAME_CLEANED, '_', ARRAY_SIZE(SPLIT(ITEM_NAME_CLEANED, '_'))) AS ITEM_NAME,
    COUNT(ITEM_NAME_CLEANED) AS ITEM_COUNTING
FROM
    ITEM_COUNT ic
LEFT JOIN {{ ref('tft_units') }} u  -- TFT_UNITS 테이블과 조인
    ON ic.CHARACTER_ID = u.CHARACTER_ID  -- CHARACTER_ID를 기준으로 조인
GROUP BY u.NAME, ITEM_NAME_CLEANED
ORDER BY u.NAME, ITEM_COUNTING DESC