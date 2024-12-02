{{ config(
    materialized='table'
) }}
WITH top_players AS (
    SELECT DISTINCT MATCH_ID, PUUID
    FROM TFT_SILVER_DATA.MATCH_PLAYER_DETAILS
    WHERE WIN = TRUE -- 순방 유저만 필터링
    AND CREATED_AT = (
        SELECT MAX(CREATED_AT)
        FROM {{ ref('match_player_details') }}
    )
)
SELECT 
    LOWER(tu.NAME) AS UNIT_NAME,
    COUNT(*) AS UNIT_COUNT
FROM {{ ref('match_unit_details') }} m
JOIN {{ ref('tft_units') }} tu
    ON m.CHARACTER_ID = tu.CHARACTER_ID
JOIN top_players tp
    ON m.MATCH_ID = tp.MATCH_ID AND m.PUUID = tp.PUUID
WHERE m.CREATED_AT = (
        SELECT MAX(CREATED_AT)
        FROM {{ ref('match_unit_details') }}
    )
    AND tu.COST = 5 -- 5코스트 유닛만 필터링
GROUP BY LOWER(tu.NAME)
ORDER BY UNIT_COUNT DESC