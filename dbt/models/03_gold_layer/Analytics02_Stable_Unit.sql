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
    LOWER(tu.NAME) AS UNIT_NAME, -- 유닛 이름 (소문자로 변환)
    COUNT(*) AS UNIT_COUNT       -- 유닛 등장 횟수
FROM {{ ref('match_unit_details') }} m
JOIN TFT_SILVER_DATA.TFT_UNITS tu
    ON m.CHARACTER_ID = tu.CHARACTER_ID  -- CHARACTER_ID와 ID 매칭
JOIN top_players tp
    ON m.MATCH_ID = tp.MATCH_ID AND m.PUUID = tp.PUUID
WHERE m.CREATED_AT = (
        SELECT MAX(CREATED_AT)
        FROM {{ ref('match_unit_details') }}
    )
GROUP BY LOWER(tu.NAME)
ORDER BY UNIT_COUNT DESC