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
    REGEXP_REPLACE(mat.TRAIT_NAME, '^TFT[0-9]*_', '') AS TRAIT_NAME, -- TFT_ 접두사 제거
    mat.TRAIT_TIER_CURRENT AS CURRENT_TIER, -- 시너지 단계
    COUNT(*) AS COUNT -- 시너지 등장 횟수
FROM {{ ref('match_activated_traits_table') }} mat
JOIN top_players tp
    ON mat.MATCH_ID = tp.MATCH_ID AND mat.PUUID = tp.PUUID
WHERE mat.CREATED_AT = (
        SELECT MAX(CREATED_AT)
        FROM {{ ref('match_activated_traits_table') }}
    )
  AND mat.TRAIT_TIER_CURRENT NOT IN (0, 4) -- 시너지 단계 0, 4 제외
GROUP BY REGEXP_REPLACE(mat.TRAIT_NAME, '^TFT[0-9]*_', ''), mat.TRAIT_TIER_CURRENT
ORDER BY COUNT DESC