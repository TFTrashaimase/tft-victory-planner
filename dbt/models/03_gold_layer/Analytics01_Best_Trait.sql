{{ config(
    materialized='table'
) }}
WITH top_match_details AS (
    SELECT DISTINCT MATCH_ID, PUUID, TOTAL_DAMAGE_TO_PLAYERS
    FROM TFT_SILVER_DATA.MATCH_PLAYER_DETAILS
    WHERE created_at = (
        SELECT MAX(created_at)
        FROM {{ ref('match_player_details') }}
    )
    ORDER BY TOTAL_DAMAGE_TO_PLAYERS DESC
    LIMIT 3
)
SELECT 
    t.MATCH_ID, 
    t.PUUID, 
    REGEXP_REPLACE(t.TRAIT_NAME, '^TFT13_', '') AS TRAIT_NAME_CLEANED, -- 접두사 제거
    t.TRAIT_TIER_CURRENT, 
    d.TOTAL_DAMAGE_TO_PLAYERS
FROM {{ ref('match_activated_traits_table') }} t
JOIN top_match_details d
    ON t.MATCH_ID = d.MATCH_ID AND t.PUUID = d.PUUID
WHERE t.TRAIT_TIER_CURRENT NOT IN (0, 4) -- 0과 4단계 제외
ORDER BY d.TOTAL_DAMAGE_TO_PLAYERS DESC, t.MATCH_ID, t.PUUID ASC, t.TRAIT_TIER_CURRENT DESC