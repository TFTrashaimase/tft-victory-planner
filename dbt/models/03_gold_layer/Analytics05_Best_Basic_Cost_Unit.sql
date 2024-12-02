{{ config(
    materialized='table'
) }}
WITH top_players AS (
    SELECT DISTINCT MATCH_ID, PUUID
    FROM {{ ref('match_player_details') }}
    WHERE WIN = TRUE -- 순방 유저만 필터링
    AND CREATED_AT = (
        SELECT MAX(CREATED_AT)
        FROM {{ ref('match_player_details') }}
    )
)
SELECT
    tu.name,
    tu.cost,
    COUNT(*) AS COUNTING
FROM {{ ref('match_unit_details') }} ud
JOIN top_players tp
    ON ud.match_id = tp.match_id AND ud.puuid = tp.puuid
		JOIN {{ ref('tft_units') }} tu
    ON ud.CHARACTER_ID = tu.CHARACTER_ID
WHERE ud.UNIT_TIER = 3
GROUP BY tu.name, tu.cost
ORDER BY COST DESC, COUNTING DESC