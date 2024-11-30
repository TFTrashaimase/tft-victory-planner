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
    u.NAME AS CHARACTER_NAME,   -- Adding the character name from TFT_UNITS
    u.COST AS CHARACTER_COST,    -- Adding the cost from TFT_UNITS
    d.UNIT_TIER, 
    t.TOTAL_DAMAGE_TO_PLAYERS
FROM {{ ref('match_unit_details') }} d
JOIN top_match_details t
    ON t.MATCH_ID = d.MATCH_ID 
    AND t.PUUID = d.PUUID
JOIN {{ ref('tft_units') }} u
    ON LOWER(d.CHARACTER_ID) = LOWER(u.CHARACTER_ID)  -- Case-insensitive join
ORDER BY t.TOTAL_DAMAGE_TO_PLAYERS DESC, CHARACTER_COST DESC, d.UNIT_TIER DESC