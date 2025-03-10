{{ config(
    materialized='table'
) }}

WITH bronze_data AS (
    SELECT DISTINCT
        *
    FROM
        {{ ref('raw_data_src') }}
)
SELECT
    -- JSON 데이터에서 추출
    data:metadata:match_id::STRING AS match_id,
    p.value:puuid::STRING AS puuid,
    p.value:gold_left::INTEGER AS gold_left,
    p.value:last_round::INTEGER AS last_round,
    p.value:level::INTEGER AS level,
    p.value:placement::INTEGER AS placement,
    p.value:players_eliminated::INTEGER AS players_eliminated,
    p.value:riotIdGameName::STRING AS riot_id_game_name,
    p.value:riotIdTagline::STRING AS riot_id_tagline,
    p.value:time_eliminated::FLOAT AS time_eliminated,
    p.value:total_damage_to_players::INTEGER AS total_damage_to_players,
    p.value:win AS win,
    CURRENT_TIMESTAMP() AS created_at
FROM bronze_data,
LATERAL FLATTEN(INPUT => data:info:participants) p,
LATERAL FLATTEN(INPUT => p.value:traits) t
