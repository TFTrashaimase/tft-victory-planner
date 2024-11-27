{{ config(
    materialized='table',
    schema='SILVER_DATA'
) }}

WITH bronze_data AS (
    SELECT * FROM {{ ref('raw_data_src') }}
)
SELECT
    -- JSON 데이터에서 추출
    JSON_DATA:metadata:match_id::STRING AS match_id,
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

    -- 승리 여부 계산
    CASE 
        WHEN p.value:placement = 1 THEN TRUE
        ELSE FALSE
    END AS win,

    -- 생성 시간 삽입
    CURRENT_TIMESTAMP() AS created_at
FROM bronze_data,
LATERAL FLATTEN(INPUT => json_data:info:participants) p,
LATERAL FLATTEN(INPUT => p.value:traits) t