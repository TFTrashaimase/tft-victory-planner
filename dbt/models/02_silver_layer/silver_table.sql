WITH bronze_data AS (
    SELECT * FROM {{ ref('raw_data_src') }}
)
SELECT
    json_data:metadata:match_id::STRING AS match_id,
    TO_TIMESTAMP(json_data:info:game_datetime::INTEGER / 1000) AS game_datetime,
    json_data:info:game_length::FLOAT AS game_length,
    json_data:info:game_version::STRING AS game_version,
    p.value:puuid::STRING AS puuid,
    p.value:placement::INTEGER AS placement,
    p.value:gold_left::INTEGER AS gold_left,
    p.value:level::INTEGER AS level,
    p.value:last_round::INTEGER AS last_round,
    p.value:total_damage_to_players::FLOAT AS total_damage_to_players,
    p.value:time_eliminated::FLOAT AS time_eliminated_seconds,
    (p.value:time_eliminated::FLOAT / 60) AS time_eliminated_minutes,
    TO_TIMESTAMP(CONCAT(TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'), ' 00:00:00')) AS created_at,
    t.value:name::STRING AS trait_name,
    t.value:num_units::INTEGER AS trait_num_units,
    t.value:style::INTEGER AS trait_style,
    t.value:tier_current::INTEGER AS trait_tier_current,
    t.value:tier_total::INTEGER AS trait_tier_total
FROM
   bronze_data,
   LATERAL FLATTEN(INPUT => json_data:info:participants) p,
   LATERAL FLATTEN(INPUT => p.value:traits) t