{{ config(
    materialized='incremental',
    unique_key='id',
) }}

WITH bronze_data AS (
    SELECT DISTINCT
        *
    FROM
        {{ ref('raw_data_src') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY p.value:puuid) AS id,
    data:metadata:match_id::STRING AS match_id,
    p.value:puuid::STRING AS puuid,
    t.value:name::STRING AS trait_name,
    t.value:num_units::INTEGER AS trait_num_units,
    t.value:style::INTEGER AS trait_style,
    t.value:tier_current::INTEGER AS trait_tier_current,
    t.value:tier_total::INTEGER AS trait_tier_total,
    CURRENT_TIMESTAMP() AS created_at
FROM
    bronze_data,
    LATERAL FLATTEN(INPUT => data:info:participants) p,
    LATERAL FLATTEN(INPUT => p.value:traits) t
