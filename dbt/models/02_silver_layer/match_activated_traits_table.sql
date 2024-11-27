{{ config(
    materialized='incremental',
    unique_key='id',
    schema='SILVER_DATA'
) }}

WITH bronze_data AS (
    SELECT DISTINCT
        *
    FROM
        {{ ref('raw_data_src') }}
)

SELECT
    id_seq.NEXTVAL AS id,
    json_data:metadata:match_id::STRING AS match_id,
    p.value:puuid::STRING AS puuid,
    t.value:name::STRING AS trait_name,
    t.value:num_units::INTEGER AS trait_num_units,
    t.value:style::INTEGER AS trait_style,
    t.value:tier_current::INTEGER AS trait_tier_current,
    t.value:tier_total::INTEGER AS trait_tier_total,
    TO_TIMESTAMP(CONCAT(TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'), ' 00:00:00')) AS created_at
FROM
    bronze_data,
    LATERAL FLATTEN(INPUT => json_data:info:participants) p,
    LATERAL FLATTEN(INPUT => p.value:traits) t