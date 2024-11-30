{{ config(
    materialized='table'
) }}
WITH RECENT_DATA AS (
    SELECT 
        CHARACTER_ID,
        COUNT(*) AS CHARACTER_COUNT
    FROM (
        SELECT DISTINCT
            *
        FROM {{ ref('match_unit_details') }}
        WHERE CREATED_AT = (
                SELECT MAX(CREATED_AT)
                FROM {{ ref('match_unit_details') }}
            )
    ) AS FILTERED_DATA
    GROUP BY CHARACTER_ID
)
SELECT 
    TU.NAME,
    TU.COST,
    RD.CHARACTER_COUNT
FROM RECENT_DATA RD
LEFT JOIN {{ ref('tft_units') }} TU
ON RD.CHARACTER_ID = TU.CHARACTER_ID
WHERE TU.NAME IS NOT NULL AND TU.COST IS NOT NULL
ORDER BY COST, CHARACTER_COUNT DESC