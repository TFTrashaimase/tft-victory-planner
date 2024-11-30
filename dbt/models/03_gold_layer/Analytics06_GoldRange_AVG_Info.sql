{{ config(
    materialized='table'
) }}
SELECT 
    CASE 
        WHEN GOLD_LEFT BETWEEN 0 AND 9 THEN '0-9'
        WHEN GOLD_LEFT BETWEEN 10 AND 19 THEN '10-19'
        WHEN GOLD_LEFT BETWEEN 20 AND 29 THEN '20-29'
        WHEN GOLD_LEFT BETWEEN 30 AND 39 THEN '30-39'
        WHEN GOLD_LEFT BETWEEN 40 AND 49 THEN '40-49'
        WHEN GOLD_LEFT BETWEEN 50 AND 59 THEN '50-59'
        WHEN GOLD_LEFT BETWEEN 60 AND 69 THEN '60-69'
        WHEN GOLD_LEFT BETWEEN 70 AND 79 THEN '70-79'
        WHEN GOLD_LEFT BETWEEN 80 AND 89 THEN '80-89'
        WHEN GOLD_LEFT BETWEEN 90 AND 99 THEN '90-99'
        ELSE '100+'
    END AS gold_range,
    ROUND(AVG(PLACEMENT), 1) AS avg_placement,         -- 소수점 첫째 자리까지
    CAST(AVG(time_eliminated) AS INT) AS avg_time_eliminated -- 정수로 변환
FROM {{ ref('match_player_details') }}
GROUP BY gold_range
ORDER BY 
    CASE 
        WHEN gold_range = '0-9' THEN 1
        WHEN gold_range = '10-19' THEN 2
        WHEN gold_range = '20-29' THEN 3
        WHEN gold_range = '30-39' THEN 4
        WHEN gold_range = '40-49' THEN 5
        WHEN gold_range = '50-59' THEN 6
        WHEN gold_range = '60-69' THEN 7
        WHEN gold_range = '70-79' THEN 8
        WHEN gold_range = '80-89' THEN 9
        WHEN gold_range = '90-99' THEN 10
        ELSE 11
    END