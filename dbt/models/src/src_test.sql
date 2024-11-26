{{
    config(
        materialized = 'view'
    )
}}
WITH src_test AS(
    SELECT * FROM TFTPROJECT_TEAM2.RAW_DATA.WAGE_AGE_YEAR2
)
SELECT
    AGE,
    YEAR
FROM
    src_test