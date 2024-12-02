{{  config(
    materialized = 'ephemeral'
)}}
SELECT
    *
FROM
    TFT_RAW_DATA.RAW_DATA
