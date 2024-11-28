{{  config(
    materialized = 'ephemeral',
    schema = 'RAW_DATA'
)}}
SELECT
    *
FROM
    TFT_RAW_DATA.RAW_DATA
