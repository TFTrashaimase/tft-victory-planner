{{  config(
    materialized = 'ephemeral', 
    schema = 'RAW_DATA'
)}}
SELECT
    *
FROM
    TFT_RAW_DATA.raw_champion_meta
