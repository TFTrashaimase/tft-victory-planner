{{  config(
    materialized = 'ephemeral'
)}}
SELECT
    *
FROM
    TFT_RAW_DATA.raw_champion_meta
