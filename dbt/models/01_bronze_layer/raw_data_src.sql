{{  config(
    materialized = 'ephemeral',
    schema = 'TFT_RAW_DATA'
)}}

SELECT
    *
FROM
    EHDGML_RAW_DATA.RAW_DATA