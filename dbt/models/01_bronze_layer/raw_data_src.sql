{{  config(
    materialized = 'ephemeral',
    schema = 'RAW_DATA'
)}}

SELECT
    *
FROM
    EHDGML_RAW_DATA.RAW_DATA