{{
    config(
        materialized='table'
    )
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY source.code_type, source.code_value) AS code_id,
    source.*,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM
    {{ ref('raw_common') }} AS source
