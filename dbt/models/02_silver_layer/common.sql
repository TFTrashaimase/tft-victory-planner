{{
    config(
        materialized='incremental',
        unique_key=['code_type', 'code_value'],
        merge_exclude_columns = ['created_at']
    )
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY source.code_type, source.code_value) AS code_id,
    *,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('raw_common') }} AS source
