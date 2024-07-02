{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns'
)}}



SELECT
    'insert' AS operation,
    true AS currentflag
FROM
    {{ source('axis_core', '_airbyte_raw_walletdetails') }} src

