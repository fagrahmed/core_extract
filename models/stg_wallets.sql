{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}
{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'stg_wallets')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result.rows else False %}

SELECT
    'insert' AS operation,
    true AS currentflag
FROM
    {{ source('axis_core', '_airbyte_raw_walletdetails') }} src
{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE src._airbyte_emitted_at > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'wallets_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
