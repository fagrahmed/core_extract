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
    md5(
        COALESCE(_airbyte_data->>'walletid', '') || '-' || 
        COALESCE(_airbyte_data->>'walletnumber', '') || '-' || 
        COALESCE(_airbyte_data->>'lastmodified', '') || '-' || 
        (now()::timestamptz)::text
    ) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    _airbyte_data->>'walletid' AS walletid,
    _airbyte_data->>'walletnumber' AS walletnumber,
    md5(
        COALESCE(_airbyte_data->>'walletid', '') || '::' || 
        COALESCE(_airbyte_data->>'walletnumber', '') || '::' || 
        COALESCE(_airbyte_data->>'walletStatus', '') || '::' || 
        COALESCE(_airbyte_data->>'nationalid', '') || '::' || 
        COALESCE(_airbyte_data->>'firstname', '') || '::' || 
        COALESCE(_airbyte_data->>'lastname', '') || '::' || 
        COALESCE(_airbyte_data->>'clientdata', '') || '::' || 
        COALESCE(_airbyte_data->>'partnerid', '') || '::' || 
        COALESCE(_airbyte_data->>'activatedat', '') || '::' || 
        COALESCE(_airbyte_data->>'reactivatedat', '') || '::' || 
        COALESCE((_airbyte_data->>'waivedamount')::text, '') || '::' || 
        COALESCE(_airbyte_data->>'suspendedreason', '') || '::' || 
        COALESCE((_airbyte_data->>'pinsetflag')::text, '')
    ) AS hash_column,
    ((_airbyte_data->>'createdat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_createdat_utc2,
    ((_airbyte_data->>'lastmodified')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_modifiedat_utc2,
    ((_airbyte_data->>'suspendedat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_suspendedat_utc2,
    ((_airbyte_data->>'unsuspendedat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_unsuspendedat_utc2,
    ((_airbyte_data->>'unregisteredat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_unregisteredat_utc2,
    ((_airbyte_data->>'activatedat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_activatedat_utc2,
    ((_airbyte_data->>'reactivatedat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_reactivatedat_utc2,
    ((_airbyte_data->>'lasttxnts')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_lasttxnts_utc2,
    _airbyte_data->>'wallettype' AS wallet_type,
    _airbyte_data->>'walletStatus' AS wallet_status,
    _airbyte_data->>'walletprofileid' AS profileid,
    _airbyte_data->>'partnerid' AS partnerid,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate
FROM
    {{ source('axis_core', '_airbyte_raw_walletdetails') }} src

