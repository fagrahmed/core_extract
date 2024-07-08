{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    on_schema_change='append_new_columns'
)}}
    
{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}
    
{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

SELECT
    md5( COALESCE(walletid, '') || '-' || COALESCE(walletnumber, '') || '-' || COALESCE(lastmodified::text, '') || '-' || (now()::timestamptz)::text)
    AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    walletid AS walletid,
    walletnumber AS walletnumber,
    md5(
        COALESCE(walletid, '') || '::' || COALESCE(walletnumber, '') || '::' || COALESCE(walletStatus, '') || '::' ||
        COALESCE(nationalid, '') || '::' || COALESCE(firstname, '') || '::' || COALESCE(lastname, '') || '::' ||
        COALESCE(clientdata::text, '') || '::' || COALESCE(partnerid, '') || '::' || COALESCE(activatedat, '') || '::' || 
        COALESCE(reactivatedat, '') || '::' || COALESCE(waivedamount::text, '') || '::' || COALESCE(suspendedreason, '') || '::' ||
        COALESCE(pinsetflag::text, '')
    ) AS hash_column,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_createdat_utc2,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_modifiedat_utc2,
    (suspendedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_suspendedat_utc2,
    (unsuspendedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_unsuspendedat_utc2,
    (unregisteredat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_unregisteredat_utc2,
    (activatedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_activatedat_utc2,
    (reactivatedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_reactivatedat_utc2,
    (lasttxnts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_lasttxnts_utc2,
    wallettype AS wallet_type,
    walletStatus AS wallet_status,
    walletprofileid AS profileid,
    partnerid AS partnerid,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate
FROM
    {{ source('axis_core', 'walletdetails') }} src
{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE (_airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours')
            > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'inc_wallets_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
