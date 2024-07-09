
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}
    
{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_stg_new')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

{% if table_exists %}

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.walletid,
    stg.walletnumber,
    stg.hash_column,
    stg.wallet_createdat_local,
    stg.wallet_modifiedat_local,
    stg.wallet_suspendedat_local,
    stg.wallet_unsuspendedat_local,
    stg.wallet_unregisteredat_local,
    stg.wallet_activatedat_local,
    stg.wallet_reactivatedat_local,
    stg.wallet_lasttxnts_local,
    stg.utc,
    stg.wallet_type,
    stg.wallet_status,
    stg.profileid,
    stg.partnerid,
    stg.loaddate

FROM {{ source('dbt-dimensions', 'inc_wallets_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'inc_wallets_dimension') }} dim ON stg.walletid = dim.walletid
WHERE dim.walletid IS NULL

{% else %}

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.walletid,
    stg.walletnumber,
    stg.hash_column,
    stg.wallet_createdat_local,
    stg.wallet_modifiedat_local,
    stg.wallet_suspendedat_local,
    stg.wallet_unsuspendedat_local,
    stg.wallet_unregisteredat_local,
    stg.wallet_activatedat_local,
    stg.wallet_reactivatedat_local,
    stg.wallet_lasttxnts_local,
    stg.utc,
    stg.wallet_type,
    stg.wallet_status,
    stg.profileid,
    stg.partnerid,
    stg.loaddate

FROM {{ source('dbt-dimensions', 'inc_wallets_stg') }} stg

{% endif %}