
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'inc_wallets_stg_new') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'inc_wallets_stg_new') }};{% endif %}"
    ]
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
    stg.wallet_createdat_utc2,
    stg.wallet_modifiedat_utc2,
    stg.wallet_suspendedat_utc2,
    stg.wallet_unsuspendedat_utc2,
    stg.wallet_unregisteredat_utc2,
    stg.wallet_activatedat_utc2,
    stg.wallet_reactivatedat_utc2,
    stg.wallet_lasttxnts_utc2,
    stg.wallet_type,
    stg.wallet_status,
    stg.profileid,
    stg.partnerid,
    stg.loaddate

FROM {{ source('dbt-dimensions', 'inc_wallets_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'inc_wallets_dimension') }} dim ON stg.wallet_id = dim.wallet_id
WHERE dim.wallet_id IS NULL

{% else %}

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.walletid,
    stg.walletnumber,
    stg.hash_column,
    stg.wallet_createdat_utc2,
    stg.wallet_modifiedat_utc2,
    stg.wallet_suspendedat_utc2,
    stg.wallet_unsuspendedat_utc2,
    stg.wallet_unregisteredat_utc2,
    stg.wallet_activatedat_utc2,
    stg.wallet_reactivatedat_utc2,
    stg.wallet_lasttxnts_utc2,
    stg.wallet_type,
    stg.wallet_status,
    stg.profileid,
    stg.partnerid,
    stg.loaddate

FROM {{ source('dbt-dimensions', 'inc_wallets_stg') }} stg

{% endif %}