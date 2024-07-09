
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    depends_on=['inc_wallets_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'inc_wallets_stg_exp') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'inc_wallets_stg_exp') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}


SELECT
    final.id AS id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS expdate,
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
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate  

FROM {{ source('dbt-dimensions', 'inc_wallets_stg') }} stg
JOIN {{ source('dbt-dimensions', 'inc_wallets_dimension')}} final
    ON stg.walletid = final.walletid AND stg.walletnumber = final.walletnumber
WHERE final.operation = 'update' AND stg.loaddate > final.loaddate 
        AND stg.hash_column != final.hash_column


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

FROM {{ ref('inc_wallets_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz 
{% endif %}
