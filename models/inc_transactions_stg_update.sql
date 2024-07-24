{{ config(
    materialized='incremental',
    unique_key= ['txndetailsid'],
    depends_on=['inc_transactions_stg'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id,
    'update' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    stg.txndetailsid,
    stg.walletdetailsid,
    stg.clientdetails,
    stg.transaction_createdat_local,
    stg.transaction_modifiedat_local,
    stg.utc,
    stg.txntype,
    stg.transactionstatus,
    stg.transactionchannel,
    stg.transactiondomain,
    stg.transactionaction,
    stg.interchangeaction,
    stg.interchange_amount,
    stg.service_fees,
    stg.amount,
    stg.balance_before,
    stg.balance_after,
    stg.actual_balance_before,
    stg.actual_balance_after,
    stg.hasservicefees,
    stg.transactionreference,
    stg.isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate,
    stg.is_fees

FROM {{ source('dbt-dimensions', 'inc_wallets_stg') }} stg
JOIN {{ source('dbt-dimensions', 'inc_wallets_dimension')}} final
    ON stg.txndetailsid = final.txndetailsid 
WHERE stg.loaddate > final.loaddate

{% else %}

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.txndetailsid,
    stg.walletdetailsid,
    stg.clientdetails,
    stg.transaction_createdat_local,
    stg.transaction_modifiedat_local,
    stg.utc,
    stg.txntype,
    stg.transactionstatus,
    stg.transactionchannel,
    stg.transactiondomain,
    stg.transactionaction,
    stg.interchangeaction,
    stg.interchange_amount,
    stg.service_fees,
    stg.amount,
    stg.balance_before,
    stg.balance_after,
    stg.actual_balance_before,
    stg.actual_balance_after,
    stg.hasservicefees,
    stg.transactionreference,
    stg.isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate,
    stg.is_fees

FROM {{ ref('inc_transactions_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz

{% endif %}