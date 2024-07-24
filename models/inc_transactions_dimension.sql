{{
    config(
        materialized="incremental",
        unique_key= ["txndetailsid"],
        on_schema_change='append_new_columns',
	    incremental_strategy = 'merge'
	)
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

-- Ensure dependencies are clearly defined for dbt
{% set _ = ref('inc_transactions_stg_update') %}
{% set _ = ref('inc_transactions_stg_new') %}
{% set _ = ref('inc_transactions_stg') %}

SELECT
    id,
    operation,
    currentflag,
    expdate,
    txndetailsid,
    walletdetailsid,
    clientdetails,
    transaction_createdat_local,
    transaction_modifiedat_local,
    utc,
    txntype,
    transactionstatus,
    transactionchannel,
    transactiondomain,
    transactionaction,
    interchangeaction,
    interchange_amount,
    service_fees,
    amount,
    balance_before,
    balance_after,
    actual_balance_before,
    actual_balance_after,
    hasservicefees,
    transactionreference,
    isreversedflag,
    loaddate,
    is_fees

FROM {{ source('dbt-dimensions', 'inc_transactions_stg_update') }}

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    txndetailsid,
    walletdetailsid,
    clientdetails,
    transaction_createdat_local,
    transaction_modifiedat_local,
    utc,
    txntype,
    transactionstatus,
    transactionchannel,
    transactiondomain,
    transactionaction,
    interchangeaction,
    interchange_amount,
    service_fees,
    amount,
    balance_before,
    balance_after,
    actual_balance_before,
    actual_balance_after,
    hasservicefees,
    transactionreference,
    isreversedflag,
    loaddate,
    is_fees

FROM {{ source('dbt-dimensions', 'inc_transactions_stg_new') }}

{% endif %}