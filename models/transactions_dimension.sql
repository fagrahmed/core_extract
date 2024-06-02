-- models/transactions_dimension.sql


{{ config(materialized='incremental',
    unique_key= ['txndetailsid'],
    on_schema_change='create',) }}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}


SELECT
    md5(txndetailsid || '-' || now()::text) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    txndetailsid,
    walletdetailsid,
    clientdetails,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_createdat_utc2,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_modifiedat_utc2,
    txntype,
    transactionstatus,
    transactionchannel,
    transactiondomain,
    transactionaction,
    interchangeaction,
    interchangeamount::float as interchange_amount,
    servicefees_aibyte_transform::float as service_fees,
    txnrequestedamount_aibyte_transform::float as amount,
    walletbalancebefore_aibyte_transform::float as balance_before,
    walletbalanceafter_aibyte_transform::float as balance_after,
    walletactualbalancebefore_aibyte_transform as actual_balance_before,
    walletactualbalanceafter_aibyte_transform as actual_balance_after,
    hasservicefees,
    transactionreference,
    isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate


FROM {{ source('axis_core', 'transactiondetails')}} src

{% if is_incremental() and table_exists %}
    WHERE src._airbyte_emitted_at > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'transactions_dimension') }}), '1900-01-01'::timestamp)
{% endif %}