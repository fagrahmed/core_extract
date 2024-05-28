-- models/inc_transactions.sql

{{
    config(
        materialized='incremental',
        unique_key='loaddate',
        on_schema_change='fail'
    )
}}

SELECT
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
    interchangeamount::float AS interchange_amount,
    servicefees_aibyte_transform::float AS service_fees,
    txnrequestedamount_aibyte_transform::float AS amount,
    walletbalancebefore_aibyte_transform::float AS balance_before,
    walletbalanceafter_aibyte_transform::float AS balance_after,
    walletactualbalancebefore_aibyte_transform AS actual_balance_before,
    walletactualbalanceafter_aibyte_transform AS actual_balance_after,
    hasservicefees,
    transactionreference,
    isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate

FROM {{ source('axis_core', 'transactiondetails')}}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run

WHERE lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours' >= (SELECT max(loaddate::timestamptz) FROM {{ this }})

{% endif %}

group by lastmodified, txndetailsid, walletdetailsid, clientdetails, transaction_createdat_utc2, transaction_modifiedat_utc2, txntype, 
transactionstatus, transactionchannel, transactiondomain, transactionaction, interchangeaction, interchangeamount, service_fees, 
amount, balance_before, balance_after, actual_balance_before, actual_balance_after, hasservicefees, transactionreference, isreversedflag,
loaddate