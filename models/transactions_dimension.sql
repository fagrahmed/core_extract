-- models/transactions_dimension.sql

{{ config(materialized='table') }}

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
    

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
    null::timestamptz as expdate,
    true::boolean as currentflag

FROM {{ source('axis_core', 'transactiondetails')}}
