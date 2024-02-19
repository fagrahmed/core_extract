-- models/transactions_dimension.sql

{{ config(materialized='table') }}

SELECT
    txndetailsid,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_createdat_utc2,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_modifiedat_utc2,
    txntype,
    transactionstatus,
    transactionchannel,
    transactiondomain,
    transactionaction,
    interchangeaction,

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
    null::timestamptz as expdate,
    true::boolean as currentflag

FROM {{ source('axis_core', 'transactiondetails')}}


-- {{ config(
--     materialized='incremental',
--     unique_key='txndetailsid',
--     strategy='timestamp',
--     check_cols=['transaction_modifiedat_utc2']
-- ) }}

-- WITH source_data as (
--     SELECT
--         txndetailsid,
--         (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_createdat_utc2,
--         (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_modifiedat_utc2,
--         txntype,
--         transactionstatus,
--         transactionchannel,
--         transactiondomain,
--         transactionaction,
--         interchangeaction,
--         (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
--         null::timestamptz as expdate,
--         true::boolean as currentflag
--     FROM {{ source('axis_core', 'transactiondetails')}}
-- )

-- MERGE INTO {{ this }} AS target
-- USING source_data AS source
-- ON target.txndetailsid = source.txndetailsid
-- WHEN MATCHED AND target.transaction_modifiedat_utc2 < source.transaction_modifiedat_utc2 THEN
--     UPDATE SET
--         expdate = now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours',
--         currentflag = false
-- WHEN NOT MATCHED THEN
--     INSERT (
--         txndetailsid,
--         transaction_createdat_utc2,
--         transaction_modifiedat_utc2,
--         txntype,
--         transactionstatus,
--         transactionchannel,
--         transactiondomain,
--         transactionaction,
--         interchangeaction,
--         loaddate,
--         expdate,
--         currentflag
--     ) VALUES (
--         source.txndetailsid,
--         source.transaction_createdat_utc2,
--         source.transaction_modifiedat_utc2,
--         source.txntype,
--         source.transactionstatus,
--         source.transactionchannel,
--         source.transactiondomain,
--         source.transactionaction,
--         source.interchangeaction,
--         source.loaddate,
--         source.expdate,
--         source.currentflag
--     );
