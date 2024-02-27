-- models/wallets_dimension.sql

{{ config(materialized= 'table')}}

SELECT
    walletId,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_createdat_utc2,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_modifiedat_utc2,
    walletStatus as wallet_status,
    

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
    null::timestamptz as expdate,
    true::boolean as currentflag

FROM {{source('axis_core', 'walletdetails')}}
