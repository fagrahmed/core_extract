-- models/wallets_dimension.sql

{{ config(materialized= 'table')}}

SELECT
    id SERIAL PRIMARY KEY,
    walletId,
    walletnumber,
    
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_createdat_utc2,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_modifiedat_utc2,
    (suspendedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_suspendedat_utc2,
    (unsuspendedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_unsuspendedat_utc2,
    (unregisteredat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_unregisteredat_utc2,
    (activatedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_activatedat_utc2,
    (reactivatedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_reactivatedat_utc2,
    (lasttxnts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS wallet_lasttxnts_utc2,
    
    wallettype as wallet_type,
    walletStatus as wallet_status,
    walletprofileid as profileid,
    partnerid,
    

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') as loaddate,
    null::timestamptz as expdate,
    true::boolean as currentflag

FROM {{source('axis_core', 'walletdetails')}}
