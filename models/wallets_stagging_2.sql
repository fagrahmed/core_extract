
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    on_schema_change='create'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

with update_old as (
    SELECT
        stg.unique_id AS unique_id,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column AND final.operation = 'insert' THEN 'update'
            ELSE 'exp'
        END AS operation,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN true
            ELSE false
        END AS currentflag,
        CASE
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN null::timestamptz
            ELSE now()::timestamptz
        END AS expdate,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.walletid
            ELSE final.walletid
        END AS walletid,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.walletnumber
            ELSE final.walletnumber
        END AS walletnumber,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.hash_column
            ELSE final.hash_column
        END AS hash_column,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_createdat_utc2
            ELSE final.wallet_createdat_utc2
        END AS wallet_createdat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_modifiedat_utc2
            ELSE final.wallet_modifiedat_utc2
        END AS wallet_modifiedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_suspendedat_utc2
            ELSE final.wallet_suspendedat_utc2
        END AS wallet_suspendedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_unsuspendedat_utc2
            ELSE final.wallet_unsuspendedat_utc2
        END AS wallet_unsuspendedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_unregisteredat_utc2
            ELSE final.wallet_unregisteredat_utc2
        END AS wallet_unregisteredat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_activatedat_utc2
            ELSE final.wallet_activatedat_utc2
        END AS wallet_activatedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_reactivatedat_utc2
            ELSE final.wallet_reactivatedat_utc2
        END AS wallet_reactivatedat_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_lasttxnts_utc2
            ELSE final.wallet_lasttxnts_utc2
        END AS wallet_lasttxnts_utc2,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_type
            ELSE final.wallet_type
        END AS wallet_type,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.wallet_status
            ELSE final.wallet_status
        END AS wallet_status,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.profileid
            ELSE final.profileid
        END AS profileid,
        CASE 
            WHEN final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column THEN stg.partnerid
            ELSE final.partnerid
        END AS partnerid,
        (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate  

    FROM {{ source('dbt-dimensions', 'wallets_stagging') }} stg
    JOIN {{ source('dbt-dimensions', 'wallets_dimension')}} final
        ON stg.walletid = final.walletid AND stg.walletnumber = final.walletnumber
    WHERE final.hash_column is not null and final.operation != 'exp'
)

SELECT * from update_old

{% else %}

SELECT

    stg.unique_id AS unique_id,
    stg.operation AS operation,
    stg.currentflag AS currentflag,
    stg.expdate AS expdate,
    stg.walletid AS walletid,
    stg.walletnumber AS walletnumber,
    stg.hash_column AS hash_column,
    stg.wallet_createdat_utc2 AS wallet_createdat_utc2,
    stg.wallet_modifiedat_utc2 AS wallet_modifiedat_utc2,
    stg.wallet_suspendedat_utc2 AS wallet_suspendedat_utc2,
    stg.wallet_unsuspendedat_utc2 AS wallet_unsuspendedat_utc2,
    stg.wallet_unregisteredat_utc2 AS wallet_unregisteredat_utc2,
    stg.wallet_activatedat_utc2 AS wallet_activatedat_utc2,
    stg.wallet_reactivatedat_utc2 AS wallet_reactivatedat_utc2,
    stg.wallet_lasttxnts_utc2 AS wallet_lasttxnts_utc2,
    stg.wallet_type AS wallet_type,
    stg.wallet_status AS wallet_status,
    stg.profileid AS profileid,
    stg.partnerid AS partnerid,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate

FROM {{ source('dbt-dimensions', 'wallets_stagging') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz
{% endif %}

