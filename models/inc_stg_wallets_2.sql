
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    depends_on=['inc_stg_wallets'],
    on_schema_change='create'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

with update_old as (
    SELECT
        stg.id AS id,
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

    FROM {{ source('dbt-dimensions', 'inc_stg_wallets') }} stg
    JOIN {{ source('dbt-dimensions', 'inc_wallets_dimension')}} final
        ON stg.walletid = final.walletid AND stg.walletnumber = final.walletnumber
    WHERE final.hash_column is not null and final.operation != 'exp'
        AND stg.loaddate > final.loaddate
)

SELECT * from update_old

{% else %}

SELECT *
FROM {{ ref('inc_stg_wallets') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz
{% endif %}
