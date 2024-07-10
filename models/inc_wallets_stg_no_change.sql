

{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        on_schema_change='append_new_columns'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

-- Ensure dependencies are clearly defined for dbt
{% set _ = ref('inc_wallets_stg_update') %}
{% set _ = ref('inc_wallets_stg_exp') %}

{% if table_exists %}

WITH upd_exp_rec AS (

    SELECT 
		id,
		operation,
		currentflag,
		expdate,
		walletid,
		walletnumber,
		hash_column,
		wallet_createdat_local,
		wallet_modifiedat_local,
		wallet_suspendedat_local,
		wallet_unsuspendedat_local,
		wallet_unregisteredat_local,
		wallet_activatedat_local,
		wallet_reactivatedat_local,
		wallet_lasttxnts_local,
		utc,
		wallet_type,
		wallet_status,
		profileid,
		partnerid,
		loaddate
	
    FROM {{ref("inc_wallets_stg_update")}}

    UNION ALL

    SELECT 
		id,
		operation,
		currentflag,
		expdate,
		walletid,
		walletnumber,
		hash_column,
		wallet_createdat_local,
		wallet_modifiedat_local,
		wallet_suspendedat_local,
		wallet_unsuspendedat_local,
		wallet_unregisteredat_local,
		wallet_activatedat_local,
		wallet_reactivatedat_local,
		wallet_lasttxnts_local,
		utc,
		wallet_type,
		wallet_status,
		profileid,
		partnerid,
		loaddate
	
    FROM {{ref("inc_wallets_stg_exp")}}
)


, remove_old_from_dim AS (

	SELECT
		old_rec.id,
		old_rec.operation,
		old_rec.currentflag,
		old_rec.expdate,
		old_rec.walletid,
		old_rec.walletnumber,
		old_rec.hash_column,
		old_rec.wallet_createdat_local,
		old_rec.wallet_modifiedat_local,
		old_rec.wallet_suspendedat_local,
		old_rec.wallet_unsuspendedat_local,
		old_rec.wallet_unregisteredat_local,
		old_rec.wallet_activatedat_local,
		old_rec.wallet_reactivatedat_local,
		old_rec.wallet_lasttxnts_local,
		old_rec.utc,
		old_rec.wallet_type,
		old_rec.wallet_status,
		old_rec.profileid,
		old_rec.partnerid,
		old_rec.loaddate
	
	FROM {{ source('dbt-dimensions', 'inc_wallets_dimension') }} AS old_rec
	LEFT JOIN upd_exp_rec ON old_rec.id = upd_exp_rec.id
	WHERE upd_exp_rec.id IS NULL
			
)

SELECT
	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_local,
	wallet_modifiedat_local,
	wallet_suspendedat_local,
	wallet_unsuspendedat_local,
	wallet_unregisteredat_local,
	wallet_activatedat_local,
	wallet_reactivatedat_local,
	wallet_lasttxnts_local,
	utc,
	wallet_type,
	wallet_status,
	profileid,
	partnerid,
	loaddate

FROM remove_old_from_dim

{% else %}

SELECT
	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_local,
	wallet_modifiedat_local,
	wallet_suspendedat_local,
	wallet_unsuspendedat_local,
	wallet_unregisteredat_local,
	wallet_activatedat_local,
	wallet_reactivatedat_local,
	wallet_lasttxnts_local,
	utc,
	wallet_type,
	wallet_status,
	profileid,
	partnerid,
	loaddate

FROM {{source('dbt-dimensions', 'inc_wallets_dimension')}}
WHERE loaddate > '2050-01-01'::timestamptz

{% endif %}