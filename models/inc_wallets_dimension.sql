

{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        on_schema_change='fail'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

with upd_exp_rec as (

    select 
		id,
		operation,
		currentflag,
		expdate,
		walletid,
		walletnumber,
		hash_column,
		wallet_createdat_utc2,
		wallet_modifiedat_utc2,
		wallet_suspendedat_utc2,
		wallet_unsuspendedat_utc2,
		wallet_unregisteredat_utc2,
		wallet_activatedat_utc2,
		wallet_reactivatedat_utc2,
		wallet_lasttxnts_utc2,
		wallet_type,
		wallet_status,
		profileid,
		partnerid,
		loaddate
	
    from {{ref("inc_wallets_stg_update")}}

    union all

    select 
		id,
		operation,
		currentflag,
		expdate,
		walletid,
		walletnumber,
		hash_column,
		wallet_createdat_utc2,
		wallet_modifiedat_utc2,
		wallet_suspendedat_utc2,
		wallet_unsuspendedat_utc2,
		wallet_unregisteredat_utc2,
		wallet_activatedat_utc2,
		wallet_reactivatedat_utc2,
		wallet_lasttxnts_utc2,
		wallet_type,
		wallet_status,
		profileid,
		partnerid,
		loaddate
	
    from {{ref("inc_wallets_stg_exp")}}
)

{% if table_exists %}
, remove_old_from_dim as (
	select 
		old_rec.id,
		old_rec.operation,
		old_rec.currentflag,
		old_rec.expdate,
		old_rec.walletid,
		old_rec.walletnumber,
		old_rec.hash_column,
		old_rec.wallet_createdat_utc2,
		old_rec.wallet_modifiedat_utc2,
		old_rec.wallet_suspendedat_utc2,
		old_rec.wallet_unsuspendedat_utc2,
		old_rec.wallet_unregisteredat_utc2,
		old_rec.wallet_activatedat_utc2,
		old_rec.wallet_reactivatedat_utc2,
		old_rec.wallet_lasttxnts_utc2,
		old_rec.wallet_type,
		old_rec.wallet_status,
		old_rec.profileid,
		old_rec.partnerid,
		old_rec.loaddate
	
	from {{ ref("inc_wallets_dimension")}} as old_rec
	left join upd_exp_rec on old_rec.id = upd_exp_rec.id
	where upd_exp_rec.id is null 
			
)

select 
	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_utc2,
	wallet_modifiedat_utc2,
	wallet_suspendedat_utc2,
	wallet_unsuspendedat_utc2,
	wallet_unregisteredat_utc2,
	wallet_activatedat_utc2,
	wallet_reactivatedat_utc2,
	wallet_lasttxnts_utc2,
	wallet_type,
	wallet_status,
	profileid,
	partnerid,
	loaddate

from remove_old_from_dim

union all

select 
	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_utc2,
	wallet_modifiedat_utc2,
	wallet_suspendedat_utc2,
	wallet_unsuspendedat_utc2,
	wallet_unregisteredat_utc2,
	wallet_activatedat_utc2,
	wallet_reactivatedat_utc2,
	wallet_lasttxnts_utc2,
	wallet_type,
	wallet_status,
	profileid,
	partnerid,
	loaddate
from upd_exp_rec

union all

{% endif %}

select 
	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_utc2,
	wallet_modifiedat_utc2,
	wallet_suspendedat_utc2,
	wallet_unsuspendedat_utc2,
	wallet_unregisteredat_utc2,
	wallet_activatedat_utc2,
	wallet_reactivatedat_utc2,
	wallet_lasttxnts_utc2,
	wallet_type,
	wallet_status,
	profileid,
	partnerid,
	loaddate

from {{ref("inc_wallets_stg_new")}}
