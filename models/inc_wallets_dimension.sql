

{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        on_schema_change='fail'
    )
}}

with step_1 as (
    select *
    from {{ ref("inc_wallets_stg_new") }} 

    union 

    select *
    from {{ref("inc_wallets_stg_update")}}

    union 

    select *
    from {{ref("inc_wallets_stg_exp")}}
)

{%if is_incremental() %}
/* Exclude records that already exist in the destination table to remove old entries for update and exp*/
, step_2 as(
    select 
        new_records.*
    from step_1 as new_records
    left join {{this}} as old_records
        on new_records.id = old_records.id
    where old_records.id is null

)
{% endif %}

select *
from {% if is_incremental() %} step_2 {% else %} step_1 {% endif %}
order by loaddate
