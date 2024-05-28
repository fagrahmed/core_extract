

{{
    config(
        materialized="incremental",
        unique_key= "hash_column",
        post_hook="
            DROP TABLE IF EXISTS wallets_stagging;
            DROP TABLE IF EXISTS wallets_stagging_2;
            "
    )
}}

with step_1 as (
    select stg.*
    from {{ ref("wallets_stagging") }} stg 
    left join {{ ref("wallets_stagging_2")}} stg2 on stg.hash_column = stg2.hash_column
    where stg2.hash_column is null

    union 

    select *
    from {{ref("wallets_stagging_2")}}
)

{%if is_incremental() %}
/* Exclude records that already exist in the destination table */
, step_2 as(
    select 
        new_records.*
    from step_1 as new_records
    left join {{this}} as old_records
        on new_records.unique_id = old_records.unique_id
    where old_records.unique_id is null

)
{% endif %}

select *
from {% if is_incremental() %} step_2 {% else %} step_1 {% endif %}
order by loaddate