{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='arrest_key',
        pre_hook="""
        delete from {{ this }}
        where arrest_key in (
            select arrest_key
            from {{ ref('int_fact_nypd_arrests') }}
        )
        """
    )
}}

select
    *
from {{ ref('int_fact_nypd_arrests') }} as incoming

{% if is_incremental() %}

where not exists (
    select 1
    from {{ this }} as existing
    where existing.arrest_key = incoming.arrest_key
)

{% endif %}
