{{
    config(
        materialized='table',
        unique_key='law_cat_cd_id'
    )
}}

select
*
from {{ref('stg_dim_law_cat_cd')}}
