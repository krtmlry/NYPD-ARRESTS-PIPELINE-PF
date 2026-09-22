{{
    config(
        materialized='table',
        unique_key='perp_sex_id'
    )
}}


select
*
from {{ref('stg_dim_perp_sex')}}
