{{
    config(
        materialized='table',
        unique_key='perp_race_id'
    )
}}


select
*
from {{ref('stg_dim_perp_race')}}
