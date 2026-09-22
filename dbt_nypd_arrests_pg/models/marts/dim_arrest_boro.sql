{{
    config(
        materialized='table',
        unique_key='arrest_boro_id'
    )
}}


select
*
from {{ref('stg_dim_arrest_boro')}}
