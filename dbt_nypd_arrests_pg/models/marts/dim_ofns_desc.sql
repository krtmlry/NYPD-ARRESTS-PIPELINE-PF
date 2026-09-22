{{
    config(
        materialized='table',
        unique_key='ofns_desc_id'
    )
}}

select
*
from {{ref('stg_dim_ofns_desc')}}
