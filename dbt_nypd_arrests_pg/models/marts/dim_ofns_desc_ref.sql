{{
    config(
        materialized='table',
        unique_key='ref_id'
    )
}}

select
*
from {{ref('stg_dim_ofns_desc_ref')}}
