{{
    config(
        materialized='table',
        unique_key='jurisdiction_code_id'
    )
}}


select
*
from {{ref('stg_dim_jurisdiction_code')}}
