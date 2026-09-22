{{
    config(
        materialized='table',
        unique_key='age_group_id'
    )
}}

select
*
from {{ ref('stg_dim_age_group') }}
