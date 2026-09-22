{{
    config(
        materialized='table',
        unique_key='date_id'
    )
}}


select
*
from {{ source('source_raw_nypd', 'raw_dim_date') }}