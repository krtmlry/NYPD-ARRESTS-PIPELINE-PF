{{
    config(
        materialize='table',
        unique_key='arrest_precinct_id'
    )
}}

select
*
from {{ref('stg_dim_arrest_precinct')}}
