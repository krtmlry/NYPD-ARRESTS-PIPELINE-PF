{{
    config(
        materialized='view'
    )
}}

with valid_records as (
    select
        *
    from {{ source('source_raw_nypd', 'raw_nypd_arrests') }}
    where trim(arrest_date) is not Null
      and trim(arrest_date) != '(null)'

      and trim(arrest_key) is not Null
      and trim(arrest_key) != '(null)'

      and trim(latitude) is not Null
      and trim(latitude) != '(null)'
      and trim(latitude) != '0.0'

      and trim(longitude) is not Null
      and trim(longitude) != '(null)'
      and trim(longitude) != '0.0'

	  and trim(jurisdiction_code) in ('0','1','2')
),

valid_unique_records as (
    select
        distinct on(arrest_key)
        *
    from valid_records
)

select
    *
from valid_unique_records
