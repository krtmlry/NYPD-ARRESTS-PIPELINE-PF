{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        post_hook=[
            'truncate table raw.raw_nypd_arrests'
        ]
    )
}}

-- depends_on: {{ ref('nypd_arrests_dn') }}

select
    *
from {{ source('source_raw_nypd', 'raw_nypd_arrests') }}
where trim(arrest_key) is not null
  and trim(arrest_key) != '(null)'
