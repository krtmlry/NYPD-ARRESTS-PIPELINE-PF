{{
    config(
        materialized='table'
    )
}}

select
    row_number() over(order by perp_race asc)::smallint as perp_race_id,
    trim(perp_race)::varchar as perp_race
from {{source('source_raw_nypd', 'raw_dim_perp_race')}}
order by perp_race_id asc
