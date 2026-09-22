{{
    config(
        materialized='table'
    )
}}

with q1 as (
        select
            case
                when trim(arrest_boro) is Null then 'U'
                else arrest_boro
            end as arrest_boro
        from {{ source('source_raw_nypd', 'raw_dim_arrest_boro') }}
)

select
    row_number() over(order by arrest_boro asc)::smallint as arrest_boro_id,
    arrest_boro::varchar as arrest_boro,
    case
        when trim(arrest_boro) = 'B' then 'BRONX'
        when trim(arrest_boro) = 'K' then 'BROOKLYN'
        when trim(arrest_boro) = 'M' then 'MANHATTAN'
        when trim(arrest_boro) = 'Q' then 'QUEENS'
        when trim(arrest_boro) = 'S' then 'STATEN ISLAND'
        when trim(arrest_boro) = 'U' then 'UNKNOWN'
    end::varchar as arrest_boro_desc
from q1
order by arrest_boro_id asc
