{{
    config(
        materialized='table'
    )
}}

with valid_precincts as (
    select
    *
    from {{ source('source_raw_nypd', 'raw_dim_arrest_precinct') }}
    where arrest_precinct::int between 1 and 123
),

unknown_precinct as (
    select
        0::smallint as arrest_precinct
),

all_precincts as (
    select
        arrest_precinct::smallint
    from unknown_precinct

    union all

    select
        arrest_precinct::smallint
    from valid_precincts
),

precincts_name as (
    select
        arrest_precinct::smallint as arrest_precinct_id,
        arrest_precinct::smallint as arrest_precinct,
        case
            when arrest_precinct::int = 0 then 'UNKNOWN'
            when arrest_precinct::int % 100 in (11, 12, 13) then arrest_precinct || 'th Precinct'
            when arrest_precinct::int % 10 = 1 then arrest_precinct || 'st Precinct'
            when arrest_precinct::int % 10 = 2 then arrest_precinct || 'nd Precinct'
            when arrest_precinct::int % 10 = 3 then arrest_precinct || 'rd Precinct'
            else arrest_precinct || 'th Precinct'
        end::varchar as precinct_name
    from all_precincts
    order by arrest_precinct_id
)
select
    arrest_precinct_id,
    arrest_precinct,
    precinct_name,
    case
            when arrest_precinct between 1 and 34 then 'MANHATTAN'
            when arrest_precinct between 35 and 52 then 'BRONX'
            when arrest_precinct between 53 and 94 then 'BROOKLYN'
            when arrest_precinct between 95 and 116 then 'QUEENS'
            when arrest_precinct between 117 and 123 then 'STATEN ISLAND'
            when arrest_precinct = 0 then 'UNKNOWN'
            else 'UNKNOWN'
    end::varchar as precinct_boro
from precincts_name
