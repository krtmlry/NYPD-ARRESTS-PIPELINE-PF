{{
    config(
        materialized='table'
    )
}}


with valid_jurisdiction as (
    select
    *
    from {{ source('source_raw_nypd', 'raw_dim_jurisdiction_code') }}
    where trim(jurisdiction_code) in ('0','1','2')
),

unknown_jurisdiction as (
    select
        3::smallint as jurisdiction_code

),

all_jurisdiction as (
    select
        jurisdiction_code::smallint
    from valid_jurisdiction
    union all
    select
        jurisdiction_code::smallint
    from unknown_jurisdiction
)

select
    row_number() over(order by jurisdiction_code asc)::smallint as jurisdiction_code_id,
    jurisdiction_code::smallint as jurisdiction_code,
    case
        when jurisdiction_code = 0 then 'PATROL'
        when jurisdiction_code = 1 then 'TRANSIT'
        when jurisdiction_code = 2 then 'HOUSING'
    end::varchar as jurisdiction_code_desc
from all_jurisdiction
