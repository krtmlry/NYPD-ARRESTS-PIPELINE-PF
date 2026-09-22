{{
    config(
        materialized='table'
    )
}}


with q1 as (
    select
        case
            when trim(law_cat_cd) is Null then 'U'
            when trim(law_cat_cd) in ('(null)', '9') then 'U'
        else trim(law_cat_cd)
        end as law_cat_cd_mod
    from {{ source('source_raw_nypd', 'raw_dim_law_cat_cd') }}
),

q2 as (
    select
        distinct law_cat_cd_mod
    from q1
)

select
    row_number() over(order by law_cat_cd_mod asc)::smallint as law_cat_cd_id,
    law_cat_cd_mod::varchar as law_cat_cd,
    case
        when law_cat_cd_mod = 'F' then 'FELONY'
        when law_cat_cd_mod = 'I' then 'INFRACTION'
        when law_cat_cd_mod = 'M' then 'MISDEMEANOR'
        when law_cat_cd_mod = 'U' then 'UNKNOWN'
        when law_cat_cd_mod = 'V' then 'VIOLATION'
        else 'UNKNOWN'
    end::varchar as law_cat_cd_desc
from q2
order by law_cat_cd_id asc
