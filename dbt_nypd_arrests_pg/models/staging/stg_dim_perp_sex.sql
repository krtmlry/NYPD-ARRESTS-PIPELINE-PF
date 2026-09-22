{{
    config(
        materialized='table'
    )
}}


with q1 as (
select
	case
		when trim(perp_sex) is Null then 'U'
		when trim(upper(perp_sex)) = upper('(null)') then 'U'
		else perp_sex
	end as perp_sex_format
from {{source('source_raw_nypd', 'raw_dim_perp_sex')}}
),

q2 as (
	select
	distinct perp_sex_format as perp_sex
	from q1
)

select
    row_number() over(order by perp_sex asc)::smallint as perp_sex_id,
    trim(perp_sex)::varchar as perp_sex,
    case
        when perp_sex = 'F' then 'FEMALE'
        when perp_sex = 'M' then 'MALE'
        when perp_sex = 'U' then 'UNKNOWN'
    end::varchar as perp_sex_desc
from q2
