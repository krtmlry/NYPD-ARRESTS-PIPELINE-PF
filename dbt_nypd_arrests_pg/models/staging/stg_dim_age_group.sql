{{
    config(
        materialized='table'
    )
}}


with q1 as (
	select
	distinct
		case
			when trim(age_group) is Null then 'UNKNOWN'
			when trim(upper(age_group)) = upper('(null)') then 'UNKNOWN'
			else trim(age_group)
		end as age_group_format
	from {{ source('source_raw_nypd', 'raw_dim_age_group') }}
	order by age_group_format asc
),

q2 as (
	select
	*
	from q1
	where trim(age_group_format) in ('<18', '18-24', '25-44', '45-64', '65+', 'UNKNOWN')
)

select
    case
        when age_group_format = '<18' then 1
        when age_group_format = '18-24' then 2
        when age_group_format = '25-44' then 3
        when age_group_format = '45-64' then 4
        when age_group_format = '65+' then 5
        when age_group_format = 'UNKNOWN' then 6
    end::smallint as age_group_id,
    age_group_format::varchar as age_group
from q2
order by age_group_id asc
