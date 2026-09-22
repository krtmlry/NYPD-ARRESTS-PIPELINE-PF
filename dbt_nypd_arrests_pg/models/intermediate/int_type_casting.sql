{{
    config(
        materialized='view'
    )
}}


with ofns_desc_map as (
    select
        a.arrest_key,
        a.arrest_date,
        b.ofns_desc_clean_ref as ofns_desc,
        a.law_cat_cd,
        a.arrest_boro,
        a.arrest_precinct,
        a.jurisdiction_code,
        a.age_group,
        a.perp_sex,
        a.perp_race,
        a.latitude,
        a.longitude,
        a.sched_id,
        a.loaded_at
    from {{ ref('stg_nypd_arrests') }} as a
    left join {{ source('source_marts_dim','dim_ofns_desc_ref') }} as b on a.ofns_desc = b.ofns_desc_raw
),

replace_values as (
	select
		arrest_key,
		arrest_date,
		case
			when trim(ofns_desc) is Null then 'UNKNOWN'
			when trim(upper(ofns_desc)) = upper('(null)') then 'UNKNOWN'
			else trim(ofns_desc)
		end as f_ofns_desc,
		case
		    when trim(law_cat_cd) in ('F','M','V','I') then trim(law_cat_cd)
			else 'U'
		end as f_law_cat_cd,
		case
            when trim(arrest_boro) is Null then 'U'
			when trim(upper(arrest_boro)) = upper('(null)') then 'U'
            else arrest_boro
		end as f_arrest_boro,
		case
			when trim(arrest_precinct) is Null then '0'
			when trim(upper(arrest_precinct)) = upper('(null)') then '0'
			when arrest_precinct::int not between 1 and 123 then '0'
			else trim(arrest_precinct)
		end as f_arrest_precinct,
		case
			when trim(jurisdiction_code) is Null then '3'
			when trim(upper(jurisdiction_code)) = upper('(null)') then '3'
			else jurisdiction_code
		end as f_jurisdiction_code,
		case
			when trim(age_group) is Null then 'UNKNOWN'
			when trim(upper(age_group)) = upper('(null)') then 'UNKNOWN'
			else age_group
		end as f_age_group,
		case
			when trim(perp_sex) is Null then 'U'
			when trim(upper(perp_sex)) = upper('(null)') then 'U'
			else perp_sex
		end as f_perp_sex,
		case
			when trim(perp_race) is Null then 'UNKNOWN'
			when trim(upper(perp_race)) = upper('(null)') then 'UNKNOWN'
			else perp_race
		end as f_perp_race,
		latitude,
		longitude,
		sched_id,
		loaded_at
	from ofns_desc_map
),

clean_and_formatted as (

    select
        trim(arrest_key)::bigint as arrest_key,
        trim(arrest_date)::date as arrest_date,
        trim(f_ofns_desc)::varchar as ofns_desc,
        trim(f_law_cat_cd)::varchar as law_cat_cd,
        trim(f_arrest_boro)::varchar as arrest_boro,
        trim(f_arrest_precinct)::smallint as arrest_precinct,
        trim(f_jurisdiction_code)::smallint as jurisdiction_code,
        trim(f_age_group)::varchar as age_group,
        trim(f_perp_sex)::varchar as perp_sex,
        trim(f_perp_race)::varchar as perp_race,
        trim(latitude)::double precision as latitude,
        trim(longitude)::double precision as longitude,
        sched_id::bigint as sched_id,
        loaded_at::timestamp as loaded_at
    from replace_values
)

select
*
from clean_and_formatted
