{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='arrest_key',
        pre_hook="""
        delete from {{ this }}
        where arrest_key in (
            select arrest_key
            from {{ ref('fact_nypd_arrests') }}
        )
        """
    )
}}


select
	a.arrest_key,
	b.date as arrest_date,
	c.ofns_desc,
	d.law_cat_cd_desc as law_cat_cd,
	e.arrest_boro_desc as arrest_boro,
	f.precinct_name as precinct_name,
	f.precinct_boro as precinct_boro,
	g.jurisdiction_code_desc as jurisdiction_code,
	h.age_group,
	i.perp_sex_desc as perp_sex,
	j.perp_race,
	a.latitude,
	a.longitude,
	a.sched_id
from {{ ref('fact_nypd_arrests') }} as a
    left join {{source('source_marts_dim', 'dim_date')}} 				as b on a.date_id       		= b.date_id
    left join {{source('source_marts_dim', 'dim_ofns_desc')}} 			as c on a.ofns_desc_id         	= c.ofns_desc_id
    left join {{source('source_marts_dim', 'dim_law_cat_cd')}} 		    as d on a.law_cat_cd_id        	= d.law_cat_cd_id
    left join {{source('source_marts_dim', 'dim_arrest_boro')}} 		as e on a.arrest_boro_id       	= e.arrest_boro_id
    left join {{source('source_marts_dim', 'dim_arrest_precinct')}} 	as f on a.arrest_precinct_id   	= f.arrest_precinct_id
    left join {{source('source_marts_dim', 'dim_jurisdiction_code')}} 	as g on a.jurisdiction_code_id 	= g.jurisdiction_code_id
    left join {{source('source_marts_dim', 'dim_age_group')}} 			as h on a.age_group_id         	= h.age_group_id
    left join {{source('source_marts_dim', 'dim_perp_sex')}} 			as i on a.perp_sex_id          	= i.perp_sex_id
    left join {{source('source_marts_dim', 'dim_perp_race')}} 			as j on a.perp_race_id         	= j.perp_race_id


{% if is_incremental() %}

where not exists (
    select 1
    from {{ this }} as existing
    where existing.arrest_key = a.arrest_key
)

{% endif %}
