{{
    config(
        materialized='view'
    )
}}

select
    a.arrest_key,
    b.date_id,
    c.ofns_desc_id,
    d.law_cat_cd_id,
    e.arrest_boro_id,
    f.arrest_precinct_id,
    g.jurisdiction_code_id,
    h.age_group_id,
    i.perp_sex_id,
    j.perp_race_id,
    a.latitude,
    a.longitude,
    a.sched_id,
    a.loaded_at
from {{ ref('int_type_casting') }} as a
left join {{ source('source_marts_dim','dim_date') }}               as b on a.arrest_date       = b.date
left join {{ source('source_marts_dim','dim_ofns_desc') }}          as c on a.ofns_desc         = c.ofns_desc
left join {{ source('source_marts_dim','dim_law_cat_cd') }}         as d on a.law_cat_cd        = d.law_cat_cd
left join {{ source('source_marts_dim','dim_arrest_boro') }}        as e on a.arrest_boro       = e.arrest_boro
left join {{ source('source_marts_dim','dim_arrest_precinct') }}    as f on a.arrest_precinct   = f.arrest_precinct
left join {{ source('source_marts_dim','dim_jurisdiction_code') }}  as g on a.jurisdiction_code = g.jurisdiction_code
left join {{ source('source_marts_dim','dim_age_group') }}          as h on a.age_group         = h.age_group
left join {{ source('source_marts_dim','dim_perp_sex') }}           as i on a.perp_sex          = i.perp_sex
left join {{ source('source_marts_dim','dim_perp_race') }}          as j on a.perp_race         = j.perp_race
