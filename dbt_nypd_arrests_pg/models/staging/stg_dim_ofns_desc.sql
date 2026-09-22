
{{
    config(
        materialized='table'
    )
}}

select
    distinct ref_id::smallint as ofns_desc_id,
    ofns_desc_clean_ref::varchar as ofns_desc,
    case
        when ofns_desc_clean_ref in (
            'MURDER & NON-NEGL. MANSLAUGHTER',
            'ROBBERY',
            'BURGLARY',
            'RAPE',
            'FELONY ASSAULT',
            'GRAND LARCENY',
            'GRAND LARCENY OF MOTOR VEHICLE'
        )
        then true
        else false
    end as major_felony
from {{ref('stg_dim_ofns_desc_ref')}}
order by ofns_desc_id asc
