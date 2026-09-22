
{{
    config(
        materialized='table'
    )
}}


with ref_ofns_desc AS (
        select
            trim(ofns_desc) AS ofns_desc_raw,
            case
                when trim(ofns_desc) in (
                    'ADMINISTRATIVE CODE',
                    'ADMINISTRATIVE CODES'
                )
                    then 'ADMINISTRATIVE CODE'

                when trim(ofns_desc) in (
                    'CRIMINAL MISCHIEF & RELATED OF',
                    'CRIMINAL MISCHIEF & RELATED OFFENSES'
                )
                    then 'CRIMINAL MISCHIEF & RELATED OFFENSES'

                when trim(ofns_desc) in (
                    'DISRUPTION OF A RELIGIOUS SERV',
                    'DISRUPTION OF A RELIGIOUS SERVICE'
                )
                    then 'DISRUPTION OF A RELIGIOUS SERVICE'

                when trim(ofns_desc) = 'ENDAN WELFARE INCOMP'
                    then 'ENDANGERING THE WELFARE OF AN INCOMPETENT'

                when trim(ofns_desc) = 'HARRASSMENT 2'
                    then 'HARASSMENT 2'

                when trim(ofns_desc) in (
                    'HOMICIDE-NEGLIGENT,UNCLASSIFIE',
                    'HOMICIDE-NEGLIGENT,UNCLASSIFIED'
                )
                    then 'HOMICIDE-NEGLIGENT,UNCLASSIFIED'

                when trim(ofns_desc) in (
                    'INTOXICATED & IMPAIRED DRIVING',
                    'INTOXICATED/IMPAIRED DRIVING'
                )
                    then 'INTOXICATED & IMPAIRED DRIVING'

                when trim(ofns_desc) in (
                    'KIDNAPPING AND RELATED OFFENSES',
                    'KIDNAPPING & RELATED OFFENSES'
                )
                    then 'KIDNAPPING & RELATED OFFENSES'

                when trim(ofns_desc) in (
                    'LOITERING/GAMBLING (CARDS, DIC',
                    'LOITERING/GAMBLING (CARDS, DICE, ETC)'
                )
                    then 'LOITERING/GAMBLING (CARDS, DICE, ETC)'

                when trim(ofns_desc) in (
                    'MURDER & NON-NEGL. MANSLAUGHTE',
                    'MURDER & NON-NEGL. MANSLAUGHTER'
                )
                    then 'MURDER & NON-NEGL. MANSLAUGHTER'

                when trim(ofns_desc) in (
                    'OFF. AGNST PUB ORD SENSBLTY &',
                    'OFF. AGNST PUB ORD SENSBLTY & RGHTS TO PRIV'
                )
                    then 'OFF. AGNST PUB ORD SENSBLTY & RGHTS TO PRIV'

                when trim(ofns_desc) in (
                    'OFFENSES AGAINST PUBLIC ADMINI',
                    'OFFENSES AGAINST PUBLIC ADMINISTRATION'
                )
                    then 'OFFENSES AGAINST PUBLIC ADMINISTRATION'

                when trim(ofns_desc) in (
                    'OTHER OFFENSES RELATED TO THEF',
                    'OTHER OFFENSES RELATED TO THEFT'
                )
                    then 'OTHER OFFENSES RELATED TO THEFT'

                when trim(ofns_desc) in (
                    'OTHER STATE LAWS (NON PENAL LA',
                    'OTHER STATE LAWS (NON PENAL LAW)'
                )
                    then 'OTHER STATE LAWS (NON PENAL LAW)'

                when trim(ofns_desc) in (
                    'UNDER THE INFLUENCE, DRUGS',
                    'UNDER THE INFLUENCE OF DRUGS'
                )
                    then 'UNDER THE INFLUENCE OF DRUGS'

                when trim(ofns_desc) in (
                    'UNLAWFUL POSS. WEAP. ON SCHOOL',
                    'UNLAWFUL POSS. WEAP. ON SCHOOL GROUNDS'
                )
                    then 'UNLAWFUL POSS. WEAP. ON SCHOOL GROUNDS'

                when trim(ofns_desc) is Null then 'UNKNOWN'
                when trim(ofns_desc) = '(null)' then 'UNKNOWN'
                else trim(ofns_desc)
            end as ofns_desc_clean_ref

        from {{source('source_raw_nypd', 'raw_dim_ofns_desc')}}
)

select
    dense_rank() over (order by ofns_desc_clean_ref asc)::smallint as ref_id,
    ofns_desc_raw::varchar as ofns_desc_raw,
    ofns_desc_clean_ref::varchar as ofns_desc_clean_ref
from ref_ofns_desc
order by ref_id asc
