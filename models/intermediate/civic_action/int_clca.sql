{{ config(
    tags=['civic_action'],
    schema='intermediate'
) }}

with 
    cte_aims_clca as (select 
        'AIMS' as source,
        'Apnalaya' as name_of_the_partner_ngo,
        'M East' as ward,
        civic_action_code,
        civic_action,
        civic_action_status,
        number_of_families_benefited::numeric::int,
        civic_action_date_initiated::date as civic_action_date_initiated
        from {{source('civic_action', 'clca_aims')}}
        where civic_action_status='Completed'
    ),

    cte_kobo_clca as (select
        'Kobo' as source,
        name_of_the_partner_ngo,
        'M East' as ward,
        civic_action_code,
        name_of_the_civic_action as civic_action,
        stage_of_civic_action as civic_action_status,
        number_of_families_benefited::numeric::int,
        date as civic_action_date_initiated
        from {{source('civic_action', 'clca_kobo')}}
        where stage_of_civic_action = 'Completed (पुरी हुई)'
    )

Select * from cte_aims_clca
UNION ALL
select * from cte_kobo_clca