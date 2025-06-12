{{ config(
    tags=['civic_action'],
    schema='intermediate'
) }}

with 
    cte_aims_hhca as (select 
        'AIMS' as source,
        'Apnalaya' as name_of_the_partner_ngo,
        'M East' as ward,
        civic_action_code,
        civic_action,
        civic_action_status,
        1 as number_of_individuals_benefited,
        civic_action_date_initiated::date as civic_action_date_initiated
        from {{source('civic_action', 'hhca_aims')}}
        where civic_action_status='Completed'
    ),
    cte_kobo_hhca as (select 
        'Kobo' as source,
        'Partner' as name_of_the_partner_ngo,
        'M East' as ward,
        civic_action_code,
        civic_action,
        civic_action_status,
        1 as number_of_individuals_benefited,
        TO_DATE(civic_action_date_initiated, 'DD-MM-YYYY')::date AS civic_action_date_initiated
        from {{source('civic_action', 'hhca_kobo')}}
        where civic_action_status='Completed'
    )

Select * from cte_aims_hhca
UNION ALL
select * from cte_kobo_hhca
