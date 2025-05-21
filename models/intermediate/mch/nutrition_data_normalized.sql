{{ config(
    tags=['growth_monitoring'],
    schema='intermediate'
) }}

with source as (
    select *
    from {{ source('growth_monitoring', 'nutrition_data') }}
    where date_of_weighing NOT LIKE '%?%'
),

typed as (

    select
        machine_code,
        'M East' as ward,
        age,
        gender,
        height,
        weight,
        under_wt_category,
        case 
            when under_wt_category IN ('1','2') then 'Yes'
            else 'No'
        end as under_wt_status,
        under_wt,
        stunting_category,
        case 
            when stunting_category IN ('1','2') then 'Yes'
            else 'No'
        end as stunting_status,
        under_wt1,
        wasting_category,
        case 
            when wasting_category IN ('1','2') then 'Yes'
            else 'No'
        end as wasting_status,
        date_of_weighing as date_of_weighing_raw,
        to_date(date_of_weighing,'DD-MM-YY') as date_of_weighing,
        date_of_birth
    from source
    where date_of_weighing <> '04-13-21'
)

select * from typed 
