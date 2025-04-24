{{ config(
    tags=['growth_monitoring'],
    schema='intermediate'
) }}

with source as (
    select * 
    from {{ source('growth_monitoring', 'nutrition_data') }}
),

typed as (

    select
        cast(id as integer) as id,
        machine_code,
        old_new,
        cast(age_in_months as integer) as age_in_months,
        gender,
        height_in_cm,
        weight_in_kg,
        under_wt_category,
        case 
            when under_wt_category IN ('n','A') then 'No'
            else 'Yes'
        end as under_wt_status,
        under_wt,
        stunting_category,
        case 
            when stunting_category IN ('n','A') then 'No'
            else 'Yes'
        end as stunting_status,
        under_wt1,
        wasting_category,
        case 
            when wasting_category IN ('n','A') then 'No'
            else 'Yes'
        end as wasting_status,
        under_wt_2,
        cast(date_of_weighing as date) as date_of_weighing,
        cast(date_of_birth as date) as date_of_birth,
        com,
        staff,
        awc_no,
        aww_name,
        age_group
    from source
)

select * from typed
