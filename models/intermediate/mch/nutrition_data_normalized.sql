{{ config(
    tags=['growth_monitoring'],
    schema='intermediate'
) }}

with source as (
    select *, LEFT(date_of_weighing, 10) AS date_of_weighing_corrected
    from {{ source('growth_monitoring', 'nutrition_data') }}
    where machine_code <> ''
),

typed as (

    select
        CAST(REGEXP_REPLACE(machine_code, '\.0$', '') AS INT) AS machine_code,
        'M East' as ward,
        old_new,
        cast(NULLIF(age_in_months,'') as decimal) as age_in_months,
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
        date_of_weighing as date_of_weighing_raw,
        {{ validate_date("date_of_weighing_corrected") }} as date_of_weighing,
        date_of_weighing_corrected,
        date_of_birth,
        awc_no,
        aww_name,
        age_group
    from source
)

select * from typed
