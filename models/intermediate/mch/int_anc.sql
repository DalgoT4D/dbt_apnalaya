{{ config(
    tags=['mch'],
    schema='intermediate'
) }}

with 
    deduped_cte_aims as (
    {{ dbt_utils.deduplicate(
        relation=source('anc','aims_anc'),
        partition_by='unique_id',
        order_by='updated_at desc',
    )}}
    ),

    cte_aims_valid_cases as (
        select * from deduped_cte_aims 
        where (current_location_pregnant_woman='In the Commuity' AND miscarriage = 'No')
        ),

    cte_aims_anc as (select 
        'AIMS' as source,
        a.unique_id,
        a.ward,
        CASE WHEN a.check_up IS NOT NULL THEN 'Yes' ELSE 'No' END as check_up,
        CASE WHEN a.recevied_tt_booster ='NA' THEN NULL ELSE a.recevied_tt_booster END as recevied_tt_booster,
        CASE WHEN a.no_of_ifa_tablets_consumed IN ('1','2') THEN 'Yes' ELSE 'No' END as no_of_ifa_tablets_consumed, 
        CASE WHEN b.place_of_delivery = '#NA' THEN NULL
                 WHEN b.place_of_delivery IN ('Private Institution', 'Private Hospital') THEN 'Private'
                 ELSE 'Government' END as place_of_delivery,
        (COALESCE(b.date_deli_termination_pregnancy,a.expected_date_of_delivery))::date as date_of_delivery
    from cte_aims_valid_cases as a
    LEFT JOIN (select preg_registration_unique_id, place_of_delivery, date_deli_termination_pregnancy from {{ source('anc', 'aims_pnc') }}) as b 
    ON a.preg_registration_unique_id=b.preg_registration_unique_id),

    cte_aanandimaa_anc as (select
            'Aanandimaa' as source,
            concat(mother_id,edd_date) as child_id,
            ward,
            CASE 
                WHEN COALESCE("ANC1_TestDate","ANC2_TestDate","ANC3_TestDate","ANC4_TestDate") ='NA' 
                    THEN NULL
                WHEN (COALESCE("ANC1_TestDate","ANC2_TestDate","ANC3_TestDate","ANC4_TestDate") IS NOT NULL) 
                THEN 'Yes' 
            ELSE 'No' 
            END as checkup,
            CASE WHEN "TT Booster" ='NA' THEN NULL ELSE "TT Booster" END as recevied_tt_booster,
            NULL as no_of_ifa_tablets_consumed,
            CASE WHEN "outcomePlaceOfDelivery" = '#NA' THEN NULL
                 WHEN "outcomePlaceOfDelivery" IN ('Private Institution', 'Private Hospital') THEN 'Private'
                 ELSE 'Government' END as place_of_delivery,
            (COALESCE(outcome_date,edd_date))::date as date_of_delivery
        from {{ source('anc', 'aanandimaa_anc') }} where "outcomeDeliveryStatus" = 'Delivered')

Select * from cte_aims_anc
UNION ALL
select * from cte_aanandimaa_anc