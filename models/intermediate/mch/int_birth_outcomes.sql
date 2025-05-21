{{ config(
    tags=['mch'],
    schema='intermediate'
) }}

with cte_anc_cases as (
    SELECT 
    'Aanandimaa' as source,
    concat(mother_id,edd_date) as child_id,
    ward,
    highrisk,
    "outcomeDeliveryComplications" as outcome_delivery_complications,
    NULLIF(outcome_children_weight,'NA')::float as weight_at_birth,
    NULLIF(outcome_date,'NA')::date as date_of_birth,
    "birthType" as birth_type
FROM {{ source('anc', 'aanandimaa_anc') }}  
)

-- Exclude AIMS data due to integer birth weight 
-- SELECT 
--     'AIMS' as source,
--     child_unique_id as child_id,
--     "Ward" as ward,
--     weight_at_birth_kg_gm::float as weight_at_birth,
--     (NULLIF("DOB",'NA'))::date as date_of_birth
-- FROM {{ source('anc', 'aims_birth_outcomes') }}
-- UNION ALL
SELECT * FROM cte_anc_cases 