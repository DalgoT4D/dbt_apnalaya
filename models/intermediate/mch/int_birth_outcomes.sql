{{ config(
    tags=['anc'],
    schema='intermediate'
) }}

with cte_anc_cases as (
    SELECT 
    'Aanandimma' as source,
    concat(mother_id,edd_date) as child_id,
    ward,
    outcome_children_weight::float as weight_at_birth,
    outcome_date::date as date_of_birth,
    "birthType" as birth_type
FROM {{ source('anc', 'aanandimaa_anc') }} where outcome_children_weight != 'NA' 
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
SELECT * FROM cte_anc_cases where birth_type != 'Still Birth'