{{ config(
    tags=['anc'],
    schema='intermediate'
) }}

SELECT 
    'AIMS' as source,
    child_unique_id as child_id,
    "Ward" as ward,
    weight_at_birth_kg_gm::float as weight_at_birth,
    (NULLIF("DOB",'NA'))::date as date_of_birth
FROM {{ source('anc', 'aims_birth_outcomes') }}
UNION ALL
(SELECT 
    'Aanandimma' as source,
    concat(mother_id,edd_date) as child_id,
    ward,
    outcome_children_weight::float as weight_at_birth,
    outcome_date::date as date_of_birth
FROM {{ source('anc', 'aanandimaa_anc') }} where outcome_children_weight != 'NA')