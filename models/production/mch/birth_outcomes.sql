{{ config(
  tags=['anc'],
  schema='production'
) }}

select *,
       case when weight_at_birth >= 2.5 then 'Yes' else 'No' end as healthy_birth_weight
 from {{ ref('int_birth_outcomes') }} 