{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('stakeholder_visit_record_partnership_normalized') }} 