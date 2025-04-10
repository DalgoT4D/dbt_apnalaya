{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('interaction_record_partnership_normalized') }} 