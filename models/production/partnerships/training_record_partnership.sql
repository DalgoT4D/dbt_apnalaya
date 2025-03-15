{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('training_record_partnership_normalized') }} 