{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('registration_record_partnership_normalized') }} 