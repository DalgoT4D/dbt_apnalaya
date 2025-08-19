{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('group_information_record_normalized') }} 