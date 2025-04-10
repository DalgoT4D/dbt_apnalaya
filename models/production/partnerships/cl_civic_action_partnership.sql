{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('cl_civic_action_record_partnership_normalized') }} 