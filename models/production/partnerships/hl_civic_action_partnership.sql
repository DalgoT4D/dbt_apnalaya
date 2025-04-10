{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('hl_civic_action_record_partnership_normalized') }} 