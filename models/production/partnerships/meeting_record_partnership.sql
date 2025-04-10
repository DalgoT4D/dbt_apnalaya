{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('meeting_record_partnership_normalized') }} 