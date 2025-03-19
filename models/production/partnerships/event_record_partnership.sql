{{ config(
  tags=['partnerships'],
  schema='production'
) }}

select * from {{ ref('event_record_partnership_normalized') }} 