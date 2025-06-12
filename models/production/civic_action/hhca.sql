{{ config(
  tags=['civic_action'],
  schema='production'
) }}

select * from {{ ref('int_hhca') }}