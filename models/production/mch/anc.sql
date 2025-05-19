{{ config(
  tags=['anc'],
  schema='production'
) }}

select * from {{ ref('int_anc') }}