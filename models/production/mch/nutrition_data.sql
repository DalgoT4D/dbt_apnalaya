{{ config(
  tags=['mch'],
  schema='production'
) }}

select * from {{ ref('int_nutrition_data') }} 