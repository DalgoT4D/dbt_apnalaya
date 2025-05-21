{{ config(
  tags=['mch'],
  schema='production'
) }}

select * from {{ ref('nutrition_data_normalized') }} 