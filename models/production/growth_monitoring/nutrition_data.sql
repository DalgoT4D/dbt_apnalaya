{{ config(
  tags=['growth_monitoring'],
  schema='production'
) }}

select * from {{ ref('nutrition_data_normalized') }} 