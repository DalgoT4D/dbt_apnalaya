{{ config(
  tags=['partnerships'],
  schema='production'
) }}

with training_record as (SELECT
    {{ dbt_utils.star(from=ref('training_record_partnership_normalized'), except=["partner_name", "normalized_partner_name"]) }},
    normalized_partner_name as partner_name
FROM {{ ref('training_record_partnership_normalized') }}),

nulls_cte as (select * from training_record where (group_name is null)),

non_nulls_cte as (select * from training_record where (group_name is not null)),

non_nulls_ranked_cte as (
  SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY group_name, session_name
               ORDER BY submission_time DESC
           ) AS row_num
    FROM non_nulls_cte
),

deduped_cte as (
   SELECT * from non_nulls_ranked_cte  where row_num = 1
    )

SELECT * FROM deduped_cte
UNION ALL
SELECT *, 1 AS rown_num FROM nulls_cte