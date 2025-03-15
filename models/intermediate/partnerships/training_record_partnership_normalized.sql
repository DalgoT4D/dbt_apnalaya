{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','training_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

training_data as (
SELECT 
    -- Training Data
    (td.data->>'_id')::int AS training_id,
     td.data->>'Name_of_the_partner' AS partner_name,
    COALESCE(td.data->>'Cluster_Name',td.data->>'Cluster_Name_001', td.data->>'Cluster_Name_002') AS cluster_name, 
    COALESCE(td.data->>'Group_Name_HALWA',td.data->>'Group_Name_Vipla','Other') AS group_name, 
    (td.data->>'Date')::date AS submission_date,
    (td.data->>'_submission_time')::timestamp AS submission_time,
    td.data->>'_submitted_by' AS submitted_by,
    td.data->>'Training_Nature' AS training_nature,
    td.data->>'Module_Topic_Name' AS module_topic_name,
    td.data->>'Citizenship_Session_Name' AS citizenship_session_name,
    (td.data->>'Total_Staff_Present')::int AS total_staff_present,
    (td.data->>'Number_of_Facilitators')::int AS number_of_facilitators,
    (td.data->>'Total_Participants_Called')::int AS total_participants_called,
    (td.data->>'Total_Participants_Present')::int AS total_participants_present
FROM deduped_cte as td
)

SELECT *,DATE_TRUNC('month', submission_date)::date AS submission_month from training_data