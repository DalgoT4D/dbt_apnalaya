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
    (td.data->>'_id')::int AS id,
     td.data->>'Name_of_the_partner' AS partner_name,
    COALESCE(td.data->>'Cluster_Name',td.data->>'Cluster_Name_001', td.data->>'Cluster_Name_002') AS cluster_name, 
    COALESCE(td.data->>'Group_Name_HALWA',td.data->>'Group_Name_Vipla',td.data->>'Group_Name_Sparsha') AS group_name, 
    (td.data->>'Date')::date AS monitoring_date,
    (td.data->>'_submission_time')::timestamp AS submission_time,
    td.data->>'_submitted_by' AS submitted_by,
    td.data->>'Training_Nature' AS training_nature,
    td.data->>'Module_Topic_Name' AS module_topic_name,
    td.data->>'Citizenship_Session_Name' AS session_name,
    (td.data->>'Total_Staff_Present')::int AS total_staff_present,
    (td.data->>'Number_of_Facilitators')::int AS number_of_facilitators,
    (td.data->>'Total_Participants_Called')::int AS total_participants_called,
    (td.data->>'Total_Participants_Present')::int AS total_participants_present,
    (td.data->>'Total_Community_Members_Present')::int AS total_community_members_present
FROM deduped_cte as td
)

-- Normalize partner name when blank separated strings are equivalent
SELECT t.*,DATE_TRUNC('month', t.monitoring_date)::date AS monitoring_month,
   {{ normalize_string('t.partner_name') }} AS normalized_partner_name
   from training_data as t
   

